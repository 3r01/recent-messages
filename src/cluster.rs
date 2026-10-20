use crate::web::auth::{TwitchUserAccessToken, UserAuthorization};
use crate::{config::ClusterConfig, db::StorageError};
use chrono::{TimeZone, Utc};
use reqwest::{Client, Url};
use rusqlite::{Connection, types::Value as SqlValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

const MAX_RESPONSE: usize = 16 * 1024 * 1024;
const CHANNEL_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS rm_channel (channel_login TEXT PRIMARY KEY, ignored INTEGER NOT NULL DEFAULT 0, always_join INTEGER NOT NULL DEFAULT 0, purge_ms INTEGER NOT NULL DEFAULT 0)";
const AUTH_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS rm_auth (access_token TEXT PRIMARY KEY, twitch_access_token TEXT NOT NULL, twitch_refresh_token TEXT NOT NULL, validated_ms INTEGER NOT NULL, valid_until_ms INTEGER NOT NULL, user_id TEXT NOT NULL, user_login TEXT NOT NULL, user_name TEXT NOT NULL, user_profile_image_url TEXT NOT NULL)";

#[derive(Clone)]
pub struct ClusterControl {
    connection: Arc<Mutex<Connection>>,
    mutation_lock: Arc<AsyncMutex<()>>,
    pub(crate) receive_lock: Arc<AsyncMutex<()>>,
    client: Client,
    peer: Url,
    replicas: Arc<[Url]>,
    token: Arc<str>,
    owner: bool,
    authority: Option<Url>,
}
impl std::fmt::Debug for ClusterControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterControl")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, Eq, PartialEq)]
pub struct ChannelPolicy {
    pub ignored: bool,
    pub always_join: bool,
    pub purge_ms: i64,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Snapshot {
    epoch: String,
    revision: i64,
    channels: Vec<Vec<Value>>,
    sessions: Vec<Vec<Value>>,
}
#[derive(Serialize, Deserialize)]
pub(crate) enum Mutation {
    Ignore {
        channel: String,
        ignored: bool,
    },
    Purge {
        channel: String,
        cutoff: i64,
    },
    Pin {
        channel: String,
        enabled: bool,
        maximum: usize,
    },
    ReplacePins(Vec<String>),
    BootstrapPins(Vec<String>),
    SaveAuth {
        auth: UserAuthorization,
        insert: bool,
    },
    Revoke(String),
}
struct StatementResult {
    rows_affected: usize,
}
fn unavailable() -> StorageError {
    StorageError::ClusterUnavailable
}
fn sql_value(value: &Value) -> Result<SqlValue, StorageError> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::String(v) => SqlValue::Text(v.clone()),
        Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Value::Number(v) => SqlValue::Integer(v.as_i64().ok_or_else(unavailable)?),
        _ => return Err(unavailable()),
    })
}
fn private_origin(value: &str) -> Result<Url, StorageError> {
    let peer = Url::parse(value).map_err(|_| unavailable())?;
    let private = peer
        .host_str()
        .and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
        .is_some_and(|ip| match ip {
            std::net::IpAddr::V4(ip) => {
                ip.is_private()
                    || ip.is_loopback()
                    || (ip.octets()[0] == 100 && (64..128).contains(&ip.octets()[1]))
            }
            std::net::IpAddr::V6(ip) => ip.is_loopback() || (ip.segments()[0] & 0xfe00 == 0xfc00),
        });
    if !private
        || !matches!(peer.scheme(), "http" | "https")
        || !peer.username().is_empty()
        || peer.password().is_some()
        || peer.path() != "/"
        || peer.query().is_some()
        || peer.fragment().is_some()
    {
        return Err(unavailable());
    }
    Ok(peer)
}
impl ClusterControl {
    pub async fn from_config(
        config: &ClusterConfig,
        token: Option<&str>,
        path: &std::path::Path,
    ) -> Result<Option<Self>, StorageError> {
        if config.role == "off" {
            return Ok(None);
        }
        if !matches!(config.role.as_str(), "owner" | "replica" | "consensus")
            || config.request_timeout.is_zero()
            || config.sync_interval.is_zero()
        {
            return Err(unavailable());
        }
        let token = token.filter(|s| s.len() >= 32).ok_or_else(unavailable)?;
        if config.replica_urls.len() > 24 {
            return Err(unavailable());
        }
        let authority = if config.role == "consensus" {
            Some(private_origin(&config.authority_url)?)
        } else {
            None
        };
        let peer = private_origin(if authority.is_some() {
            &config.authority_url
        } else {
            &config.peer_url
        })?;
        if config.role == "replica" && !config.replica_urls.is_empty() {
            return Err(unavailable());
        }
        let mut replicas = Vec::new();
        if config.role == "owner" {
            if config.replica_urls.is_empty() {
                replicas.push(peer.clone());
            } else {
                for value in &config.replica_urls {
                    let url = private_origin(value)?;
                    if replicas.contains(&url) {
                        return Err(unavailable());
                    }
                    replicas.push(url);
                }
            }
        }
        let connection = Connection::open(path.with_extension("owner.sqlite"))?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        connection.execute_batch(&format!("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; {CHANNEL_SCHEMA}; {AUTH_SCHEMA}; CREATE TABLE IF NOT EXISTS rm_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"))?;
        let result = Self {
            connection: Arc::new(Mutex::new(connection)),
            mutation_lock: Arc::new(AsyncMutex::new(())),
            receive_lock: Arc::new(AsyncMutex::new(())),
            client: Client::builder()
                .connect_timeout(std::time::Duration::from_millis(500))
                .timeout(config.request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_proxy()
                .build()
                .map_err(|_| unavailable())?,
            peer,
            replicas: replicas.into(),
            token: Arc::from(token),
            owner: config.role == "owner",
            authority,
        };
        if result.owner {
            result.check().await?;
        }
        Ok(Some(result))
    }
    pub(crate) fn uses_consensus(&self) -> bool {
        self.authority.is_some()
    }
    pub(crate) fn is_owner(&self) -> bool {
        self.owner
    }
    async fn database<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        let db = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = db.lock().map_err(|_| unavailable())?;
            f(&mut connection)
        })
        .await
        .map_err(|_| unavailable())?
    }
    async fn authority_request(
        &self,
        path: &str,
        statements: Vec<Value>,
    ) -> Result<Vec<Value>, StorageError> {
        let url = self
            .authority
            .as_ref()
            .ok_or_else(unavailable)?
            .join(path)
            .map_err(|_| unavailable())?;
        let expected = statements.len();
        let mut response = self
            .client
            .post(url)
            .basic_auth("application", Some(&*self.token))
            .json(&statements)
            .send()
            .await
            .map_err(|_| unavailable())?;
        if !response.status().is_success() {
            return Err(unavailable());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if bytes.len() + chunk.len() > MAX_RESPONSE {
                return Err(unavailable());
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
        let results = body
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(unavailable)?;
        if body.get("error").is_some()
            || body.get("error_v2").is_some()
            || results.len() != expected
            || results
                .iter()
                .any(|r| !r.is_object() || r.get("error").is_some() || r.get("error_v2").is_some())
        {
            return Err(unavailable());
        }
        Ok(results.clone())
    }
    async fn write(
        &self,
        mut statements: Vec<Value>,
    ) -> Result<Vec<StatementResult>, StorageError> {
        if self.uses_consensus() {
            let count = statements.len();
            statements.push(json!([
                "UPDATE rm_metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'"
            ]));
            let results = self
                .authority_request("db/execute?transaction", statements)
                .await?;
            return results
                .into_iter()
                .take(count)
                .map(|r| {
                    let rows_affected = match r.get("rows_affected") {
                        None => 0,
                        Some(v) => usize::try_from(v.as_u64().ok_or_else(unavailable)?)
                            .map_err(|_| unavailable())?,
                    };
                    Ok(StatementResult { rows_affected })
                })
                .collect();
        }
        self.database(move |db| {
            let tx = db.transaction()?;
            let mut results = Vec::new();
            for statement in statements {
                let values = statement.as_array().ok_or_else(unavailable)?;
                let sql = values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(unavailable)?;
                let params = values[1..]
                    .iter()
                    .map(sql_value)
                    .collect::<Result<Vec<_>, _>>()?;
                results.push(StatementResult {
                    rows_affected: tx.execute(sql, rusqlite::params_from_iter(params))?,
                });
            }
            tx.execute(
                "UPDATE rm_metadata SET value=CAST(value AS INTEGER)+1 WHERE key='revision'",
                [],
            )?;
            tx.commit()?;
            Ok(results)
        })
        .await
    }
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Vec<Value>>, StorageError> {
        if self.uses_consensus() {
            let mut statement = vec![json!(sql)];
            statement.extend(params);
            let results = self
                .authority_request("db/query?level=strong&transaction", vec![json!(statement)])
                .await?;
            return match results[0].get("values") {
                None | Some(Value::Null) => Ok(Vec::new()),
                Some(Value::Array(rows)) => rows
                    .iter()
                    .map(|r| r.as_array().cloned().ok_or_else(unavailable))
                    .collect(),
                _ => Err(unavailable()),
            };
        }
        let sql = sql.to_owned();
        self.database(move |db| {
            let params = params
                .iter()
                .map(sql_value)
                .collect::<Result<Vec<_>, _>>()?;
            let mut stmt = db.prepare(&sql)?;
            let width = stmt.column_count();
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
                (0..width)
                    .map(|i| {
                        Ok(match row.get::<_, SqlValue>(i)? {
                            SqlValue::Null => Value::Null,
                            SqlValue::Integer(v) => json!(v),
                            SqlValue::Text(v) => json!(v),
                            _ => return Err(rusqlite::Error::InvalidQuery),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }
    pub async fn check(&self) -> Result<(), StorageError> {
        if self
            .query(
                "SELECT value FROM rm_metadata WHERE key='initialized'",
                vec![],
            )
            .await?
            == vec![vec![json!("1")]]
        {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    pub(crate) async fn has_snapshot(&self) -> Result<bool, StorageError> {
        Ok(!self
            .query("SELECT value FROM rm_metadata WHERE key='epoch'", vec![])
            .await?
            .is_empty())
    }
    pub(crate) async fn mark_ready(&self) -> Result<(), StorageError> {
        self.database(|db| {
            db.execute(
                "INSERT OR REPLACE INTO rm_metadata VALUES('initialized','1')",
                [],
            )?;
            Ok(())
        })
        .await
    }
    pub(crate) async fn snapshot(&self) -> Result<Snapshot, StorageError> {
        self.database(|db| {
            let tx = db.transaction()?;
            let epoch: String = tx.query_row("SELECT value FROM rm_metadata WHERE key='epoch'", [], |r| r.get(0))?;
            let revision: String = tx.query_row("SELECT value FROM rm_metadata WHERE key='revision'", [], |r| r.get(0))?;
            let read = |sql: &str, width: usize| -> Result<Vec<Vec<Value>>, StorageError> {
                let mut stmt = tx.prepare(sql)?;
                let rows = stmt.query_map([], |r| (0..width).map(|i| Ok(match r.get::<_,SqlValue>(i)? { SqlValue::Integer(v)=>json!(v), SqlValue::Text(v)=>json!(v), _=>return Err(rusqlite::Error::InvalidQuery) })).collect::<Result<Vec<_>,_>>())?;
                Ok(rows.collect::<Result<Vec<_>,_>>()?)
            };
            Ok(Snapshot { epoch, revision: revision.parse().map_err(|_| unavailable())?, channels: read("SELECT channel_login,ignored,always_join,purge_ms FROM rm_channel ORDER BY channel_login",4)?, sessions: read("SELECT * FROM rm_auth ORDER BY access_token",9)? })
        }).await
    }
    pub(crate) async fn receive(&self, snapshot: Snapshot) -> Result<bool, StorageError> {
        if self.owner || self.uses_consensus() || snapshot.epoch.is_empty() || snapshot.revision < 0
        {
            return Err(unavailable());
        }
        self.database(move |db| {
            use rusqlite::OptionalExtension;
            let tx = db.transaction()?;
            let previous: Option<String> = tx
                .query_row("SELECT value FROM rm_metadata WHERE key='epoch'", [], |r| {
                    r.get(0)
                })
                .optional()?;
            if let Some(epoch) = previous {
                if epoch != snapshot.epoch {
                    return Err(unavailable());
                }
                let revision: String = tx.query_row(
                    "SELECT value FROM rm_metadata WHERE key='revision'",
                    [],
                    |r| r.get(0),
                )?;
                let revision = revision.parse::<i64>().map_err(|_| unavailable())?;
                if snapshot.revision < revision {
                    return Err(unavailable());
                }
                if snapshot.revision == revision {
                    return Ok(false);
                }
            }
            for row in &snapshot.channels {
                let name = string(row, 0)?;
                if row.len() != 4
                    || twitch_irc::validate::validate_login(&name).is_err()
                    || !matches!(integer(row, 1)?, 0 | 1)
                    || !matches!(integer(row, 2)?, 0 | 1)
                {
                    return Err(unavailable());
                }
                let old: Option<i64> = tx
                    .query_row(
                        "SELECT purge_ms FROM rm_channel WHERE channel_login=?",
                        [&name],
                        |r| r.get(0),
                    )
                    .optional()?;
                if integer(row, 3)? < old.unwrap_or(0) {
                    return Err(unavailable());
                }
            }
            let retained: std::collections::HashSet<_> = snapshot
                .channels
                .iter()
                .map(|r| string(r, 0))
                .collect::<Result<_, _>>()?;
            {
                let mut stmt =
                    tx.prepare("SELECT channel_login FROM rm_channel WHERE purge_ms>0")?;
                for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
                    if !retained.contains(&row?) {
                        return Err(unavailable());
                    }
                }
            }
            tx.execute("DELETE FROM rm_channel", [])?;
            tx.execute("DELETE FROM rm_auth", [])?;
            for row in snapshot.channels {
                tx.execute(
                    "INSERT INTO rm_channel VALUES(?,?,?,?)",
                    rusqlite::params_from_iter(
                        row.iter().map(sql_value).collect::<Result<Vec<_>, _>>()?,
                    ),
                )?;
            }
            for row in snapshot.sessions {
                if row.len() != 9 {
                    return Err(unavailable());
                }
                tx.execute(
                    "INSERT INTO rm_auth VALUES(?,?,?,?,?,?,?,?,?)",
                    rusqlite::params_from_iter(
                        row.iter().map(sql_value).collect::<Result<Vec<_>, _>>()?,
                    ),
                )?;
            }
            for (key, value) in [
                ("epoch", snapshot.epoch),
                ("revision", snapshot.revision.to_string()),
                ("initialized", "0".into()),
                ("pins_bootstrapped", "1".into()),
            ] {
                tx.execute(
                    "INSERT OR REPLACE INTO rm_metadata VALUES(?,?)",
                    [key, &value],
                )?;
            }
            tx.commit()?;
            Ok(true)
        })
        .await
    }
    async fn remote<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        payload: Option<&impl Serialize>,
    ) -> Result<T, StorageError> {
        self.remote_to(&self.peer, path, payload).await
    }
    async fn remote_to<T: serde::de::DeserializeOwned>(
        &self,
        peer: &Url,
        path: &str,
        payload: Option<&impl Serialize>,
    ) -> Result<T, StorageError> {
        let url = peer.join(path).map_err(|_| unavailable())?;
        let request = if let Some(payload) = payload {
            self.client.post(url).json(payload)
        } else {
            self.client.get(url)
        };
        let mut response = request
            .bearer_auth(&*self.token)
            .send()
            .await
            .map_err(|_| unavailable())?;
        if !response.status().is_success() {
            return Err(unavailable());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if bytes.len() + chunk.len() > MAX_RESPONSE {
                return Err(unavailable());
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| unavailable())
    }
    pub(crate) async fn push(&self) -> Result<(), StorageError> {
        let _guard = self.mutation_lock.lock().await;
        self.push_unlocked().await
    }
    async fn push_unlocked(&self) -> Result<(), StorageError> {
        if !self.owner || self.replicas.is_empty() {
            return Err(unavailable());
        }
        let snapshot = self.snapshot().await?;
        let results = futures::future::join_all(self.replicas.iter().map(|peer| async {
            let acknowledged: bool = self
                .remote_to(peer, "internal/v1/control-state", Some(&snapshot))
                .await?;
            if acknowledged {
                Ok(())
            } else {
                Err(unavailable())
            }
        }))
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }
    pub(crate) async fn pull(&self) -> Result<Snapshot, StorageError> {
        self.remote("internal/v1/control-state", None::<&Snapshot>)
            .await
    }
    pub(crate) async fn mutate(&self, operation: Mutation) -> Result<bool, StorageError> {
        if !self.owner && !self.uses_consensus() {
            return self
                .remote("internal/v1/control-mutation", Some(&operation))
                .await;
        }
        let _guard = self.mutation_lock.lock().await;
        self.check().await?;
        let result = match operation {
            Mutation::Ignore { channel, ignored } => {
                self.set_ignored_local(&channel, ignored).await?;
                false
            }
            Mutation::Purge { channel, cutoff } => {
                self.purge_local(&channel, cutoff).await?;
                false
            }
            Mutation::Pin {
                channel,
                enabled,
                maximum,
            } => {
                self.set_pin_local(&channel, enabled, maximum).await?;
                false
            }
            Mutation::ReplacePins(channels) => {
                self.replace_pins_local(&channels).await?;
                false
            }
            Mutation::BootstrapPins(channels) => self.bootstrap_pins_local(&channels).await?,
            Mutation::SaveAuth { auth, insert } => {
                self.save_auth_local(&auth, insert).await?;
                false
            }
            Mutation::Revoke(token) => {
                self.revoke_local(&token).await?;
                false
            }
        };
        if !self.uses_consensus() {
            self.push_unlocked().await?;
        }
        Ok(result)
    }
    pub async fn set_ignored(&self, channel: &str, ignored: bool) -> Result<(), StorageError> {
        self.mutate(Mutation::Ignore {
            channel: channel.to_owned(),
            ignored,
        })
        .await
        .map(|_| ())
    }
    pub async fn purge(&self, channel: &str, cutoff: i64) -> Result<(), StorageError> {
        self.mutate(Mutation::Purge {
            channel: channel.to_owned(),
            cutoff,
        })
        .await
        .map(|_| ())
    }
    pub async fn set_pin(
        &self,
        channel: &str,
        enabled: bool,
        maximum: usize,
    ) -> Result<(), StorageError> {
        self.mutate(Mutation::Pin {
            channel: channel.to_owned(),
            enabled,
            maximum,
        })
        .await
        .map(|_| ())
    }
    pub async fn replace_pins(&self, channels: &[String]) -> Result<(), StorageError> {
        self.mutate(Mutation::ReplacePins(channels.to_vec()))
            .await
            .map(|_| ())
    }
    pub async fn bootstrap_pins(&self, channels: &[String]) -> Result<bool, StorageError> {
        if channels.is_empty() {
            return Ok(false);
        }
        self.mutate(Mutation::BootstrapPins(channels.to_vec()))
            .await
    }
    pub async fn save_auth(
        &self,
        auth: &UserAuthorization,
        insert: bool,
    ) -> Result<(), StorageError> {
        self.mutate(Mutation::SaveAuth {
            auth: auth.clone(),
            insert,
        })
        .await
        .map(|_| ())
    }
    pub async fn revoke(&self, token: &str) -> Result<(), StorageError> {
        self.mutate(Mutation::Revoke(token.to_owned()))
            .await
            .map(|_| ())
    }
    pub async fn policy(&self, channel: &str) -> Result<ChannelPolicy, StorageError> {
        let rows = self
            .query(
                "SELECT ignored, always_join, purge_ms FROM rm_channel WHERE channel_login=?",
                vec![json!(channel.to_ascii_lowercase())],
            )
            .await?;
        rows.first()
            .map(|r| policy_from_row(r))
            .transpose()
            .map(|p| p.unwrap_or_default())
    }

    pub async fn policies(&self) -> Result<Vec<(String, ChannelPolicy)>, StorageError> {
        self.query("SELECT channel_login, ignored, always_join, purge_ms FROM rm_channel ORDER BY channel_login", vec![])
            .await?.into_iter().map(|r| Ok((string(&r, 0)?, policy_from_row(&r[1..])?))).collect()
    }

    async fn set_ignored_local(&self, channel: &str, ignored: bool) -> Result<(), StorageError> {
        self.write(vec![json!([
            "INSERT INTO rm_channel(channel_login, ignored, purge_ms) VALUES (?, ?, ?)
          ON CONFLICT(channel_login) DO UPDATE SET ignored=excluded.ignored,
          purge_ms=MAX(rm_channel.purge_ms, excluded.purge_ms)",
            channel.to_ascii_lowercase(),
            i32::from(ignored),
            Utc::now().timestamp_millis()
        ])])
        .await
        .map(|_| ())
    }

    async fn purge_local(&self, channel: &str, cutoff: i64) -> Result<(), StorageError> {
        self.write(vec![json!(["INSERT INTO rm_channel(channel_login, purge_ms) VALUES (?, ?)
          ON CONFLICT(channel_login) DO UPDATE SET purge_ms=MAX(rm_channel.purge_ms, excluded.purge_ms)",
          channel.to_ascii_lowercase(), cutoff])]).await.map(|_| ())
    }

    pub async fn pins(&self) -> Result<Vec<String>, StorageError> {
        self.query(
            "SELECT channel_login FROM rm_channel WHERE always_join=1 ORDER BY channel_login",
            vec![],
        )
        .await?
        .iter()
        .map(|r| string(r, 0))
        .collect()
    }

    async fn set_pin_local(
        &self,
        channel: &str,
        enabled: bool,
        maximum: usize,
    ) -> Result<(), StorageError> {
        let marker = json!([
            "INSERT OR REPLACE INTO rm_metadata(key, value) VALUES ('pins_bootstrapped', '1')"
        ]);
        let results = if enabled {
            self.write(vec![
                json!([
                    "INSERT INTO rm_channel(channel_login, always_join)
              SELECT ?, 1 WHERE (SELECT COUNT(*) FROM rm_channel WHERE always_join=1) < ?
              OR EXISTS(SELECT 1 FROM rm_channel WHERE channel_login=? AND always_join=1)
              ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                    channel.to_ascii_lowercase(),
                    maximum,
                    channel.to_ascii_lowercase()
                ]),
                marker,
            ])
            .await?
        } else {
            self.write(vec![
                json!([
                    "UPDATE rm_channel SET always_join=0 WHERE channel_login=?",
                    channel.to_ascii_lowercase()
                ]),
                marker,
            ])
            .await?
        };
        if enabled && results[0].rows_affected == 0 {
            return Err(StorageError::AlwaysJoinLimit(maximum));
        }
        Ok(())
    }

    async fn replace_pins_local(&self, channels: &[String]) -> Result<(), StorageError> {
        let mut statements = vec![
            json!(["UPDATE rm_channel SET always_join=0"]),
            json!([
                "INSERT OR REPLACE INTO rm_metadata(key, value) VALUES ('pins_bootstrapped', '1')"
            ]),
        ];
        statements.extend(channels.iter().map(|c| {
            json!([
                "INSERT INTO rm_channel(channel_login, always_join) VALUES (?, 1)
            ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                c.to_ascii_lowercase()
            ])
        }));
        self.write(statements).await.map(|_| ())
    }

    async fn bootstrap_pins_local(&self, channels: &[String]) -> Result<bool, StorageError> {
        if channels.is_empty() {
            return Ok(false);
        }
        let mut statements: Vec<Value> = channels
            .iter()
            .map(|c| {
                json!([
                    "INSERT INTO rm_channel(channel_login, always_join)
             SELECT ?, 1 WHERE NOT EXISTS(SELECT 1 FROM rm_metadata WHERE key='pins_bootstrapped')
             ON CONFLICT(channel_login) DO UPDATE SET always_join=1",
                    c.to_ascii_lowercase()
                ])
            })
            .collect();
        statements.push(json!([
            "INSERT OR IGNORE INTO rm_metadata(key, value) VALUES ('pins_bootstrapped', '1')"
        ]));
        let result = self.write(statements).await?;
        Ok(result.last().is_some_and(|r| r.rows_affected > 0))
    }

    async fn save_auth_local(
        &self,
        auth: &UserAuthorization,
        insert: bool,
    ) -> Result<(), StorageError> {
        let sql = if insert {
            "INSERT INTO rm_auth(access_token, twitch_access_token, twitch_refresh_token, validated_ms, valid_until_ms,
              user_id, user_login, user_name, user_profile_image_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        } else {
            "UPDATE rm_auth SET twitch_access_token=?, twitch_refresh_token=?, validated_ms=?, valid_until_ms=?,
              user_id=?, user_login=?, user_name=?, user_profile_image_url=? WHERE access_token=?"
        };
        let mut values = vec![
            json!(auth.twitch_token.access_token),
            json!(auth.twitch_token.refresh_token),
            json!(auth.twitch_authorization_last_validated.timestamp_millis()),
            json!(auth.valid_until.timestamp_millis()),
            json!(auth.user_id),
            json!(auth.user_login),
            json!(auth.user_name),
            json!(auth.user_profile_image_url),
        ];
        if insert {
            values.insert(0, json!(auth.access_token));
        } else {
            values.push(json!(auth.access_token));
        }
        values.insert(0, json!(sql));
        self.write(vec![json!(values)]).await.map(|_| ())
    }

    pub async fn auth(&self, token: &str) -> Result<Option<UserAuthorization>, StorageError> {
        let rows = self.query("SELECT access_token, twitch_access_token, twitch_refresh_token, validated_ms, valid_until_ms,
          user_id, user_login, user_name, user_profile_image_url FROM rm_auth WHERE access_token=? AND valid_until_ms>=?",
            vec![json!(token), json!(Utc::now().timestamp_millis())]).await?;
        rows.first()
            .map(|r| {
                Ok(UserAuthorization {
                    access_token: string(r, 0)?,
                    twitch_token: TwitchUserAccessToken {
                        access_token: string(r, 1)?,
                        refresh_token: string(r, 2)?,
                    },
                    twitch_authorization_last_validated: Utc
                        .timestamp_millis_opt(integer(r, 3)?)
                        .single()
                        .ok_or_else(unavailable)?,
                    valid_until: Utc
                        .timestamp_millis_opt(integer(r, 4)?)
                        .single()
                        .ok_or_else(unavailable)?,
                    user_id: string(r, 5)?,
                    user_login: string(r, 6)?,
                    user_name: string(r, 7)?,
                    user_profile_image_url: string(r, 8)?,
                })
            })
            .transpose()
    }

    async fn revoke_local(&self, token: &str) -> Result<(), StorageError> {
        self.write(vec![json!([
            "DELETE FROM rm_auth WHERE access_token=?",
            token
        ])])
        .await
        .map(|_| ())
    }
}
fn string(row: &[Value], index: usize) -> Result<String, StorageError> {
    row.get(index)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(unavailable)
}
fn integer(row: &[Value], index: usize) -> Result<i64, StorageError> {
    row.get(index)
        .and_then(Value::as_i64)
        .ok_or_else(unavailable)
}
fn policy_from_row(row: &[Value]) -> Result<ChannelPolicy, StorageError> {
    Ok(ChannelPolicy {
        ignored: integer(row, 0)? != 0,
        always_join: integer(row, 1)? != 0,
        purge_ms: integer(row, 2)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Extension, Json, Router,
        routing::{get, post},
    };
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "rm-pair-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
        fn seed(&self) {
            let db = Connection::open(self.0.join("control.owner.sqlite")).unwrap();
            db.execute_batch(&format!("{CHANNEL_SCHEMA};{AUTH_SCHEMA};CREATE TABLE rm_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO rm_metadata VALUES('initialized','1'),('epoch','fixture'),('revision','0'),('pins_bootstrapped','1');")).unwrap();
        }
        async fn node(&self, role: &str, url: String) -> ClusterControl {
            ClusterControl::from_config(
                &ClusterConfig {
                    role: role.into(),
                    peer_url: url,
                    ..ClusterConfig::default()
                },
                Some("synthetic-peer-token-at-least-32-characters"),
                &self.0.join("control.sqlite"),
            )
            .await
            .unwrap()
            .unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    async fn accept(
        Extension(node): Extension<ClusterControl>,
        Json(snapshot): Json<Snapshot>,
    ) -> Json<bool> {
        node.receive(snapshot).await.unwrap();
        node.mark_ready().await.unwrap();
        Json(true)
    }
    async fn snapshot(Extension(node): Extension<ClusterControl>) -> Json<Snapshot> {
        Json(node.snapshot().await.unwrap())
    }
    async fn mutate(
        Extension(node): Extension<ClusterControl>,
        Json(op): Json<Mutation>,
    ) -> Json<bool> {
        Json(node.mutate(op).await.unwrap())
    }
    #[tokio::test]
    async fn independent_reads_survive_peer_loss_and_pending_policy_catches_up() {
        let primary = Fixture::new();
        primary.seed();
        let secondary = Fixture::new();
        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner = primary
            .node(
                "owner",
                format!("http://{}/", listener_b.local_addr().unwrap()),
            )
            .await;
        let mirror = secondary
            .node(
                "replica",
                format!("http://{}/", listener_a.local_addr().unwrap()),
            )
            .await;
        assert!(
            mirror.check().await.is_err(),
            "fresh mirrors must not admit reads before initial state"
        );
        let app_b = Router::new()
            .route("/internal/v1/control-state", post(accept))
            .layer(Extension(mirror.clone()));
        let b = tokio::spawn(async move { axum::serve(listener_b, app_b).await.unwrap() });
        let app_a = Router::new()
            .route("/internal/v1/control-state", get(snapshot))
            .route("/internal/v1/control-mutation", post(mutate))
            .layer(Extension(owner.clone()));
        let a = tokio::spawn(async move { axum::serve(listener_a, app_a).await.unwrap() });
        owner.push().await.unwrap();
        mirror.check().await.unwrap();
        mirror.set_ignored("fixture", true).await.unwrap();
        assert!(owner.policy("fixture").await.unwrap().ignored);
        assert!(mirror.policy("fixture").await.unwrap().ignored);
        let cutoff = mirror.policy("fixture").await.unwrap().purge_ms;
        owner.purge("fixture", cutoff + 10).await.unwrap();
        owner.purge("fixture", cutoff - 10).await.unwrap();
        assert_eq!(
            mirror.policy("fixture").await.unwrap().purge_ms,
            cutoff + 10
        );
        let auth = UserAuthorization {
            access_token: "synthetic-session".into(),
            twitch_token: TwitchUserAccessToken {
                access_token: "synthetic-twitch".into(),
                refresh_token: "synthetic-refresh".into(),
            },
            twitch_authorization_last_validated: Utc::now(),
            valid_until: Utc::now() + chrono::Duration::days(1),
            user_id: "1".into(),
            user_login: "fixture".into(),
            user_name: "Fixture".into(),
            user_profile_image_url: String::new(),
        };
        owner.save_auth(&auth, true).await.unwrap();
        assert!(mirror.auth(&auth.access_token).await.unwrap().is_some());
        mirror.revoke(&auth.access_token).await.unwrap();
        assert!(owner.auth(&auth.access_token).await.unwrap().is_none());
        owner.set_pin("pinned", true, 100).await.unwrap();
        assert_eq!(mirror.pins().await.unwrap(), vec!["pinned"]);
        b.abort();
        let _ = b.await;
        assert!(
            owner.set_ignored("fixture", false).await.is_err(),
            "unacknowledged owner changes must report failure/pending"
        );
        owner.check().await.unwrap();
        mirror.check().await.unwrap();
        assert!(!owner.policy("fixture").await.unwrap().ignored);
        assert!(
            mirror.policy("fixture").await.unwrap().ignored,
            "disconnected mirrors retain last applied policy"
        );
        mirror
            .receive(owner.snapshot().await.unwrap())
            .await
            .unwrap();
        mirror.mark_ready().await.unwrap();
        assert!(!mirror.policy("fixture").await.unwrap().ignored);
        a.abort();
        let _ = a.await;
        mirror.check().await.unwrap();
        assert_eq!(mirror.pins().await.unwrap(), vec!["pinned"]);
    }
    #[tokio::test]
    async fn every_configured_mirror_must_acknowledge_before_success() {
        let primary = Fixture::new();
        primary.seed();
        let f1 = Fixture::new();
        let f2 = Fixture::new();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = l1.local_addr().unwrap();
        let addr2 = l2.local_addr().unwrap();
        let url1 = format!("http://{addr1}/");
        let url2 = format!("http://{addr2}/");
        let owner = ClusterControl::from_config(
            &ClusterConfig {
                role: "owner".into(),
                peer_url: url1.clone(),
                replica_urls: vec![url1, url2],
                ..Default::default()
            },
            Some("synthetic-peer-token-at-least-32-characters"),
            &primary.0.join("control.sqlite"),
        )
        .await
        .unwrap()
        .unwrap();
        let m1 = f1.node("replica", "http://127.0.0.1:1/".into()).await;
        let m2 = f2.node("replica", "http://127.0.0.1:1/".into()).await;
        let route = |m: ClusterControl| {
            Router::new()
                .route("/internal/v1/control-state", post(accept))
                .layer(Extension(m))
        };
        let app1 = route(m1.clone());
        let app2 = route(m2.clone());
        let h1 = tokio::spawn(async move { axum::serve(l1, app1).await.unwrap() });
        let h2 = tokio::spawn(async move { axum::serve(l2, app2).await.unwrap() });
        owner.set_ignored("fixture", true).await.unwrap();
        assert!(m1.policy("fixture").await.unwrap().ignored);
        assert!(m2.policy("fixture").await.unwrap().ignored);
        owner.revoke("already-missing-session").await.unwrap();
        h2.abort();
        let _ = h2.await;
        assert!(
            owner.set_ignored("fixture", false).await.is_err(),
            "one surviving mirror is insufficient to acknowledge a change for all serving nodes"
        );
        owner.check().await.unwrap();
        m1.check().await.unwrap();
        m2.check().await.unwrap();
        let l2 = tokio::net::TcpListener::bind(addr2).await.unwrap();
        let app2 = route(m2.clone());
        let h2 = tokio::spawn(async move { axum::serve(l2, app2).await.unwrap() });
        owner.push().await.unwrap();
        assert!(!m1.policy("fixture").await.unwrap().ignored);
        assert!(!m2.policy("fixture").await.unwrap().ignored);
        h1.abort();
        h2.abort();
        let _ = h1.await;
        let _ = h2.await;
    }
    #[tokio::test]
    async fn stale_snapshots_and_epoch_changes_cannot_resurrect_policy() {
        let primary = Fixture::new();
        primary.seed();
        let secondary = Fixture::new();
        let owner = primary.node("owner", "http://127.0.0.1:1/".into()).await;
        let mirror = secondary
            .node("replica", "http://127.0.0.1:1/".into())
            .await;
        let old = owner.snapshot().await.unwrap();
        owner.purge_local("fixture", 100).await.unwrap();
        mirror
            .receive(owner.snapshot().await.unwrap())
            .await
            .unwrap();
        mirror.mark_ready().await.unwrap();
        assert!(mirror.receive(old).await.is_err());
        let mut foreign = owner.snapshot().await.unwrap();
        foreign.epoch = "other".into();
        foreign.revision += 1;
        assert!(mirror.receive(foreign).await.is_err());
        let mut regressed = owner.snapshot().await.unwrap();
        regressed.revision += 1;
        regressed.channels[0][3] = json!(99);
        assert!(mirror.receive(regressed).await.is_err());
        assert!(
            !mirror
                .receive(owner.snapshot().await.unwrap())
                .await
                .unwrap(),
            "duplicate snapshot is idempotent"
        );
        mirror.check().await.unwrap();
        assert_eq!(mirror.policy("fixture").await.unwrap().purge_ms, 100);
    }
}

#[cfg(test)]
#[path = "cluster_consensus_tests.rs"]
mod consensus_tests;
