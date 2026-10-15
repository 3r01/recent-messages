use super::*;
use std::process::{Child, Command, Stdio};

struct Fleet {
    root: std::path::PathBuf,
    nodes: Vec<Option<Child>>,
    http: Vec<String>,
    raft: Vec<String>,
    binary: String,
}
impl Fleet {
    fn new(binary: String) -> Self {
        let root = std::env::temp_dir().join(format!(
            "rm-consensus-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut http = Vec::new();
        let mut raft = Vec::new();
        let mut ports = Vec::new();
        for _ in 0..8 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            ports.push(listener);
        }
        for i in 0..4 {
            http.push(ports[i * 2].local_addr().unwrap().to_string());
            raft.push(ports[i * 2 + 1].local_addr().unwrap().to_string());
        }
        let auth = json!([{"username":"application","password":"synthetic-peer-token-at-least-32-characters","perms":["all"]}]);
        std::fs::write(root.join("auth.json"), serde_json::to_vec(&auth).unwrap()).unwrap();
        Self {
            root,
            nodes: Vec::new(),
            http,
            raft,
            binary,
        }
    }
    fn start(&mut self, i: usize) {
        let mut command = Command::new(&self.binary);
        command
            .args([
                "-node-id",
                &format!("peer-{i}"),
                "-http-addr",
                &self.http[i],
                "-raft-addr",
                &self.raft[i],
                "-auth",
            ])
            .arg(self.root.join("auth.json"))
            .args([
                "-join-as",
                "application",
                "-raft-cluster-remove-shutdown=false",
            ]);
        if i != 0 {
            command.args(["-join", &self.raft[0]]);
        }
        let log = std::fs::File::create(self.root.join(format!("peer-{i}.log"))).unwrap();
        command
            .arg(self.root.join(format!("data-{i}")))
            .stdout(Stdio::null())
            .stderr(log);
        let child = command.spawn().unwrap();
        if i < self.nodes.len() {
            self.nodes[i] = Some(child);
        } else {
            self.nodes.push(Some(child));
        }
    }
    fn stop(&mut self, i: usize) {
        if let Some(mut child) = self.nodes[i].take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        for child in self.nodes.iter_mut().filter_map(Option::as_mut) {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::thread::panicking() {
            for i in 0..self.nodes.len() {
                if let Ok(log) = std::fs::read_to_string(self.root.join(format!("peer-{i}.log"))) {
                    eprintln!(
                        "peer-{i}: {}",
                        log.lines()
                            .rev()
                            .take(12)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>()
                            .join("\n")
                    );
                }
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn eventually(control: &ClusterControl) {
    for _ in 0..100 {
        if control.check().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("policy majority did not become ready");
}

#[tokio::test]
#[ignore = "requires RM_POLICY_TEST_BINARY pointing to the pinned rqlited executable"]
async fn automatic_election_preserves_policy_and_rejects_minority() {
    let mut fleet = Fleet::new(std::env::var("RM_POLICY_TEST_BINARY").unwrap());
    fleet.start(0);
    let mut controls = Vec::new();
    for i in 0..4 {
        controls.push(
            ClusterControl::from_config(
                &ClusterConfig {
                    role: "consensus".into(),
                    authority_url: format!("http://{}/", fleet.http[i]),
                    request_timeout: std::time::Duration::from_secs(2),
                    ..Default::default()
                },
                Some("synthetic-peer-token-at-least-32-characters"),
                &fleet.root.join(format!("app-{i}.sqlite")),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    for _ in 0..100 {
        let seed = vec![
            json!([CHANNEL_SCHEMA]),
            json!([AUTH_SCHEMA]),
            json!([
                "CREATE TABLE IF NOT EXISTS rm_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)"
            ]),
            json!([
                "INSERT OR IGNORE INTO rm_metadata VALUES('epoch','election-fixture'),('revision','0'),('initialized','1'),('pins_bootstrapped','1')"
            ]),
        ];
        if controls[0]
            .authority_request("db/execute?transaction", seed)
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    eventually(&controls[0]).await;
    for i in 1..4 {
        fleet.start(i);
        eventually(&controls[i]).await;
    }
    controls[2].set_ignored("fixture", true).await.unwrap();
    controls[1]
        .purge("fixture", 9_000_000_000_000)
        .await
        .unwrap();
    controls[3].set_pin("pinned", true, 10).await.unwrap();
    fleet.stop(0); // This node bootstrapped the cluster and is its original leader.
    eventually(&controls[1]).await;
    controls[3].set_ignored("fixture", false).await.unwrap();
    assert_eq!(
        controls[1].policy("fixture").await.unwrap().purge_ms,
        9_000_000_000_000
    );
    assert_eq!(controls[2].pins().await.unwrap(), vec!["pinned"]);
    fleet.stop(3);
    assert!(controls[1].check().await.is_err());
    assert!(
        controls[2]
            .set_pin("must-not-commit", true, 10)
            .await
            .is_err()
    );
    fleet.start(3);
    eventually(&controls[1]).await;
    assert!(!controls[3].policy("fixture").await.unwrap().ignored);
    assert_eq!(controls[1].pins().await.unwrap(), vec!["pinned"]);
}

#[tokio::test]
async fn authority_queries_are_strong_and_sql_errors_fail_closed() {
    use axum::{Extension, Json, Router, extract::OriginalUri, routing::post};
    use std::sync::atomic::{AtomicBool, Ordering};
    async fn query(
        headers: http::HeaderMap,
        OriginalUri(uri): OriginalUri,
        Extension(fail): Extension<Arc<AtomicBool>>,
        Json(statements): Json<Value>,
    ) -> Json<Value> {
        assert!(
            headers[http::header::AUTHORIZATION]
                .to_str()
                .unwrap()
                .starts_with("Basic ")
        );
        assert!(uri.query().unwrap().contains("level=strong"));
        assert!(uri.query().unwrap().contains("transaction"));
        assert_eq!(
            statements[0][0],
            "SELECT value FROM rm_metadata WHERE key='initialized'"
        );
        if fail.load(Ordering::SeqCst) {
            Json(json!({"results":[{"error_v2":{"message":"synthetic database failure"}}]}))
        } else {
            Json(json!({"results":[{"columns":["value"],"values":[["1"]]}]}))
        }
    }
    let root =
        std::env::temp_dir().join(format!("rm-authority-protocol-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&root).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let fail = Arc::new(AtomicBool::new(false));
    let app = Router::new()
        .route("/db/query", post(query))
        .layer(Extension(fail.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let control = ClusterControl::from_config(
        &ClusterConfig {
            role: "consensus".into(),
            authority_url: url,
            ..Default::default()
        },
        Some("synthetic-peer-token-at-least-32-characters"),
        &root.join("control.sqlite"),
    )
    .await
    .unwrap()
    .unwrap();
    control.check().await.unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(
        control.check().await.is_err(),
        "HTTP 200 with a database error must not admit traffic"
    );
    server.abort();
    let _ = server.await;
    assert!(
        control.check().await.is_err(),
        "a lost policy authority cannot admit stale reads"
    );
    let _ = std::fs::remove_dir_all(root);
}
