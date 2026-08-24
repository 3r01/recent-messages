use thiserror::Error;
use twitch_irc::message::IRCMessage;

use super::{CanonicalRecord, SourceFidelity};

const RETAINED_COMMANDS: &[&str] = &["PRIVMSG", "CLEARCHAT", "CLEARMSG", "USERNOTICE"];

#[derive(Debug, Error)]
pub enum RawIrcError {
    #[error("invalid IRC message: {0}")]
    Parse(String),
    #[error("IRC command is not retained: {0}")]
    UnsupportedCommand(String),
    #[error("retained IRC message has no valid channel")]
    MissingChannel,
    #[error("retained {command} has no stable identity; required {required}")]
    MissingIdentity {
        command: String,
        required: &'static str,
    },
}

pub fn canonicalize_raw_irc(
    raw: &str,
    received_at_ms: i64,
) -> Result<CanonicalRecord, RawIrcError> {
    canonicalize_raw_irc_from(raw, received_at_ms, "unknown", SourceFidelity::Unknown)
}

pub fn canonicalize_raw_irc_from(
    raw: &str,
    received_at_ms: i64,
    source_id: &str,
    fidelity: SourceFidelity,
) -> Result<CanonicalRecord, RawIrcError> {
    let raw = raw.trim_end_matches(['\r', '\n']);
    let message = IRCMessage::parse(raw).map_err(|error| RawIrcError::Parse(error.to_string()))?;
    if !RETAINED_COMMANDS.contains(&message.command.as_str()) {
        return Err(RawIrcError::UnsupportedCommand(message.command));
    }
    let channel_key = message
        .params
        .first()
        .and_then(|channel| normalize_channel(channel))
        .ok_or(RawIrcError::MissingChannel)?;
    validate_identity(&message)?;
    let event_at_ms = message
        .tags
        .0
        .get("tmi-sent-ts")
        .and_then(|timestamp| timestamp.parse().ok())
        .unwrap_or(received_at_ms);
    let received_at_ms = message
        .tags
        .0
        .get("rm-received-ts")
        .and_then(|timestamp| timestamp.parse().ok())
        .unwrap_or(received_at_ms);
    let event_key = event_key(&message, &channel_key);
    Ok(CanonicalRecord {
        channel_key,
        event_at_ms,
        received_at_ms,
        event_key,
        source_id: source_id.to_owned(),
        fidelity,
        raw_irc: raw.as_bytes().to_vec(),
    })
}

fn validate_identity(message: &IRCMessage) -> Result<(), RawIrcError> {
    let required = match message.command.as_str() {
        "PRIVMSG" | "USERNOTICE" if !has_tag(message, "id") => Some("id"),
        "CLEARMSG" if !has_tag(message, "target-msg-id") => Some("target-msg-id"),
        _ if !has_tag(message, "tmi-sent-ts") => Some("tmi-sent-ts"),
        "CLEARCHAT" if message.params.len() > 1 && !has_tag(message, "target-user-id") => {
            Some("target-user-id for a user-specific clear")
        }
        _ => None,
    };
    if let Some(required) = required {
        return Err(RawIrcError::MissingIdentity {
            command: message.command.clone(),
            required,
        });
    }
    Ok(())
}

fn has_tag(message: &IRCMessage, name: &str) -> bool {
    message
        .tags
        .0
        .get(name)
        .is_some_and(|value| !value.is_empty())
}

fn normalize_channel(channel: &str) -> Option<String> {
    let channel = channel.trim_start_matches('#').to_ascii_lowercase();
    (!channel.is_empty()
        && channel
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'))
    .then_some(channel)
}

fn event_key(message: &IRCMessage, channel: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    add_part(&mut hasher, b"twitch-irc-event-v1");
    add_part(&mut hasher, message.command.as_bytes());
    add_part(&mut hasher, channel.as_bytes());
    if let Some(id) = message.tags.0.get("id").filter(|id| !id.is_empty()) {
        add_part(&mut hasher, b"id");
        add_part(&mut hasher, id.as_bytes());
        return *hasher.finalize().as_bytes();
    }
    for name in [
        "tmi-sent-ts",
        "target-msg-id",
        "target-user-id",
        "ban-duration",
        "source-id",
    ] {
        if let Some(value) = message.tags.0.get(name) {
            add_part(&mut hasher, name.as_bytes());
            add_part(&mut hasher, value.as_bytes());
        }
    }
    for parameter in message.params.iter().skip(1) {
        add_part(&mut hasher, parameter.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn add_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_privmsg_and_uses_twitch_timestamp() {
        let raw = "@id=abc;tmi-sent-ts=1700000000000 :user!user@user.tmi.twitch.tv PRIVMSG #Example :hello";
        let record = canonicalize_raw_irc(raw, 1_700_000_000_100).unwrap();
        assert_eq!(record.channel_key, "example");
        assert_eq!(record.event_at_ms, 1_700_000_000_000);
        assert_eq!(record.raw_irc, raw.as_bytes());
    }

    #[test]
    fn rejects_id_bearing_events_without_a_source_stable_timestamp() {
        let raw = "@id=abc :user!user@user.tmi.twitch.tv PRIVMSG #example :hello";
        assert!(matches!(
            canonicalize_raw_irc(raw, 1_700_000_000_100),
            Err(RawIrcError::MissingIdentity {
                required: "tmi-sent-ts",
                ..
            })
        ));
    }

    #[test]
    fn uses_recent_messages_receipt_timestamp_when_present() {
        let raw = "@id=abc;rm-received-ts=1700000000050;tmi-sent-ts=1700000000000 :user!user@user.tmi.twitch.tv PRIVMSG #example :hello";
        let record = canonicalize_raw_irc_from(
            raw,
            1_700_000_000_100,
            "repair",
            SourceFidelity::Reconstructed,
        )
        .unwrap();
        assert_eq!(record.event_at_ms, 1_700_000_000_000);
        assert_eq!(record.received_at_ms, 1_700_000_000_050);
    }

    #[test]
    fn tag_order_does_not_change_event_identity() {
        let first = canonicalize_raw_irc(
            "@room-id=1;target-user-id=2;tmi-sent-ts=3 :tmi.twitch.tv CLEARCHAT #channel :user",
            4,
        )
        .unwrap();
        let second = canonicalize_raw_irc(
            "@tmi-sent-ts=3;target-user-id=2;room-id=1 :tmi.twitch.tv CLEARCHAT #channel :user",
            5,
        )
        .unwrap();
        assert_eq!(first.event_key, second.event_key);
        assert_ne!(first.raw_irc, second.raw_irc);
    }

    #[test]
    fn retains_clearmsg_and_usernotice() {
        for raw in [
            "@target-msg-id=abc;tmi-sent-ts=1 :tmi.twitch.tv CLEARMSG #channel :deleted",
            "@id=notice;tmi-sent-ts=2 :tmi.twitch.tv USERNOTICE #channel",
        ] {
            canonicalize_raw_irc(raw, 3).unwrap();
        }
    }

    #[test]
    fn rejects_retained_events_without_stable_identity() {
        for raw in [
            ":user!user@user.tmi.twitch.tv PRIVMSG #channel :hello",
            "@tmi-sent-ts=1 :tmi.twitch.tv USERNOTICE #channel",
            "@tmi-sent-ts=1 :tmi.twitch.tv CLEARMSG #channel :deleted",
            "@target-msg-id=abc :tmi.twitch.tv CLEARMSG #channel :deleted",
            "@target-user-id=2 :tmi.twitch.tv CLEARCHAT #channel :user",
            "@tmi-sent-ts=1 :tmi.twitch.tv CLEARCHAT #channel :user",
        ] {
            assert!(matches!(
                canonicalize_raw_irc(raw, 2),
                Err(RawIrcError::MissingIdentity { .. })
            ));
        }
    }

    #[test]
    fn accepts_timestamped_room_clear_without_a_target() {
        canonicalize_raw_irc(
            "@room-id=1;tmi-sent-ts=2 :tmi.twitch.tv CLEARCHAT #channel",
            3,
        )
        .unwrap();
    }

    #[test]
    fn cosmetic_source_differences_do_not_change_stable_id_keys() {
        for (direct, reconstructed) in [
            (
                "@id=abc;mod=0;tmi-sent-ts=1 :user!user@user.tmi.twitch.tv PRIVMSG #channel :hello",
                "@id=abc;tmi-sent-ts=1 :user!user@user.tmi.twitch.tv PRIVMSG #channel :hello",
            ),
            (
                "@id=notice;color=#fff;msg-id=sub;tmi-sent-ts=2 :tmi.twitch.tv USERNOTICE #channel",
                "@id=notice;msg-id=sub;tmi-sent-ts=2 :tmi.twitch.tv USERNOTICE #channel",
            ),
            (
                "@login=user;target-msg-id=message;tmi-sent-ts=3 :tmi.twitch.tv CLEARMSG #channel :deleted",
                "@target-msg-id=message;tmi-sent-ts=3 :tmi.twitch.tv CLEARMSG #channel :deleted",
            ),
        ] {
            let direct =
                canonicalize_raw_irc_from(direct, 10, "direct", SourceFidelity::DirectIrc).unwrap();
            let reconstructed = canonicalize_raw_irc_from(
                reconstructed,
                11,
                "firehose",
                SourceFidelity::Reconstructed,
            )
            .unwrap();
            assert_eq!(direct.event_key, reconstructed.event_key);
        }
    }

    #[test]
    fn rejects_non_history_commands() {
        let error = canonicalize_raw_irc("PING :tmi.twitch.tv", 1).unwrap_err();
        assert!(matches!(error, RawIrcError::UnsupportedCommand(_)));
    }
}
