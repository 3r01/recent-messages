use crate::web::get_recent_messages::GetRecentMessagesQueryOptions;
use chrono::{DateTime, TimeZone, Utc};
use humantime::format_duration;
use itertools::Itertools;
use recent_messages2::storage::CanonicalRecord;
use std::convert::TryFrom;
use std::{collections::HashSet, sync::LazyLock};
use twitch_irc::message::{
    AsRawIRC, ClearChatAction, ClearMsgMessage, IRCMessage, IRCPrefix, IRCTags, NoticeMessage,
    ServerMessage,
};

#[derive(Debug)]
struct ContainerFrame {
    /// The original message that was received from IRC.
    original_message: ServerMessage,

    /// Time when the recent-messages service received this message. Gets converted
    /// to `rm-received-ts` on export
    time_received: DateTime<Utc>,

    /// Whether this message is marked "deleted" due to a `CLEARCHAT` or `CLEARMSG` message.
    /// Gets converted to `rm-deleted=1` on export.
    deleted_by_moderation: bool,
}

#[derive(Debug, Clone)]
struct StoredMessage {
    time_received: DateTime<Utc>,
    message_source: String,
}

impl ContainerFrame {
    fn export(self, options: &GetRecentMessagesQueryOptions) -> Option<String> {
        if options.hide_moderated_messages && self.deleted_by_moderation {
            return None;
        }

        if options.hide_moderation_messages
            && matches!(
                self.original_message,
                ServerMessage::ClearChat(_) | ServerMessage::ClearMsg(_)
            )
        {
            return None;
        }

        let mut message_to_export = if options.clearchat_to_notice {
            if let ServerMessage::ClearChat(clearchat_msg) = self.original_message {
                let (message, extra_tag) = match clearchat_msg.action {
                    ClearChatAction::ChatCleared => (
                        "Chat has been cleared by a moderator.".to_owned(),
                        "rm-clearchat".to_owned(),
                    ),
                    ClearChatAction::UserTimedOut {
                        user_login,
                        timeout_length,
                        ..
                    } => (
                        format!(
                            "{} has been timed out for {}.",
                            user_login,
                            format_duration(timeout_length)
                        ),
                        "rm-timeout".to_owned(),
                    ),
                    ClearChatAction::UserBanned { user_login, .. } => (
                        format!("{user_login} has been permanently banned."),
                        "rm-permaban".to_owned(),
                    ),
                };

                let mut tags = IRCTags::new();
                // @msg-id=rm-clearchat/rm-timeout/rm-permaban
                tags.0.insert("msg-id".to_owned(), extra_tag);

                // @msg-id=rm-timeout :tmi.twitch.tv NOTICE #channel :a_bad_user has been timed out for 5m 2s.
                IRCMessage::new(
                    tags,
                    Some(IRCPrefix::HostOnly {
                        host: "tmi.twitch.tv".to_owned(),
                    }),
                    "NOTICE".to_owned(),
                    vec![format!("#{}", clearchat_msg.channel_login), message],
                )
            } else {
                IRCMessage::from(self.original_message)
            }
        } else {
            IRCMessage::from(self.original_message)
        };

        // Add historical=1
        message_to_export
            .tags
            .0
            .insert("historical".to_owned(), "1".to_owned());
        // Add rm-received-ts=<timestamp>
        message_to_export.tags.0.insert(
            "rm-received-ts".to_owned(),
            self.time_received.timestamp_millis().to_string(),
        );

        // Add rm-deleted=1 if needed
        if self.deleted_by_moderation {
            message_to_export
                .tags
                .0
                .insert("rm-deleted".to_owned(), "1".to_owned());
        }

        Some(stable_raw_irc(message_to_export))
    }
}

fn stable_raw_irc(mut message: IRCMessage) -> String {
    let mut tags = std::mem::take(&mut message.tags.0)
        .into_iter()
        .collect::<Vec<_>>();
    tags.sort_by(|left, right| left.0.cmp(&right.0));
    if tags.is_empty() {
        return message.as_raw_irc();
    }
    let tags = tags
        .into_iter()
        .map(|(key, value)| {
            if value.is_empty() {
                key
            } else {
                format!("{key}={}", encode_tag_value(&value))
            }
        })
        .join(";");
    format!("@{tags} {}", message.as_raw_irc())
}

fn encode_tag_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            ';' => encoded.push_str("\\:"),
            ' ' => encoded.push_str("\\s"),
            '\\' => encoded.push_str("\\\\"),
            '\r' => encoded.push_str("\\r"),
            '\n' => encoded.push_str("\\n"),
            other => encoded.push(other),
        }
    }
    encoded
}

#[derive(Debug)]
struct MessageContainer {
    options: GetRecentMessagesQueryOptions,
    frames: Vec<ContainerFrame>,
}

static IGNORED_NOTICE_IDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "no_permission",
        "host_on",
        "host_off",
        "host_target_went_offline",
        "msg_channel_suspended",
    ]
    .into_iter()
    .collect()
});

impl MessageContainer {
    pub fn append_stored_msg(&mut self, message: &StoredMessage) {
        // parse the retrieved source back into a struct
        let Ok(parsed_message) = IRCMessage::parse(&message.message_source) else {
            return;
        };
        let Ok(server_message) = ServerMessage::try_from(parsed_message) else {
            return;
        };

        // we export PRIVMSG, CLEARCHAT, CLEARMSG, USERNOTICE, NOTICE and ROOMSTATE
        if !matches!(
            server_message,
            ServerMessage::Privmsg(_)
                | ServerMessage::ClearChat(_)
                | ServerMessage::ClearMsg(_)
                | ServerMessage::UserNotice(_)
                | ServerMessage::Notice(_)
                | ServerMessage::RoomState(_)
        ) {
            return;
        }

        // apply `deleted_by_moderation` flag
        match &server_message {
            ServerMessage::ClearChat(clearchat_msg) => match &clearchat_msg.action {
                ClearChatAction::ChatCleared => {
                    self.frames
                        .iter_mut()
                        .for_each(|frame| frame.deleted_by_moderation = true);
                }
                ClearChatAction::UserTimedOut { user_id, .. }
                | ClearChatAction::UserBanned { user_id, .. } => {
                    self.frames
                        .iter_mut()
                        .filter(|frame| match &frame.original_message {
                            ServerMessage::Privmsg(msg) => &msg.sender.id == user_id,
                            ServerMessage::UserNotice(msg) => &msg.sender.id == user_id,
                            _ => false,
                        })
                        .for_each(|frame| frame.deleted_by_moderation = true);
                }
            },
            ServerMessage::ClearMsg(ClearMsgMessage { message_id, .. }) => {
                self.frames
                    .iter_mut()
                    .filter(|frame| match &frame.original_message {
                        ServerMessage::Privmsg(msg) => &msg.message_id == message_id,
                        ServerMessage::UserNotice(msg) => &msg.message_id == message_id,
                        _ => false,
                    })
                    .for_each(|frame| frame.deleted_by_moderation = true);
            }
            ServerMessage::Notice(NoticeMessage {
                message_id: Some(message_id),
                ..
            }) if IGNORED_NOTICE_IDS.contains(&message_id.as_str()) => {
                // Don't export ignored NOTICE types
                return;
            }
            _ => {}
        }

        // rest of the options are handled during the `export()` call

        let frame = ContainerFrame {
            original_message: server_message,
            time_received: message.time_received,
            deleted_by_moderation: false,
        };
        self.frames.push(frame);
    }

    pub fn export(self) -> Vec<String> {
        let MessageContainer { frames, options } = self;
        frames
            .into_iter()
            .filter_map(|frame| frame.export(&options))
            .collect_vec()
    }
}

/// Processes the stored message and applies the options specified by `options`.
fn export_stored_messages(
    stored_messages: Vec<StoredMessage>,
    options: GetRecentMessagesQueryOptions,
) -> Vec<String> {
    let mut container = MessageContainer {
        options,
        frames: vec![],
    };

    for stored_message in stored_messages {
        container.append_stored_msg(&stored_message);
    }

    container.export()
}

pub fn export_canonical_records(
    records: Vec<CanonicalRecord>,
    options: GetRecentMessagesQueryOptions,
) -> Vec<String> {
    let stored = records
        .into_iter()
        .filter_map(|record| {
            Some(StoredMessage {
                time_received: Utc.timestamp_millis_opt(record.received_at_ms).single()?,
                message_source: String::from_utf8(record.raw_irc).ok()?,
            })
        })
        .collect();
    export_stored_messages(stored, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const RECEIVED_AT_MS: i64 = 1_700_000_000_123;
    const MESSAGE_ID: &str = "00000000-0000-0000-0000-000000000001";

    fn stored(message_source: impl Into<String>) -> StoredMessage {
        StoredMessage {
            time_received: Utc.timestamp_millis_opt(RECEIVED_AT_MS).unwrap(),
            message_source: message_source.into(),
        }
    }

    fn privmsg() -> StoredMessage {
        stored(format!(
            "@badge-info=;badges=;color=#123456;display-name=TestUser;emotes=;first-msg=0;flags=;id={MESSAGE_ID};mod=0;room-id=123;subscriber=0;tmi-sent-ts=1699999999000;turbo=0;user-id=456;user-type= :testuser!testuser@testuser.tmi.twitch.tv PRIVMSG #testchannel :hello world"
        ))
    }

    fn clearmsg() -> StoredMessage {
        stored(format!(
            "@login=testuser;room-id=123;target-msg-id={MESSAGE_ID};tmi-sent-ts=1700000000000 :tmi.twitch.tv CLEARMSG #testchannel :hello world"
        ))
    }

    fn timeout() -> StoredMessage {
        stored(
            "@ban-duration=300;room-id=123;target-user-id=456;tmi-sent-ts=1700000000000 :tmi.twitch.tv CLEARCHAT #testchannel :testuser",
        )
    }

    fn permanent_ban() -> StoredMessage {
        stored(
            "@room-id=123;target-user-id=456;tmi-sent-ts=1700000000000 :tmi.twitch.tv CLEARCHAT #testchannel :testuser",
        )
    }

    fn chat_clear() -> StoredMessage {
        stored("@room-id=123;tmi-sent-ts=1700000000000 :tmi.twitch.tv CLEARCHAT #testchannel")
    }

    #[test]
    fn exports_compatible_historical_metadata() {
        let messages = export_stored_messages(vec![privmsg()], Default::default());

        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("historical=1"));
        assert!(messages[0].contains("rm-received-ts=1700000000123"));
        assert!(!messages[0].contains("rm-deleted=1"));
        assert!(messages[0].ends_with(" PRIVMSG #testchannel :hello world"));
    }

    #[test]
    fn clearmsg_marks_the_target_and_is_exported() {
        let messages = export_stored_messages(vec![privmsg(), clearmsg()], Default::default());

        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains("rm-deleted=1"));
        assert!(messages[1].contains(" CLEARMSG #testchannel :hello world"));
    }

    #[test]
    fn moderation_filter_alias_semantics_are_independent() {
        let hide_deleted = GetRecentMessagesQueryOptions {
            hide_moderated_messages: true,
            ..Default::default()
        };
        let messages = export_stored_messages(vec![privmsg(), clearmsg()], hide_deleted);
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains(" CLEARMSG #testchannel :hello world"));

        let hide_commands = GetRecentMessagesQueryOptions {
            hide_moderation_messages: true,
            ..Default::default()
        };
        let messages = export_stored_messages(vec![privmsg(), clearmsg()], hide_commands);
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("rm-deleted=1"));
        assert!(messages[0].contains(" PRIVMSG #testchannel :hello world"));
    }

    #[test]
    fn converts_timeout_to_compatible_notice() {
        let options = GetRecentMessagesQueryOptions {
            clearchat_to_notice: true,
            ..Default::default()
        };
        let messages = export_stored_messages(vec![timeout()], options);

        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("msg-id=rm-timeout"));
        assert!(messages[0].contains(" NOTICE #testchannel :testuser has been timed out for 5m."));
    }

    #[test]
    fn chat_clear_marks_all_prior_messages_and_converts_to_notice() {
        let options = GetRecentMessagesQueryOptions {
            clearchat_to_notice: true,
            ..Default::default()
        };
        let messages = export_stored_messages(vec![privmsg(), chat_clear()], options);

        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains("rm-deleted=1"));
        assert!(messages[1].contains("msg-id=rm-clearchat"));
        assert!(
            messages[1].ends_with(" NOTICE #testchannel :Chat has been cleared by a moderator.")
        );
    }

    #[test]
    fn permanent_ban_marks_user_messages_and_converts_to_notice() {
        let options = GetRecentMessagesQueryOptions {
            clearchat_to_notice: true,
            ..Default::default()
        };
        let messages = export_stored_messages(vec![privmsg(), permanent_ban()], options);

        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains("rm-deleted=1"));
        assert!(messages[1].contains("msg-id=rm-permaban"));
        assert!(
            messages[1].ends_with(" NOTICE #testchannel :testuser has been permanently banned.")
        );
    }

    #[test]
    fn preserves_usernotice_reply_and_shared_chat_tags() {
        let usernotice = stored(
            "@badge-info=subscriber/0;badges=subscriber/0,premium/1;color=#8A2BE2;display-name=PilotChup;emotes=;flags=;id=c7ae5c7a-3007-4f9d-9e64-35219a5c1134;login=pilotchup;mod=0;msg-id=sub;msg-param-cumulative-months=1;msg-param-months=0;msg-param-should-share-streak=0;msg-param-sub-plan-name=Channel\\sSubscription\\s(xqcow);msg-param-sub-plan=Prime;room-id=71092938;subscriber=1;system-msg=PilotChup\\ssubscribed\\swith\\sTwitch\\sPrime.;tmi-sent-ts=1575162111790;user-id=40745007;user-type= :tmi.twitch.tv USERNOTICE #xqcow",
        );
        let reply = stored(
            "@badge-info=;badges=;client-nonce=cd56193132f934ac71b4d5ac488d4bd6;color=;display-name=LeftSwing;emotes=;first-msg=0;flags=;id=5b4f63a9-776f-4fce-bf3c-d9707f52e32d;mod=0;reply-parent-display-name=Retoon;reply-parent-msg-body=hello;reply-parent-msg-id=6b13e51b-7ecb-43b5-ba5b-2bb5288df696;reply-parent-user-id=37940952;reply-parent-user-login=retoon;returning-chatter=0;room-id=37940952;source-room-id=789;subscriber=0;tmi-sent-ts=1673925983585;turbo=0;user-id=133651738;user-type= :leftswing!leftswing@leftswing.tmi.twitch.tv PRIVMSG #retoon :@Retoon yes",
        );
        let messages = export_stored_messages(
            vec![usernotice, reply],
            GetRecentMessagesQueryOptions::default(),
        );

        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains(" USERNOTICE #xqcow"));
        assert!(messages[1].contains("reply-parent-msg-id=6b13e51b-7ecb-43b5-ba5b-2bb5288df696"));
        assert!(messages[1].contains("source-room-id=789"));
    }

    #[test]
    fn exports_tags_in_stable_order() {
        let first =
            export_stored_messages(vec![privmsg()], GetRecentMessagesQueryOptions::default());
        let second =
            export_stored_messages(vec![privmsg()], GetRecentMessagesQueryOptions::default());

        assert_eq!(first, second);
        assert!(first[0].starts_with("@badge-info;badges;"));
    }

    #[test]
    fn ignores_known_notice_types_and_malformed_records() {
        let ignored =
            stored("@msg-id=no_permission :tmi.twitch.tv NOTICE #testchannel :permission denied");
        let messages = export_stored_messages(
            vec![stored("not irc"), ignored, privmsg()],
            GetRecentMessagesQueryOptions::default(),
        );
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains(" PRIVMSG #testchannel :hello world"));
    }
}
