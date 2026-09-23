use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::CockatielClient;
use cockatiel_client::proto::{container::Payload, *};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    #[serde(default = "default_language")]
    language: String,
    #[serde(default)]
    custom_ranges: Vec<[u32; 2]>,
    #[serde(default)]
    allow_emoji: bool,
    #[serde(default)]
    allow_expressive: bool,
    #[serde(default = "default_reason")]
    flag_reason: String,
}

fn default_language() -> String {
    "latin".to_string()
}

fn default_reason() -> String {
    "out-of-language".to_string()
}

fn load_config() -> Config {
    std::fs::read_to_string("config.json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| {
            let default = Config {
                language: default_language(),
                custom_ranges: Vec::new(),
                allow_emoji: false,
                allow_expressive: false,
                flag_reason: default_reason(),
            };
            if let Ok(pretty) = serde_json::to_string_pretty(&default) {
                let _ = std::fs::write("config.json", pretty);
            }
            default
        })
}

#[derive(Debug, Clone, Deserialize)]
struct Languages {
    #[serde(default)]
    presets: HashMap<String, Vec<[u32; 2]>>,
}

fn load_languages() -> HashMap<String, Vec<[u32; 2]>> {
    std::fs::read_to_string("languages.json")
        .ok()
        .and_then(|s| serde_json::from_str::<Languages>(&s).ok())
        .map(|l| l.presets)
        .unwrap_or_default()
}

/// Common emoji Unicode ranges.
fn is_emoji(c: char) -> bool {
    let cp = c as u32;
    (0x1F300..=0x1F5FF).contains(&cp)
        || (0x1F600..=0x1F64F).contains(&cp)
        || (0x1F680..=0x1F6FF).contains(&cp)
        || (0x1F900..=0x1F9FF).contains(&cp)
        || (0x1FA70..=0x1FAFF).contains(&cp)
        || (0x2600..=0x27BF).contains(&cp)
        || (0xFE00..=0xFE0F).contains(&cp)
        || (0x1F000..=0x1F0FF).contains(&cp)
}

/// Characters commonly used for expressive/emote messages (e.g. ඞ, (๑ > ᴗ < ๑)).
fn is_expressive(c: char) -> bool {
    matches!(
        c,
        'ඞ' | '๑'
            | 'ᴗ'
            | 'ಠ'
            | '益'
            | 'ʕ'
            | 'ʔ'
            | '⊙'
            | '☉'
            | '♥'
            | '♡'
            | '✧'
            | '❀'
            | '★'
            | '☆'
    )
}

fn in_ranges(c: char, ranges: &[[u32; 2]]) -> bool {
    let cp = c as u32;
    ranges.iter().any(|r| cp >= r[0] && cp <= r[1])
}

fn violates_language(
    message: &str,
    ranges: &[[u32; 2]],
    allow_emoji: bool,
    allow_expressive: bool,
) -> bool {
    for c in message.chars() {
        if c.is_whitespace() || in_ranges(c, ranges) {
            continue;
        }
        if allow_emoji && is_emoji(c) {
            continue;
        }
        if allow_expressive && is_expressive(c) {
            continue;
        }
        return true;
    }
    false
}

/// Build the ChatMessageRejected record so the engine logs the rejection
/// clearly (reason + original raw message + what it became).
fn compose_rejected(
    message_uuid7: &str,
    chat: &ChatMessage,
    processed: &str,
    reason: &str,
    origin: &str,
) -> ChatMessageRejected {
    ChatMessageRejected {
        message_uuid7: message_uuid7.to_string(),
        message: Some(chat.clone()),
        processed_message: processed.to_string(),
        reason: reason.to_string(),
        origin: origin.to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let config = load_config();
    let all_langs = load_languages();
    let mut ranges = all_langs.get(&config.language).cloned().unwrap_or_default();
    ranges.extend(config.custom_ranges.clone());
    info!(
        "Language-constrainer active: language='{}' ({} ranges, emoji={}, expressive={})",
        config.language,
        ranges.len(),
        config.allow_emoji,
        config.allow_expressive
    );

    let client = CockatielClient::connect("language_constrainer.json").await?;
    let (mut write, mut read) = client.stream.split();
    let auth_token = client.auth_token.clone();
    let instance_uuid = client.instance_uuid7.clone();
    let module_name = client.config.module_name.clone();

    while let Some(msg) = read.next().await {
        let Ok(WsMessage::Binary(data)) = msg else {
            continue;
        };
        let Ok(container) = Container::decode(data.as_ref()) else {
            continue;
        };

        match container.payload {
            Some(Payload::MessagePreProcess(pre)) => {
                let Some(chat) = &pre.raw_message else {
                    continue;
                };
                let uuid = pre.message_uuid7.clone();
                let original = chat.raw_message.clone();

                // Out-of-language messages are held for audit: the engine
                // broadcasts an audit prompt (Approve/Reject) to connected UIs.
                if violates_language(
                    &original,
                    &ranges,
                    config.allow_emoji,
                    config.allow_expressive,
                ) {
                    warn!(
                        "Message [{}] violates language '{}' — flagging for audit",
                        uuid, config.language
                    );
                    let flag = Container {
                        version: 1,
                        auth_token: auth_token.clone(),
                        module_name: module_name.clone(),
                        module_instance_uuid7: instance_uuid.clone(),
                        payload: Some(Payload::AuditFlag(AuditFlag {
                            message_uuid7: uuid.clone(),
                            reason: config.flag_reason.clone(),
                            origin: module_name.clone(),
                        })),
                    };
                    let mut buf = Vec::new();
                    if flag.encode(&mut buf).is_ok() {
                        let _ = write.send(WsMessage::Binary(buf.into())).await;
                    }
                    // Also record the rejection clearly (raw + reason), like the
                    // banned-words module — the engine logs + persists it as a
                    // searchable `chat_rejected` timeline record.
                    let reason = format!("violates language '{}'", config.language);
                    let rej = compose_rejected(&uuid, chat, &original, &reason, &module_name);
                    let rej = Container {
                        version: 1,
                        auth_token: auth_token.clone(),
                        module_name: module_name.clone(),
                        module_instance_uuid7: instance_uuid.clone(),
                        payload: Some(Payload::ChatMessageRejected(rej)),
                    };
                    let mut buf = Vec::new();
                    if rej.encode(&mut buf).is_ok() {
                        let _ = write.send(WsMessage::Binary(buf.into())).await;
                    }
                }

                // Ack pre_process (content preserved — the audit hold prevents
                // it from being shown until a moderator releases it).
                let reply = Container {
                    version: 1,
                    auth_token: auth_token.clone(),
                    module_name: module_name.clone(),
                    module_instance_uuid7: instance_uuid.clone(),
                    payload: Some(Payload::MessagePreProcess(MessagePreProcess {
audio: Vec::new(),
                            audio_type: String::new(),
                        message_uuid7: uuid,
                        raw_message: Some(ChatMessage {
                            platform: chat.platform.clone(),
                            raw_data: chat.raw_data.clone(),
                            raw_message: original,
                            user_uuid7: chat.user_uuid7.clone(),
                            command: chat.command.clone(),
                            channel_id: chat.channel_id.clone(),
                            user_data: chat.user_data.clone(),
                        }),
                    })),
                };
                let mut buf = Vec::new();
                if reply.encode(&mut buf).is_ok() {
                    let _ = write.send(WsMessage::Binary(buf.into())).await;
                }
            }
            _ => {}
        }
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    const LATIN: [[u32; 2]; 1] = [[0x41, 0x5A]]; // A-Z only

    #[test]
    fn compose_rejected_carries_reason_raw_origin() {
        let chat = ChatMessage {
            platform: "twitch".into(),
            raw_data: vec![],
            raw_message: "Привет".into(),
            user_uuid7: "u1".into(),
            command: None,
            user_data: None,
            channel_id: "chan".into(),
        };
        let rej = compose_rejected("uuid-9", &chat, "Привет", "violates language 'latin'", "language-constrainer");
        assert_eq!(rej.message_uuid7, "uuid-9");
        assert_eq!(rej.origin, "language-constrainer");
        assert_eq!(rej.reason, "violates language 'latin'");
        assert_eq!(rej.processed_message, "Привет");
        assert_eq!(rej.message.unwrap().raw_message, "Привет");
    }

    #[test]
    fn ascii_uppercase_pass() {
        assert!(!violates_language("HELLO", &LATIN, false, false));
        assert!(!violates_language("HELLO WORLD", &LATIN, false, false));
    }

    #[test]
    fn lowercase_fails_outside_range() {
        assert!(violates_language("hello", &LATIN, false, false));
    }

    #[test]
    fn non_latin_fails() {
        assert!(violates_language("привет", &LATIN, false, false));
        assert!(violates_language("HÉLLO", &LATIN, false, false));
    }

    #[test]
    fn emoji_gated_by_flag() {
        assert!(violates_language("HELLO 👍", &LATIN, false, false));
        assert!(!violates_language("HELLO 👍", &LATIN, true, false));
    }

    #[test]
    fn expressive_gated_by_flag() {
        assert!(violates_language("HELLO ඞ", &LATIN, false, false));
        assert!(!violates_language("HELLO ඞ", &LATIN, false, true));
    }

    #[test]
    fn ranges_match() {
        assert!(in_ranges('A', &LATIN));
        assert!(in_ranges('Z', &LATIN));
        assert!(!in_ranges('a', &LATIN));
        assert!(!in_ranges('1', &LATIN));
    }
}
