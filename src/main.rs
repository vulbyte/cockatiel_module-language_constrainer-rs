use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::CockatielClient;
use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

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
    #[serde(default = "default_expressive_chars")]
    expressive_chars: Vec<String>,
    #[serde(default = "default_reconnect_base_secs")]
    reconnect_base_secs: u64,
    #[serde(default = "default_reconnect_max_secs")]
    reconnect_max_secs: u64,
}

fn default_reconnect_base_secs() -> u64 {
    1
}

fn default_reconnect_max_secs() -> u64 {
    30
}

fn default_language() -> String {
    "latin".to_string()
}

fn default_reason() -> String {
    "out-of-language".to_string()
}

fn default_expressive_chars() -> Vec<String> {
    vec![
        "ඞ".to_string(),
        "๑".to_string(),
        "ᴗ".to_string(),
        "ಠ".to_string(),
        "益".to_string(),
        "ʕ".to_string(),
        "ʔ".to_string(),
        "⊙".to_string(),
        "☉".to_string(),
        "♥".to_string(),
        "♡".to_string(),
        "✧".to_string(),
        "❀".to_string(),
        "★".to_string(),
        "☆".to_string(),
    ]
}

fn default_config() -> Config {
    Config {
        language: default_language(),
        custom_ranges: Vec::new(),
        allow_emoji: false,
        allow_expressive: false,
        flag_reason: default_reason(),
        expressive_chars: default_expressive_chars(),
        reconnect_base_secs: default_reconnect_base_secs(),
        reconnect_max_secs: default_reconnect_max_secs(),
    }
}

/// Read the Config from config.json, preferring the `module_specific` object
/// (the house convention for module settings) and falling back to the legacy
/// top-level layout so pre-existing configs keep working.
fn read_config_from_file() -> Option<Config> {
    let s = std::fs::read_to_string("config.json").ok()?;
    let root: serde_json::Value = serde_json::from_str(&s).ok()?;
    match root.get("module_specific") {
        Some(ms) if ms.is_object() => serde_json::from_value::<Config>(ms.clone()).ok(),
        _ => serde_json::from_str::<Config>(&s).ok(),
    }
}

/// Save the Config into the `module_specific` object of config.json, merging
/// with (and preserving) any existing top-level fields.
fn save_config(config: &Config) {
    let mut root: serde_json::Value = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    root["module_specific"] = serde_json::to_value(config).unwrap_or(serde_json::Value::Null);
    if let Ok(pretty) = serde_json::to_string_pretty(&root) {
        let _ = std::fs::write("config.json", pretty);
    }
}

fn load_config() -> Config {
    if let Some(cfg) = read_config_from_file() {
        // A legacy top-level config is migrated into module_specific here so
        // it stays in sync with the house layout.
        save_config(&cfg);
        return cfg;
    }
    // No saved config (or an unreadable one): create defaults and backfill
    // them into module_specific so the settings always exist.
    let cfg = default_config();
    save_config(&cfg);
    cfg
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

/// Characters commonly used for expressive/emote messages (e.g. ඞ, (๑ > ᴗ < ๑)),
/// read from config (defaults to the curated list) so operators can extend it.
fn is_expressive(c: char, expressive_chars: &[String]) -> bool {
    expressive_chars.iter().any(|s| s.starts_with(c))
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
    expressive_chars: &[String],
) -> bool {
    for c in message.chars() {
        if c.is_whitespace() || in_ranges(c, ranges) {
            continue;
        }
        if allow_emoji && is_emoji(c) {
            continue;
        }
        if allow_expressive && is_expressive(c, expressive_chars) {
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
        processed_message: Some(processed.to_string()),
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
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let auth_token = client.auth_token.clone();
    let instance_uuid = client.instance_uuid7.clone();
    let module_name = client.config.module_name.clone();

    // Owns the read half + session identity so it can reconnect with backoff
    // when the engine drops the socket (instead of dying and leaving main to
    // sleep forever while the watchdog severs the unresponsive module).
    {
        let write_shared = Arc::clone(&write_shared);
        let mut auth_token = auth_token.clone();
        let mut instance_uuid = instance_uuid.clone();
        let mut module_name = module_name.clone();
        let mut read = read;
        tokio::spawn(async move {
            'reconnect: loop {
                loop {
                    let Some(msg) = read.next().await else { break };
                    let data = match msg {
                        Ok(WsMessage::Binary(d)) => d,
                        Ok(WsMessage::Close(_)) => {
                            info!("Engine closed connection");
                            break;
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            warn!("Engine WebSocket error: {}", e);
                            break;
                        }
                    };
                    let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };

                    match container.payload {
                        Some(ModulePayload::AuthVerify(_)) => {
                            // Answer the engine's liveness probe (this module
                            // reads the socket directly, so the client's
                            // auto-answer is bypassed — without this the
                            // watchdog severs us).
                            let reply = ContainerForEngine {
                                version: 2,
                                auth_token: auth_token.clone(),
                                module_name: module_name.clone(),
                                module_instance_uuid7: instance_uuid.clone(),
                                payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                    cur_auth: auth_token.clone(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = write_shared.lock().await;
                                let _ = w.send(WsMessage::Binary(buf)).await;
                            }
                        }
                        Some(ModulePayload::MessageInProcess(process)) => {
                            let Some(chat) = &process.raw_message else { continue };
                            let uuid = process.message_uuid7.clone();
                            if !uuid.is_empty() {
                                let receipt = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth_token.clone(),
                                    module_name: module_name.clone(),
                                    module_instance_uuid7: instance_uuid.clone(),
                                    payload: Some(EnginePayload::MessageAck(MessageAck {
                                        message_uuid7: uuid.clone(),
                                    })),
                                };
                                let mut buf = Vec::new();
                                if receipt.encode(&mut buf).is_ok() {
                                    let mut w = write_shared.lock().await;
                                    let _ = w.send(WsMessage::Binary(buf)).await;
                                }
                            }
                            let original = chat.raw_message.clone();

                            // Out-of-language messages are held for audit: the
                            // engine broadcasts an audit prompt (Approve/Reject)
                            // to connected UIs.
                            if violates_language(
                                &original,
                                &ranges,
                                config.allow_emoji,
                                config.allow_expressive,
                                &config.expressive_chars,
                            ) {
                                warn!(
                                    "Message [{}] violates language '{}' — flagging for audit",
                                    uuid, config.language
                                );
                                let flag = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth_token.clone(),
                                    module_name: module_name.clone(),
                                    module_instance_uuid7: instance_uuid.clone(),
                                    payload: Some(EnginePayload::AuditFlag(AuditFlag {
                                        message_uuid7: uuid.clone(),
                                        reason: config.flag_reason.clone(),
                                        origin: module_name.clone(),
                                    })),
                                };
                                let mut buf = Vec::new();
                                if flag.encode(&mut buf).is_ok() {
                                    let mut w = write_shared.lock().await;
                                    let _ = w.send(WsMessage::Binary(buf)).await;
                                }
                                // Also record the rejection clearly (raw +
                                // reason), like the banned-words module — the
                                // engine logs + persists it as a searchable
                                // `chat_rejected` timeline record.
                                let reason = format!("violates language '{}'", config.language);
                                let rej = compose_rejected(
                                    &uuid,
                                    chat,
                                    &original,
                                    &reason,
                                    &module_name,
                                );
                                let rej = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth_token.clone(),
                                    module_name: module_name.clone(),
                                    module_instance_uuid7: instance_uuid.clone(),
                                    payload: Some(EnginePayload::ChatMessageRejected(rej)),
                                };
                                let mut buf = Vec::new();
                                if rej.encode(&mut buf).is_ok() {
                                    let mut w = write_shared.lock().await;
                                    let _ = w.send(WsMessage::Binary(buf)).await;
                                }
                            }

                            // Ack in_process (content preserved — the audit hold
                            // prevents it from being shown until a moderator
                            // releases it).
                            let reply = ContainerForEngine {
                                version: 2,
                                auth_token: auth_token.clone(),
                                module_name: module_name.clone(),
                                module_instance_uuid7: instance_uuid.clone(),
                                payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                    message_uuid7: uuid,
                                    raw_message: Some(ChatMessage {
                                        platform: chat.platform.clone(),
                                        raw_data: chat.raw_data.clone(),
                                        raw_message: original.clone(),
                                        user_uuid7: chat.user_uuid7.clone(),
                                        command: chat.command.clone(),
                                        channel_id: chat.channel_id.clone(),
                                        user_data: chat.user_data.clone(),
                                    }),
                                    processed_message: original,
                                    abandon_message: false,
                                    audio: Vec::new(),
                                    audio_type: String::new(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = write_shared.lock().await;
                                let _ = w.send(WsMessage::Binary(buf)).await;
                            }
                        }
                        _ => {}
                    }
                }

                // The engine connection dropped — reconnect with backoff
                // instead of leaving the module unresponsive.
                info!("Engine disconnected — reconnecting...");
                let mut backoff = config.reconnect_base_secs;
                loop {
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    match CockatielClient::connect("language_constrainer.json").await {
                        Ok(conn) => {
                            info!("Reconnected to engine");
                            let (w, r) = conn.stream.split();
                            *write_shared.lock().await = w;
                            auth_token = conn.auth_token;
                            instance_uuid = conn.instance_uuid7;
                            module_name = conn.config.module_name;
                            read = r;
                            continue 'reconnect;
                        }
                        Err(e) => {
                            warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                            backoff = (backoff * 2).min(config.reconnect_max_secs);
                        }
                    }
                }
            }
        });
    }

    // The read task owns the socket now; park forever. The watchdog severs a
    // silent module, and AuthVerify answers keep us alive during dead air.
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const LATIN: [[u32; 2]; 1] = [[0x41, 0x5A]]; // A-Z only

    fn expr() -> Vec<String> {
        default_expressive_chars()
    }

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
        assert_eq!(rej.processed_message, Some("Привет".to_string()));
        assert_eq!(rej.message.unwrap().raw_message, "Привет");
    }

    #[test]
    fn ascii_uppercase_pass() {
        assert!(!violates_language("HELLO", &LATIN, false, false, &expr()));
        assert!(!violates_language("HELLO WORLD", &LATIN, false, false, &expr()));
    }

    #[test]
    fn lowercase_fails_outside_range() {
        assert!(violates_language("hello", &LATIN, false, false, &expr()));
    }

    #[test]
    fn non_latin_fails() {
        assert!(violates_language("привет", &LATIN, false, false, &expr()));
        assert!(violates_language("HÉLLO", &LATIN, false, false, &expr()));
    }

    #[test]
    fn emoji_gated_by_flag() {
        assert!(violates_language("HELLO 👍", &LATIN, false, false, &expr()));
        assert!(!violates_language("HELLO 👍", &LATIN, true, false, &expr()));
    }

    #[test]
    fn expressive_gated_by_flag() {
        assert!(violates_language("HELLO ඞ", &LATIN, false, false, &expr()));
        assert!(!violates_language("HELLO ඞ", &LATIN, false, true, &expr()));
    }

    #[test]
    fn expressive_reads_from_config() {
        // The configured list drives is_expressive: a char absent from it is
        // not treated as expressive even when the flag is on.
        let mut chars = default_expressive_chars();
        chars.retain(|s| s != "ඞ");
        assert!(violates_language("HELLO ඞ", &LATIN, false, true, &chars));
        assert!(!violates_language("HELLO ♥", &LATIN, false, true, &chars));
    }

    #[test]
    fn ranges_match() {
        assert!(in_ranges('A', &LATIN));
        assert!(in_ranges('Z', &LATIN));
        assert!(!in_ranges('a', &LATIN));
        assert!(!in_ranges('1', &LATIN));
    }
}
