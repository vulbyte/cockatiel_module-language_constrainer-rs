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
use unicode_normalization::UnicodeNormalization;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
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
    #[serde(default = "default_allowed_languages")]
    allowed_languages: Vec<String>,
    #[serde(default = "default_detect_min_chars")]
    detect_min_chars: u32,
    #[serde(default = "default_detect_confidence")]
    detect_confidence: f32,
    #[serde(default = "default_reconnect_base_secs")]
    reconnect_base_secs: u32,
    #[serde(default = "default_reconnect_max_secs")]
    reconnect_max_secs: u32,
}

fn default_allowed_languages() -> Vec<String> {
    vec!["eng".to_string()]
}

fn default_detect_min_chars() -> u32 {
    12
}

fn default_detect_confidence() -> f32 {
    0.7
}

fn default_reconnect_base_secs() -> u32 {
    1
}

fn default_reconnect_max_secs() -> u32 {
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
        allowed_languages: default_allowed_languages(),
        detect_min_chars: default_detect_min_chars(),
        detect_confidence: default_detect_confidence(),
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

fn parse_languages(s: &str) -> Option<HashMap<String, Vec<[u32; 2]>>> {
    serde_json::from_str::<Languages>(s).ok().map(|l| l.presets)
}

const EMBEDDED_LANGUAGES: &str = include_str!("../languages.json");

fn load_languages() -> HashMap<String, Vec<[u32; 2]>> {
    std::fs::read_to_string("languages.json")
        .ok()
        .and_then(|s| parse_languages(&s))
        .or_else(|| parse_languages(EMBEDDED_LANGUAGES))
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
        || (0x1F1E6..=0x1F1FF).contains(&cp)
        || (0x1F3FB..=0x1F3FF).contains(&cp)
        || (0x2600..=0x27BF).contains(&cp)
        || (0xFE00..=0xFE0F).contains(&cp)
        || (0x1F000..=0x1F0FF).contains(&cp)
        || cp == 0x20E3
}

/// Characters commonly used for expressive/emote messages (e.g. ඞ, (๑ > ᴗ < ๑)),
/// read from config (defaults to the curated list) so operators can extend it.
fn is_expressive(c: char, expressive_chars: &[String]) -> bool {
    expressive_chars.iter().any(|s| s.contains(c))
}

fn in_ranges(c: char, ranges: &[[u32; 2]]) -> bool {
    let cp = c as u32;
    ranges.iter().any(|r| cp >= r[0] && cp <= r[1])
}

/// NFC-normalize so decomposed Latin (e + U+0301) is checked like precomposed é.
fn normalize(text: &str) -> String {
    text.nfc().collect()
}

/// Fold common typographic punctuation to its ASCII counterpart. The range
/// gate checks the folded char so everyday English using smart quotes /
/// apostrophes / dashes / ellipses is not held for audit; the displayed
/// (`processed_message`) text is never altered.
fn fold_typographic(c: char) -> char {
    match c {
        '\u{2018}' | '\u{2019}' => '\'',
        '\u{201C}' | '\u{201D}' => '"',
        '\u{2013}' | '\u{2014}' => '-',
        '\u{2026}' => '.',
        other => other,
    }
}

/// Per-character range gate: true when a char is outside the whitelist and not
/// exempted as emoji/expressive. Typographic punctuation is folded to ASCII
/// first (gate check only), and `is_whitespace` already accepts U+00A0 NBSP.
fn violates_range_gate(
    message: &str,
    ranges: &[[u32; 2]],
    allow_emoji: bool,
    allow_expressive: bool,
    expressive_chars: &[String],
) -> bool {
    for c in message.chars() {
        if c.is_whitespace() || in_ranges(fold_typographic(c), ranges) {
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

/// Strip a leading command token (`!cmd` / `/cmd`) and following spaces so
/// language detection sees only the argument text.
fn strip_command_token(text: &str) -> &str {
    let trimmed = text.trim_start();
    if !(trimmed.starts_with('!') || trimmed.starts_with('/')) {
        return trimmed;
    }
    match trimmed.find(char::is_whitespace) {
        Some(idx) => trimmed[idx..].trim_start(),
        None => "",
    }
}

/// True when a whitespace-delimited token looks like a URL: a scheme URL, or a
/// dotted host followed by a path (the shapes Discord/Twitch embeds use, e.g.
/// `cdn.discordapp.com/attachments/…/x.gif?ex=…`).
fn is_url_token(tok: &str) -> bool {
    let lower = tok.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        return true;
    }
    matches!(tok.find('.'), Some(dot) if dot + 1 < tok.len() && tok[dot + 1..].contains('/'))
}

/// Prepare text for `whatlang`: drop URL tokens and tokens that are mostly
/// non-alphabetic (emoji, punctuation runs, `$100`, …). whatlang misreads this
/// link/emoji soup as a foreign language (e.g. a Discord CDN URL was detected
/// as Portuguese at 0.505), which held ordinary English for audit.
fn detection_text(scan: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for tok in scan.split_whitespace() {
        if is_url_token(tok) {
            continue;
        }
        let total = tok.chars().count() as u32;
        let alpha = tok.chars().filter(|c| c.is_alphabetic()).count() as u32;
        if alpha * 2 >= total {
            kept.push(tok);
        }
    }
    kept.join(" ")
}

#[derive(Debug, Clone, Copy)]
struct DetectCfg<'a> {
    allowed_languages: &'a [String],
    min_chars: u32,
    confidence: f32,
}

fn violates_language(
    message: &str,
    ranges: &[[u32; 2]],
    allow_emoji: bool,
    allow_expressive: bool,
    expressive_chars: &[String],
    detect: DetectCfg<'_>,
) -> bool {
    if violates_range_gate(
        message,
        ranges,
        allow_emoji,
        allow_expressive,
        expressive_chars,
    ) {
        return true;
    }
    if detect.allowed_languages.is_empty() {
        return false;
    }
    let scan = strip_command_token(message);
    let scan = detection_text(scan);
    let alphabetic = scan.chars().filter(|c| c.is_alphabetic()).count() as u32;
    if alphabetic < detect.min_chars {
        return false;
    }
    match whatlang::detect(&scan) {
        None => false,
        Some(info) => {
            let allowed = detect
                .allowed_languages
                .iter()
                .any(|l| l.eq_ignore_ascii_case(info.lang().code()));
            // whatlang's `confidence` is not a probability: it saturates
            // toward 1.0 on short text, so a bare floor holds ordinary ASCII
            // English. Flag only when the detected language is not allowed
            // AND the detection is whatlang-reliable (>0.9) or clears the
            // configured floor — reliable detections always flag, and the
            // operator floor stays authoritative at the (0.7) default.
            !allowed && (info.is_reliable() || info.confidence() >= detect.confidence as f64)
        }
    }
}

/// The text a module should operate on: the accumulated in-process text from
/// earlier modules, falling back to the raw chat message only when that is
/// empty. Checking the raw message would discard earlier modules' edits.
fn effective_text<'a>(chat_raw: &'a str, incoming: &'a str) -> &'a str {
    if incoming.trim().is_empty() {
        chat_raw
    } else {
        incoming
    }
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

async fn send_msg(write_shared: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_shared.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
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
                // Register an empty command list: per the proto this makes the
                // module a catch-all, so it receives command messages (e.g.
                // `!tts <text>`) instead of only the owner's plain messages.
                send_msg(
                    &write_shared,
                    ContainerForEngine {
                        version: 2,
                        auth_token: auth_token.clone(),
                        module_name: module_name.clone(),
                        module_instance_uuid7: instance_uuid.clone(),
                        payload: Some(EnginePayload::Commands(Commands {
                            commands: vec![],
                            alert_on_unknown_command: false,
                        })),
                    },
                )
                .await;
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
                    let Ok(container) = ContainerForModule::decode(data.as_ref()) else {
                        continue;
                    };

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
                            send_msg(&write_shared, reply).await;
                        }
                        Some(ModulePayload::MessageInProcess(process)) => {
                            let Some(chat) = &process.raw_message else {
                                continue;
                            };
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
                                send_msg(&write_shared, receipt).await;
                            }
                            // Check the accumulated in-process text from earlier
                            // modules, falling back to the raw message only when
                            // it is empty — otherwise their edits are discarded.
                            let incoming = process.processed_message.clone();
                            let text = effective_text(&chat.raw_message, &incoming);
                            let text = normalize(text);

                            // Out-of-language messages are held for audit: the
                            // engine broadcasts an audit prompt (Approve/Reject)
                            // to connected UIs.
                            if violates_language(
                                &text,
                                &ranges,
                                config.allow_emoji,
                                config.allow_expressive,
                                &config.expressive_chars,
                                DetectCfg {
                                    allowed_languages: &config.allowed_languages,
                                    min_chars: config.detect_min_chars,
                                    confidence: config.detect_confidence,
                                },
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
                                send_msg(&write_shared, flag).await;
                                // Also record the rejection clearly (raw +
                                // reason), like the banned-words module — the
                                // engine logs + persists it as a searchable
                                // `chat_rejected` timeline record.
                                let reason = format!("violates language '{}'", config.language);
                                let rej =
                                    compose_rejected(&uuid, chat, &text, &reason, &module_name);
                                let rej = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth_token.clone(),
                                    module_name: module_name.clone(),
                                    module_instance_uuid7: instance_uuid.clone(),
                                    payload: Some(EnginePayload::ChatMessageRejected(rej)),
                                };
                                send_msg(&write_shared, rej).await;
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
                                        raw_message: chat.raw_message.clone(),
                                        user_uuid7: chat.user_uuid7.clone(),
                                        command: chat.command.clone(),
                                        channel_id: chat.channel_id.clone(),
                                        user_data: chat.user_data.clone(),
                                    }),
                                    processed_message: text.clone(),
                                    abandon_message: false,
                                    audio: Vec::new(),
                                    audio_type: String::new(),
                                })),
                            };
                            send_msg(&write_shared, reply).await;
                        }
                        _ => {}
                    }
                }

                // The engine connection dropped — reconnect with backoff
                // instead of leaving the module unresponsive.
                info!("Engine disconnected — reconnecting...");
                let mut backoff = config.reconnect_base_secs;
                loop {
                    tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
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
    const MIN: u32 = 12;
    const CONF: f32 = 0.7; // matches default_detect_confidence()

    fn expr() -> Vec<String> {
        default_expressive_chars()
    }

    fn real_latin() -> Vec<[u32; 2]> {
        load_languages().get("latin").cloned().unwrap_or_default()
    }

    fn allowed() -> Vec<String> {
        vec!["eng".to_string()]
    }

    fn cfg<'a>(allowed_languages: &'a [String], min_chars: u32, confidence: f32) -> DetectCfg<'a> {
        DetectCfg {
            allowed_languages,
            min_chars,
            confidence,
        }
    }

    // Range gate only: empty allowed_languages disables detection.
    fn range(
        msg: &str,
        ranges: &[[u32; 2]],
        emoji: bool,
        expressive: bool,
        chars: &[String],
    ) -> bool {
        violates_language(msg, ranges, emoji, expressive, chars, cfg(&[], MIN, CONF))
    }

    fn detected(msg: &str, allowed_languages: &[String], confidence: f32) -> bool {
        violates_language(
            &normalize(msg),
            &real_latin(),
            false,
            false,
            &expr(),
            cfg(allowed_languages, MIN, confidence),
        )
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
        let rej = compose_rejected(
            "uuid-9",
            &chat,
            "Привет",
            "violates language 'latin'",
            "language-constrainer",
        );
        assert_eq!(rej.message_uuid7, "uuid-9");
        assert_eq!(rej.origin, "language-constrainer");
        assert_eq!(rej.reason, "violates language 'latin'");
        assert_eq!(rej.processed_message, Some("Привет".to_string()));
        assert_eq!(rej.message.unwrap().raw_message, "Привет");
    }

    #[test]
    fn incoming_processed_text_is_used_and_raw_fallback_only_when_empty() {
        // A prior module rewrote the Cyrillic message to Latin uppercase; the
        // language check must see the transformed text, not the raw one.
        assert_eq!(effective_text("привет", "HELLO"), "HELLO");
        assert!(!range(
            effective_text("привет", "HELLO"),
            &LATIN,
            false,
            false,
            &expr()
        ));
        // An empty incoming falls back to the raw message.
        assert_eq!(effective_text("привет", ""), "привет");
        assert!(range(
            effective_text("привет", ""),
            &LATIN,
            false,
            false,
            &expr()
        ));
    }

    #[test]
    fn ascii_uppercase_pass() {
        assert!(!range("HELLO", &LATIN, false, false, &expr()));
        assert!(!range("HELLO WORLD", &LATIN, false, false, &expr()));
    }

    #[test]
    fn lowercase_fails_outside_range() {
        assert!(range("hello", &LATIN, false, false, &expr()));
    }

    #[test]
    fn non_latin_fails() {
        assert!(range("привет", &LATIN, false, false, &expr()));
        assert!(range("HÉLLO", &LATIN, false, false, &expr()));
    }

    #[test]
    fn emoji_gated_by_flag() {
        assert!(range("HELLO 👍", &LATIN, false, false, &expr()));
        assert!(!range("HELLO 👍", &LATIN, true, false, &expr()));
    }

    #[test]
    fn emoji_covers_flags_and_skin_tones() {
        for c in ['\u{1F1FA}', '\u{1F1F8}', '\u{1F3FB}', '\u{20E3}'] {
            assert!(is_emoji(c), "expected {c:?} to be emoji");
        }
    }

    #[test]
    fn expressive_gated_by_flag() {
        assert!(range("HELLO ඞ", &LATIN, false, false, &expr()));
        assert!(!range("HELLO ඞ", &LATIN, false, true, &expr()));
    }

    #[test]
    fn expressive_reads_from_config() {
        // The configured list drives is_expressive: a char absent from it is
        // not treated as expressive even when the flag is on.
        let mut chars = default_expressive_chars();
        chars.retain(|s| s != "ඞ");
        assert!(range("HELLO ඞ", &LATIN, false, true, &chars));
        assert!(!range("HELLO ♥", &LATIN, false, true, &chars));
    }

    #[test]
    fn expressive_multichar_entry_whitelists_all_its_chars() {
        let mut chars = expr();
        chars.push("(๑ > ᴗ < ๑)".to_string());
        assert!(is_expressive('>', &chars));
        assert!(is_expressive('ᴗ', &chars));
        // With the flag on they are exempt; without it ᴗ is outside the preset.
        assert!(!violates_range_gate(
            "( > ᴗ )",
            &real_latin(),
            false,
            true,
            &chars
        ));
        assert!(violates_range_gate(
            "( > ᴗ )",
            &real_latin(),
            false,
            false,
            &chars
        ));
    }

    #[test]
    fn ranges_match() {
        assert!(in_ranges('A', &LATIN));
        assert!(in_ranges('Z', &LATIN));
        assert!(!in_ranges('a', &LATIN));
        assert!(!in_ranges('1', &LATIN));
    }

    #[test]
    fn embedded_languages_fallback_has_latin_preset() {
        let presets = parse_languages(EMBEDDED_LANGUAGES).expect("embedded languages parse");
        assert!(!presets.get("latin").expect("latin preset").is_empty());
    }

    #[test]
    fn nfc_normalization_accepts_decomposed_latin() {
        // e + U+0301 folds to é (U+00E9), inside the real latin preset; the
        // raw combining mark alone is outside it.
        assert!(violates_range_gate(
            "e\u{0301}",
            &real_latin(),
            false,
            false,
            &expr()
        ));
        assert!(!violates_range_gate(
            &normalize("e\u{0301}"),
            &real_latin(),
            false,
            false,
            &expr()
        ));
    }

    #[test]
    fn non_latin_scripts_violate() {
        let cases = [
            "Сара ауаа зегьы бзиа избоит.",
            "Я люблю всіх людей.",
            "Мон вань адямиосты яратӥсько.",
            "Шупту улуска ынак мен.",
            "Мин барлык кешеләрне яратам.",
            "हम सब लोक स प्रेम करैत छी।",
            "अहं सर्वान् जनान् प्रेम करोमि।",
            "म सबै मानिसहरूलाई माया गर्छु।",
            "コンピューター",
            "ང་མི་ཚང་མར་དགའ།",
            "ฉันรักทุกคน",
            "මම සියලුම මිනිසුන්ට ආදරෙයි.",
            "ನಾನು ಎಲ್ಲ ಜನರನ್ನು ಪ್ರೀತಿಸುತ್ತೇನೆ.",
            "איך האָב ליב אַלע מענטשן.",
            "ߒߓߋ ߡߜ߭ߐߥߵ ߍ ߞߊߣߎ",
        ];
        for msg in cases {
            assert!(
                detected(msg, &allowed(), CONF),
                "expected violation for {msg:?}"
            );
        }
    }

    #[test]
    fn latin_script_foreign_languages_violate() {
        // Vietnamese is caught by the range gate (Latin-Extended-Additional chars
        // fall outside the `latin` preset); Indonesian by the detection gate.
        for msg in ["Tôi yêu tất cả mọi người.", "Saya mencintai semua orang."] {
            assert!(
                detected(msg, &allowed(), CONF),
                "expected violation for {msg:?}"
            );
        }
    }

    #[test]
    fn turkish_violates_at_tuned_confidence() {
        // whatlang 0.18's default Combined detector reports the Turkish sentence
        // as Azerbaijani at confidence 0.222, below the 0.7 floor, so it needs a
        // lower threshold to flag. Kept as documentation of that limitation.
        let msg = "Bütün insanları seviyorum.";
        assert!(!detected(msg, &allowed(), CONF));
        assert!(detected(msg, &allowed(), 0.2));
    }

    #[test]
    fn english_passes() {
        // Includes phrases whatlang misclassifies at LOW confidence (0.03-0.28:
        // uzb/tgl/fra/nob/jav) precisely so the 0.7 floor is regression-guarded
        // against false positives on ordinary English chat.
        for msg in [
            "I love all people.",
            "hello everyone how are you doing today",
            "that was a great play dude",
            "I am begging pardon",
            "let me know if you want me to play something else",
            "laughing out loud",
            "bro that was insane clutch",
            "happy birthday to you happy birthday to you",
        ] {
            assert!(
                !detected(msg, &allowed(), CONF),
                "expected pass for {msg:?}"
            );
        }
    }

    #[test]
    fn typographic_punctuation_does_not_violate() {
        // The exact live strings that were held for audit: curly apostrophes /
        // quotes / ellipses are outside the raw `latin` ranges, but the gate
        // folds them to ASCII and the smart-quote English passes detection.
        let cases = [
            "Isn\u{2019}t it broken iirc?",
            "\u{201C}Dew it!\u{201D} - Palpatine",
            "Hopefully they\u{2019}re easy enough fixes xwx",
            "Have you fixed the stream rig btw? What\u{2019}s happening???",
            "a reference to the 1940\u{2019}s cartoon\u{2026}",
            "test\u{2013}case and a long\u{2014}dash",
        ];
        for msg in cases {
            assert!(
                !violates_range_gate(msg, &real_latin(), false, false, &expr()),
                "range gate flagged {msg:?}"
            );
            assert!(
                !detected(msg, &allowed(), CONF),
                "detection flagged {msg:?}"
            );
        }
    }

    #[test]
    fn typographic_fold_works_without_preset_ranges() {
        // A custom preset that lacks the typographic ranges must still pass:
        // the gate folds these to ASCII, so it can't regress.
        let ascii: [[u32; 2]; 1] = [[32, 126]];
        for c in [
            '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}', '\u{2013}', '\u{2014}', '\u{2026}',
        ] {
            assert!(!in_ranges(c, &ascii), "{c:?} unexpectedly in ASCII range");
            assert!(
                !violates_range_gate(&c.to_string(), &ascii, false, false, &expr()),
                "fold failed for {c:?}"
            );
        }
    }

    #[test]
    fn non_breaking_space_is_whitespace() {
        assert!('\u{00A0}'.is_whitespace());
        assert!(!violates_range_gate(
            "Have\u{00A0}you fixed the stream rig?",
            &real_latin(),
            false,
            false,
            &expr()
        ));
    }

    #[test]
    fn discord_cdn_url_passes() {
        // A live Discord CDN URL was detected as Portuguese at 0.505 and held;
        // URL stripping drops it before whatlang runs.
        let url = "https://cdn.discordapp.com/attachments/1300251755614965840/1559072465362362398/pyro-pyro-tf2.gif?ex=6acdbee1&is=6acc6d61&hm=f1e8a454f27fe016b4ae5ae8f66322d16b0b2ec4d35b4d70bdc12f6786ba2ef0&";
        assert!(is_url_token(url));
        assert_eq!(detection_text(url), "");
        assert!(
            !detected(url, &allowed(), CONF),
            "expected Discord CDN URL to pass"
        );
        // A bare host+path (no scheme) is also stripped.
        assert!(!detected(
            "cdn.discordapp.com/attachments/1/2/IMG_5478.jpg?ex=abc",
            &allowed(),
            CONF
        ));
    }

    #[test]
    fn detection_text_drops_non_alphabetic_tokens() {
        assert_eq!(detection_text("hello ??? world"), "hello world");
        assert_eq!(detection_text("$100 kekw 1337"), "kekw");
        assert_eq!(detection_text("it\u{2019}s fine"), "it\u{2019}s fine");
        // Half-or-more alphabetic tokens survive; punctuation runs do not.
        assert_eq!(detection_text("a. b! \u{2014} ok"), "a. b! ok");
    }

    #[test]
    fn short_emotes_below_detection_floor_pass() {
        for msg in ["gg", "lol", "hi", "kekw", "<3"] {
            assert!(
                !detected(msg, &allowed(), CONF),
                "expected pass for {msg:?}"
            );
        }
    }

    #[test]
    fn command_token_is_stripped_for_detection() {
        assert!(detected(
            "!tts Saya mencintai semua orang.",
            &allowed(),
            CONF
        ));
        assert!(!detected("!tts hello chat", &allowed(), CONF));
        assert_eq!(strip_command_token("!tts hello"), "hello");
        assert_eq!(strip_command_token("/me waves"), "waves");
        assert_eq!(strip_command_token("plain"), "plain");
        assert_eq!(strip_command_token("!alone"), "");
    }
}
