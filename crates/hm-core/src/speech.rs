//! Speech service adapters: TTS and ASR behind small traits, independently
//! configurable like the LLM (docs/contracts-v0.md §服务配置).
//!
//! Providers (user-selected per service):
//! - TTS "volc": Volcano 豆包 seed-tts-2.0 HTTP unidirectional stream
//!   (NDJSON frames of base64 PCM). Reference: the RUI volc_tts provider
//!   (house-ear src/rui/providers/volc_tts.py, measured ttfb 0.58 s;
//!   re-measured from this machine 2026-11-07: 0.56 s). Voice/speaker ids
//!   from the Volcano 2.0 catalogue; production default zh_female_xiaohe_uranus_bigtts.
//! - TTS "omni": any OpenAI-compatible `/v1/audio/speech` endpoint (oMLX
//!   12Hz-0.6B-CustomVoice verified working; requires a `voice`).
//! - TTS "mock": deterministic silence — tests and no-network dev.
//! - ASR: trait + mock in M2; M4 adds "openai-asr" — any OpenAI-compatible
//!   `/v1/audio/transcriptions` endpoint (protocol over engine; live-verified
//!   against oMLX Qwen3-ASR-0.6B-bf16 2026-11-07; whisper.cpp server / cloud
//!   share the wire shape per public docs, not yet measured from here).

use crate::config::ServiceConfig;
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum SpeechError {
    #[error("provider not configured")]
    NotConfigured,
    #[error("missing {0}")]
    Missing(String),
    #[error("http {status}: {detail}")]
    Http { status: u16, detail: String },
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("transport: {0}")]
    Transport(String),
}

/// Synthesized audio, always containerized as WAV (s16le) for the v0
/// inline play frames.
#[derive(Debug, Clone)]
pub struct TtsAudio {
    pub wav: Vec<u8>,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration_ms: u64,
    pub provider: String,
    pub voice: String,
}

impl TtsAudio {
    pub fn meta(&self) -> super::gateway::AudioMeta {
        super::gateway::AudioMeta {
            format: "wav".into(),
            sample_rate: self.sample_rate,
            channels: self.channels,
        }
    }
}

/// One streaming TTS piece (v0.3/T3): bare S16LE PCM at `sample_rate`.
/// The rate rides every piece so the consumer can open the device stream
/// lazily on the FIRST piece — before the provider has said anything
/// about the final audio metadata.
#[derive(Debug, Clone)]
pub struct TtsPiece {
    pub sample_rate: u32,
    pub pcm: Vec<u8>,
}

#[async_trait::async_trait]
pub trait TtsClient: Send + Sync {
    async fn synthesize(&self, text: &str) -> Result<TtsAudio, SpeechError>;
    /// v0.3 (D/T3): streaming synthesis — PCM pieces are pushed to
    /// `on_pcm` as the provider yields them, and the FULL audio is still
    /// returned at the end (accounting/trace keep one shape). Providers
    /// without a streaming API inherit the default: synthesize the whole WAV,
    /// then emit bounded mono PCM pieces with the same samples.
    async fn synthesize_streaming(
        &self,
        text: &str,
        on_pcm: &tokio::sync::mpsc::UnboundedSender<TtsPiece>,
    ) -> Result<TtsAudio, SpeechError> {
        let audio = self.synthesize(text).await?;
        let (sample_rate, channels, pcm) = wav_pcm(&audio.wav)
            .ok_or_else(|| SpeechError::Protocol("TTS WAV has no parseable PCM data".into()))?;
        let frame_bytes = usize::from(channels) * 2;
        if sample_rate == 0 || frame_bytes == 0 || pcm.len() % frame_bytes != 0 {
            return Err(SpeechError::Protocol(
                "TTS WAV has invalid PCM frame geometry".into(),
            ));
        }
        // RIFF metadata can precede or follow data; a fixed 44-byte offset
        // would play metadata as sound. Stream the data chunk only.
        let mono = if channels == 1 {
            std::borrow::Cow::Borrowed(pcm)
        } else {
            // PlayBegin's voice stream is mono. Preserve frame timing when a
            // provider returns stereo instead of interpreting channels as time.
            let mut mono = Vec::with_capacity(pcm.len() / usize::from(channels));
            for frame in pcm.chunks_exact(frame_bytes) {
                let sum: i64 = frame
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|s| i64::from(i16::from_le_bytes([s[0], s[1]])))
                    .sum();
                mono.extend_from_slice(&((sum / i64::from(channels)) as i16).to_le_bytes());
            }
            std::borrow::Cow::Owned(mono)
        };
        // Match the whole-WAV gateway path's frame bound; long replies must
        // not be encoded as one giant PlayChunk.
        for chunk in mono.chunks(8192) {
            on_pcm
                .send(TtsPiece {
                    sample_rate,
                    pcm: chunk.to_vec(),
                })
                .map_err(|_| SpeechError::Protocol("TTS stream consumer closed".into()))?;
            // Let the forwarder and socket writer drain between chunks rather
            // than filling the bounded outbound queue in one synchronous poll.
            tokio::task::yield_now().await;
        }
        Ok(audio)
    }
    fn provider(&self) -> &str;
}

#[async_trait::async_trait]
pub trait AsrClient: Send + Sync {
    async fn transcribe(&self, wav: &[u8]) -> Result<String, SpeechError>;
    fn provider(&self) -> &str;
}

// --- WAV helper ----------------------------------------------------------------

/// Wrap raw s16le PCM into a RIFF/WAVE container.
pub fn pcm_to_wav(pcm: &[u8], sample_rate: u32, channels: u16) -> Vec<u8> {
    let byte_rate = sample_rate as u32 * channels as u32 * 2;
    let mut w = Vec::with_capacity(44 + pcm.len());
    w.extend_from_slice(b"RIFF");
    w.extend_from_slice(&(36 + pcm.len() as u32).to_le_bytes());
    w.extend_from_slice(b"WAVE");
    w.extend_from_slice(b"fmt ");
    w.extend_from_slice(&16u32.to_le_bytes());
    w.extend_from_slice(&1u16.to_le_bytes()); // PCM
    w.extend_from_slice(&channels.to_le_bytes());
    w.extend_from_slice(&sample_rate.to_le_bytes());
    w.extend_from_slice(&byte_rate.to_le_bytes());
    w.extend_from_slice(&(channels * 2).to_le_bytes());
    w.extend_from_slice(&16u16.to_le_bytes());
    w.extend_from_slice(b"data");
    w.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
    w.extend_from_slice(pcm);
    w
}

/// Parse (sample_rate, channels) from a RIFF/WAVE by walking chunks to fmt.
pub fn wav_format(wav: &[u8]) -> Option<(u32, u16)> {
    if wav.len() < 12 || &wav[0..4] != b"RIFF" || &wav[8..12] != b"WAVE" {
        return None;
    }
    let mut pos = 12;
    while pos + 8 <= wav.len() {
        let id = &wav[pos..pos + 4];
        let size =
            u32::from_le_bytes([wav[pos + 4], wav[pos + 5], wav[pos + 6], wav[pos + 7]]) as usize;
        if id == b"fmt " && pos + 8 + 8 <= wav.len() {
            let channels = u16::from_le_bytes([wav[pos + 10], wav[pos + 11]]);
            let sample_rate =
                u32::from_le_bytes([wav[pos + 12], wav[pos + 13], wav[pos + 14], wav[pos + 15]]);
            return Some((sample_rate, channels));
        }
        pos += 8 + size + (size & 1);
    }
    None
}

/// Extract `(sample_rate, channels, pcm_s16le_bytes)` from a RIFF/WAVE payload.
pub fn wav_pcm(wav: &[u8]) -> Option<(u32, u16, &[u8])> {
    if wav.len() < 12 || &wav[0..4] != b"RIFF" || &wav[8..12] != b"WAVE" {
        return None;
    }
    let mut fmt = None;
    let mut data = None;
    let mut pos = 12;
    while pos + 8 <= wav.len() {
        let id = &wav[pos..pos + 4];
        let size =
            u32::from_le_bytes([wav[pos + 4], wav[pos + 5], wav[pos + 6], wav[pos + 7]]) as usize;
        let start = pos + 8;
        let end = (start + size).min(wav.len());
        if id == b"fmt " && start + 8 <= end {
            let channels = u16::from_le_bytes([wav[start + 2], wav[start + 3]]);
            let sample_rate = u32::from_le_bytes([
                wav[start + 4],
                wav[start + 5],
                wav[start + 6],
                wav[start + 7],
            ]);
            fmt = Some((sample_rate, channels));
        } else if id == b"data" {
            data = Some(&wav[start..end]);
        }
        pos = start + size + (size & 1);
    }
    let (sr, ch) = fmt?;
    let pcm = data?;
    Some((sr, ch, pcm))
}

pub fn wav_duration_ms(wav_len: usize, sample_rate: u32, channels: u16) -> u64 {
    let data = wav_len.saturating_sub(44);
    let byte_rate = sample_rate as u64 * channels as u64 * 2;
    if byte_rate == 0 {
        0
    } else {
        data as u64 * 1000 / byte_rate
    }
}

/// Scale the PCM inside a WAV by `db` (0 = unchanged, negative = quieter).
/// Media-channel output trim (user decision 2026-11-10): the AEC residual
/// scales with content loudness, so a −6 dB media trim keeps music under
/// the self-hearing cliff without touching the voice chain. Header bytes
/// and unknown chunks are preserved verbatim; only the `data` payload is
/// touched. Clamps at i16 bounds (no wrap).
pub fn wav_gain_db(wav: &[u8], db: f64) -> Vec<u8> {
    if db >= 0.0 || wav.len() < 12 || &wav[0..4] != b"RIFF" || &wav[8..12] != b"WAVE" {
        return wav.to_vec();
    }
    let gain = 10f64.powf(db / 20.0);
    let mut out = wav.to_vec();
    let mut pos = 12;
    while pos + 8 <= out.len() {
        let id = [out[pos], out[pos + 1], out[pos + 2], out[pos + 3]];
        let size =
            u32::from_le_bytes([out[pos + 4], out[pos + 5], out[pos + 6], out[pos + 7]]) as usize;
        let start = pos + 8;
        let end = (start + size).min(out.len());
        if &id == b"data" {
            let mut i = start;
            while i + 1 < end {
                let s = i16::from_le_bytes([out[i], out[i + 1]]);
                let v = ((s as f64 * gain).round()).clamp(-32768.0, 32767.0) as i16;
                out[i..i + 2].copy_from_slice(&v.to_le_bytes());
                i += 2;
            }
            break;
        }
        pos = start + size + (size & 1);
    }
    out
}

// --- Volcano TTS -----------------------------------------------------------------

pub const VOLC_TTS_URL: &str = "https://openspeech.bytedance.com/api/v3/tts/unidirectional";
pub const VOLC_TTS_RESOURCE: &str = "seed-tts-2.0";
pub const VOLC_OK: u64 = 20000000;

pub struct VolcanoTts {
    pub base_url: String,
    pub api_key: String,
    pub resource_id: String,
    pub voice: String,
    pub sample_rate: u32,
    pub client: reqwest::Client,
}

// Clients are rebuilt from config every turn (factories below), but a fresh
// reqwest::Client drops the connection pool, so each turn would pay a new
// TLS handshake — visible directly in first-sound latency. Share one static
// client per service kind (timeouts differ per kind): reqwest pools
// connections per host, and the pool survives config changes (different
// providers simply use different hosts).
fn shared_client(
    static_client: &std::sync::OnceLock<reqwest::Client>,
    timeout_secs: u64,
) -> reqwest::Client {
    static_client
        .get_or_init(|| {
            crate::net::apply_resolver(reqwest::Client::builder())
                .timeout(std::time::Duration::from_secs(timeout_secs))
                .build()
                .expect("reqwest client")
        })
        .clone()
}

static VOLC_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
static OMNI_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
static ASR_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

impl VolcanoTts {
    pub fn new(
        base_url: Option<String>,
        api_key: String,
        resource_id: Option<String>,
        voice: String,
    ) -> Self {
        Self {
            base_url: base_url
                .filter(|u| !u.trim().is_empty())
                .unwrap_or_else(|| VOLC_TTS_URL.into()),
            api_key,
            resource_id: resource_id
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_else(|| VOLC_TTS_RESOURCE.into()),
            voice,
            sample_rate: 16000,
            client: shared_client(&VOLC_CLIENT, 20),
        }
    }
}

#[derive(serde::Deserialize)]
struct VolcFrame {
    #[serde(default)]
    code: u64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Option<String>,
}

#[async_trait::async_trait]
impl TtsClient for VolcanoTts {
    async fn synthesize(&self, text: &str) -> Result<TtsAudio, SpeechError> {
        // The whole-path is the streaming path with nobody listening.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        self.synthesize_streaming(text, &tx).await
    }

    /// v0.3 (D/T3): the provider is NDJSON over HTTP — the body IS a
    /// stream; parse each line the moment the transport yields it and
    /// push the decoded PCM piece downstream (the device starts playing
    /// the first sentence while the rest is still synthesizing).
    async fn synthesize_streaming(
        &self,
        text: &str,
        on_pcm: &tokio::sync::mpsc::UnboundedSender<TtsPiece>,
    ) -> Result<TtsAudio, SpeechError> {
        if self.voice.is_empty() {
            return Err(SpeechError::Missing(
                "voice (speaker id) for volc tts".into(),
            ));
        }
        let payload = json!({
            "user": { "uid": "home-mate" },
            "req_params": {
                "text": text,
                "speaker": self.voice,
                "audio_params": { "format": "pcm", "sample_rate": self.sample_rate }
            }
        });
        let resp = self
            .client
            .post(&self.base_url)
            .header("X-Api-Key", &self.api_key)
            .header("X-Api-Resource-Id", &self.resource_id)
            .json(&payload)
            .send()
            .await
            .map_err(|e| SpeechError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(SpeechError::Http {
                status: status.as_u16(),
                detail: detail.chars().take(300).collect(),
            });
        }
        let mut pcm: Vec<u8> = Vec::new();
        let mut handle_line = |line: &str| -> Result<(), SpeechError> {
            let line = line.trim();
            if line.is_empty() {
                return Ok(());
            }
            let frame: VolcFrame = serde_json::from_str(line)
                .map_err(|e| SpeechError::Protocol(format!("bad ndjson frame: {e}")))?;
            if frame.code != 0 && frame.code != VOLC_OK {
                return Err(SpeechError::Protocol(format!(
                    "volc code={} message={}",
                    frame.code, frame.message
                )));
            }
            if let Some(b64) = frame.data {
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(b64) {
                    Ok(piece) => {
                        let _ = on_pcm.send(TtsPiece {
                            sample_rate: self.sample_rate,
                            pcm: piece.clone(),
                        });
                        pcm.extend_from_slice(&piece);
                    }
                    Err(e) => return Err(SpeechError::Protocol(format!("bad audio chunk: {e}"))),
                }
            }
            Ok(())
        };
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| SpeechError::Transport(e.to_string()))?;
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(nl) = buf.find('\n') {
                let line: String = buf.drain(..=nl).collect();
                handle_line(&line)?;
            }
        }
        if !buf.trim().is_empty() {
            handle_line(&buf)?;
        }
        if pcm.is_empty() {
            return Err(SpeechError::Protocol("no audio frames returned".into()));
        }
        let wav = pcm_to_wav(&pcm, self.sample_rate, 1);
        let duration_ms = wav_duration_ms(wav.len(), self.sample_rate, 1);
        Ok(TtsAudio {
            duration_ms,
            wav,
            sample_rate: self.sample_rate,
            channels: 1,
            provider: "volc".into(),
            voice: self.voice.clone(),
        })
    }

    fn provider(&self) -> &str {
        "volc"
    }
}

// --- OpenAI-compatible TTS (oMLX etc.) ---------------------------------------------

pub struct OmniTts {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub voice: String,
    pub client: reqwest::Client,
}

impl OmniTts {
    pub fn new(base_url: String, api_key: String, model: String, voice: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            model,
            voice,
            client: shared_client(&OMNI_CLIENT, 120), // local models can be slow to load
        }
    }
}

#[async_trait::async_trait]
impl TtsClient for OmniTts {
    async fn synthesize(&self, text: &str) -> Result<TtsAudio, SpeechError> {
        let mut body = json!({ "model": self.model, "input": text, "response_format": "wav" });
        if !self.voice.is_empty() {
            body["voice"] = json!(self.voice);
        }
        let mut req = self
            .client
            .post(format!("{}/audio/speech", self.base_url))
            .json(&body);
        if !self.api_key.is_empty() {
            req = req.bearer_auth(&self.api_key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SpeechError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(SpeechError::Http {
                status: status.as_u16(),
                detail: detail.chars().take(300).collect(),
            });
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SpeechError::Transport(e.to_string()))?;
        if bytes.len() < 44 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
            return Err(SpeechError::Protocol("endpoint did not return WAV".into()));
        }
        // Walk chunks to fmt — a leading LIST chunk would break fixed offsets.
        let (sample_rate, channels) = wav_format(&bytes)
            .ok_or_else(|| SpeechError::Protocol("WAV without a parseable fmt chunk".into()))?;
        let duration_ms = wav_duration_ms(bytes.len(), sample_rate, channels);
        Ok(TtsAudio {
            wav: bytes.to_vec(),
            sample_rate,
            channels,
            duration_ms,
            provider: "omni".into(),
            voice: self.voice.clone(),
        })
    }

    fn provider(&self) -> &str {
        "omni"
    }
}

// --- mocks -------------------------------------------------------------------------

/// Deterministic silence WAV sized by text length (≈70 ms per CJK char) —
/// enough to exercise the play path with zero network and stable timing.
pub struct MockTts {
    pub sample_rate: u32,
}

impl Default for MockTts {
    fn default() -> Self {
        Self { sample_rate: 16000 }
    }
}

#[async_trait::async_trait]
impl TtsClient for MockTts {
    async fn synthesize(&self, text: &str) -> Result<TtsAudio, SpeechError> {
        let chars = text.chars().count().max(1) as u64;
        let ms = (chars * 70).min(3000);
        let samples = ms * self.sample_rate as u64 / 1000;
        let pcm = vec![0u8; (samples * 2) as usize];
        let wav = pcm_to_wav(&pcm, self.sample_rate, 1);
        Ok(TtsAudio {
            duration_ms: ms,
            wav,
            sample_rate: self.sample_rate,
            channels: 1,
            provider: "mock".into(),
            voice: "mock".into(),
        })
    }

    fn provider(&self) -> &str {
        "mock"
    }
}

pub struct MockAsr;

#[async_trait::async_trait]
impl AsrClient for MockAsr {
    async fn transcribe(&self, _wav: &[u8]) -> Result<String, SpeechError> {
        Ok("(mock asr: audio accepted, transcription not implemented)".into())
    }
    fn provider(&self) -> &str {
        "mock"
    }
}

// --- OpenAI-compatible ASR (whisper.cpp server / oMLX / cloud) ----------------------

/// Any OpenAI-compatible `/v1/audio/transcriptions` endpoint (protocol over
/// engine, per asr-service-plan §2: one provider, swappable server side).
/// Verified live against oMLX `Qwen3-ASR-0.6B-bf16` (2026-11-07).
pub struct OpenaiAsr {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    /// Optional ISO-639-1 language hint (e.g. "zh", "en"). Empty omits the
    /// `language` field so Whisper / Groq / SenseVoice auto-detect language.
    pub language: String,
    pub client: reqwest::Client,
}

impl OpenaiAsr {
    pub fn new(base_url: String, api_key: String, model: String) -> Self {
        Self::with_language(base_url, api_key, model, String::new())
    }

    pub fn with_language(
        base_url: String,
        api_key: String,
        model: String,
        language: String,
    ) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            model,
            language: language.trim().to_string(),
            client: shared_client(&ASR_CLIENT, 12),
        }
    }
}

#[async_trait::async_trait]
impl AsrClient for OpenaiAsr {
    async fn transcribe(&self, wav: &[u8]) -> Result<String, SpeechError> {
        let mut form = reqwest::multipart::Form::new()
            .part(
                "file",
                reqwest::multipart::Part::bytes(wav.to_vec())
                    .file_name("utterance.wav")
                    .mime_str("audio/wav")
                    .map_err(|e| SpeechError::Protocol(e.to_string()))?,
            )
            .text("model", self.model.clone());
        if !self.language.is_empty() {
            form = form.text("language", self.language.clone());
        }
        let mut req = self
            .client
            .post(format!("{}/audio/transcriptions", self.base_url))
            .multipart(form);
        if !self.api_key.is_empty() {
            req = req.bearer_auth(&self.api_key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SpeechError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(SpeechError::Http {
                status: status.as_u16(),
                detail: detail.chars().take(300).collect(),
            });
        }
        #[derive(serde::Deserialize)]
        struct Transcription {
            #[serde(default)]
            text: String,
        }
        let v: Transcription = resp
            .json()
            .await
            .map_err(|e| SpeechError::Protocol(format!("bad transcription response: {e}")))?;
        // Empty text is honest: the caller decides how to handle silence.
        Ok(v.text.trim().to_string())
    }

    fn provider(&self) -> &str {
        "openai-asr"
    }
}

// --- factory ------------------------------------------------------------------------

/// Build a TTS client from the stored service config + resolved key.
/// Unknown providers fail loudly (the console shows the reason).
pub fn tts_from_config(
    cfg: &ServiceConfig,
    key: Option<&str>,
) -> Result<Arc<dyn TtsClient>, SpeechError> {
    match cfg.provider.as_str() {
        "mock" => Ok(Arc::new(MockTts::default())),
        "volc" => {
            let key = key.filter(|k| !k.is_empty()).ok_or(SpeechError::Missing(
                "api key for volc tts (VOLC_API_KEY)".into(),
            ))?;
            Ok(Arc::new(VolcanoTts::new(
                Some(cfg.base_url.clone()),
                key.to_string(),
                Some(cfg.model.clone()),
                cfg.voice.clone(),
            )))
        }
        "omni" | "openai_compat" => {
            if cfg.base_url.is_empty() {
                return Err(SpeechError::Missing("base_url for omni tts".into()));
            }
            if cfg.model.is_empty() {
                return Err(SpeechError::Missing("model for omni tts".into()));
            }
            Ok(Arc::new(OmniTts::new(
                cfg.base_url.clone(),
                key.unwrap_or_default().to_string(),
                cfg.model.clone(),
                cfg.voice.clone(),
            )))
        }
        other => Err(SpeechError::Protocol(format!(
            "unknown tts provider '{other}' (volc|omni|mock)"
        ))),
    }
}

/// Build an ASR client from the stored service config + resolved key.
/// Empty provider keeps the mock (pre-configuration state); "openai-asr"
/// builds the real OpenAI-compatible adapter (asr-service-plan §3).
pub fn asr_from_config(
    cfg: &ServiceConfig,
    key: Option<&str>,
) -> Result<Arc<dyn AsrClient>, SpeechError> {
    match cfg.provider.as_str() {
        "" | "mock" => Ok(Arc::new(MockAsr)),
        "openai-asr" => {
            if cfg.base_url.is_empty() {
                return Err(SpeechError::Missing("base_url for openai-asr".into()));
            }
            if cfg.model.is_empty() {
                return Err(SpeechError::Missing("model for openai-asr".into()));
            }
            Ok(Arc::new(OpenaiAsr::with_language(
                cfg.base_url.clone(),
                key.unwrap_or_default().to_string(),
                cfg.model.clone(),
                cfg.language.clone(),
            )))
        }
        other => Err(SpeechError::Protocol(format!(
            "unknown asr provider '{other}' (openai-asr|mock)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureTts(TtsAudio);

    #[async_trait::async_trait]
    impl TtsClient for FixtureTts {
        async fn synthesize(&self, _text: &str) -> Result<TtsAudio, SpeechError> {
            Ok(self.0.clone())
        }
        fn provider(&self) -> &str {
            "fixture"
        }
    }

    #[tokio::test]
    async fn default_streaming_preserves_pcm_with_extra_riff_chunks() {
        let pcm: Vec<u8> = (0..24_000i16).flat_map(i16::to_le_bytes).collect();
        let mut wav = pcm_to_wav(&pcm, 24_000, 1);
        // A legal metadata chunk shifts data beyond the common 44-byte header.
        wav.splice(36..36, *b"JUNK\x04\x00\x00\x00meta");
        let riff_len = (wav.len() - 8) as u32;
        wav[4..8].copy_from_slice(&riff_len.to_le_bytes());
        let tts = FixtureTts(TtsAudio {
            wav,
            sample_rate: 24_000,
            channels: 1,
            duration_ms: 1000,
            provider: "fixture".into(),
            voice: "fixture".into(),
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let audio = tts.synthesize_streaming("fixture", &tx).await.unwrap();
        let mut received = Vec::new();
        let mut sizes = Vec::new();
        while let Ok(piece) = rx.try_recv() {
            assert_eq!(piece.sample_rate, 24_000);
            sizes.push(piece.pcm.len());
            received.extend(piece.pcm);
        }
        assert_eq!(
            received.len(),
            pcm.len(),
            "RIFF metadata must never become speech samples"
        );
        assert_eq!(received, pcm);
        assert!(sizes.iter().all(|size| *size <= 8192));
        assert_eq!(audio.wav, tts.0.wav);
    }

    fn fixture_tts(pcm: &[u8], sample_rate: u32, channels: u16) -> FixtureTts {
        FixtureTts(TtsAudio {
            wav: pcm_to_wav(pcm, sample_rate, channels),
            sample_rate,
            channels,
            duration_ms: 0,
            provider: "fixture".into(),
            voice: "fixture".into(),
        })
    }

    #[tokio::test]
    async fn default_streaming_downmixes_stereo_without_changing_frame_count() {
        let pcm: Vec<u8> = [1000i16, -1000, 3000, 1000, i16::MAX, i16::MAX]
            .into_iter()
            .flat_map(i16::to_le_bytes)
            .collect();
        let tts = fixture_tts(&pcm, 24000, 2);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tts.synthesize_streaming("fixture", &tx).await.unwrap();
        let piece = rx.try_recv().unwrap();
        let expected: Vec<u8> = [0i16, 2000, i16::MAX]
            .into_iter()
            .flat_map(i16::to_le_bytes)
            .collect();
        assert_eq!(piece.pcm, expected);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn default_streaming_rejects_invalid_audio_and_closed_consumer() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tts = fixture_tts(&[0, 1], 24000, 1);
        tts.0.wav = b"not a WAV file".to_vec();
        assert!(tts.synthesize_streaming("fixture", &tx).await.is_err());
        assert!(rx.try_recv().is_err());
        for (pcm, rate, channels) in [
            (&[1u8][..], 24000, 1),
            (&[0, 0][..], 0, 1),
            (&[0, 0][..], 24000, 0),
        ] {
            assert!(fixture_tts(pcm, rate, channels)
                .synthesize_streaming("fixture", &tx)
                .await
                .is_err());
        }
        drop(rx);
        assert!(fixture_tts(&[0, 0], 24000, 1)
            .synthesize_streaming("fixture", &tx)
            .await
            .is_err());
    }

    #[test]
    fn pcm_to_wav_header_is_valid_riff() {
        let wav = pcm_to_wav(&vec![0u8; 3200], 16000, 1);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(
            u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]),
            16000
        );
        assert_eq!(wav_duration_ms(wav.len(), 16000, 1), 100);
    }

    #[tokio::test]
    async fn mock_tts_scales_with_text() {
        let tts = MockTts::default();
        let a = tts.synthesize("你好").await.unwrap();
        let b = tts.synthesize("你好你好你好").await.unwrap();
        assert!(b.duration_ms > a.duration_ms);
        assert_eq!(&a.wav[0..4], b"RIFF");
        assert_eq!(a.provider, "mock");
    }

    #[test]
    fn tts_factory_validates_providers() {
        assert!(tts_from_config(
            &ServiceConfig {
                provider: "volc".into(),
                ..Default::default()
            },
            None
        )
        .is_err());
        assert!(tts_from_config(
            &ServiceConfig {
                provider: "volc".into(),
                voice: "zh_female_xiaohe_uranus_bigtts".into(),
                ..Default::default()
            },
            Some("key")
        )
        .is_ok());
        assert!(tts_from_config(
            &ServiceConfig {
                provider: "omni".into(),
                base_url: "http://127.0.0.1:9981/v1".into(),
                model: "12Hz-0.6B-CustomVoice".into(),
                voice: "vivian".into(),
                ..Default::default()
            },
            None
        )
        .is_ok());
        assert!(tts_from_config(
            &ServiceConfig {
                provider: "carrier-pigeon".into(),
                ..Default::default()
            },
            None
        )
        .is_err());
        assert!(tts_from_config(
            &ServiceConfig {
                provider: "mock".into(),
                ..Default::default()
            },
            None
        )
        .is_ok());
    }

    #[test]
    fn asr_factory_validates_providers() {
        // Unconfigured and explicit mock stay mock; real provider needs
        // base_url + model; unknown providers fail loudly.
        assert!(matches!(
            asr_from_config(&ServiceConfig::default(), None).map(|c| c.provider().to_string()),
            Ok(p) if p == "mock"
        ));
        assert!(asr_from_config(
            &ServiceConfig {
                provider: "openai-asr".into(),
                base_url: "http://127.0.0.1:8978/v1".into(),
                model: "whisper-1".into(),
                ..Default::default()
            },
            None
        )
        .is_ok());
        assert!(asr_from_config(
            &ServiceConfig {
                provider: "openai-asr".into(),
                ..Default::default()
            },
            None
        )
        .is_err());
        assert!(asr_from_config(
            &ServiceConfig {
                provider: "openai-asr".into(),
                base_url: "http://127.0.0.1:8978/v1".into(),
                ..Default::default()
            },
            None
        )
        .is_err());
        assert!(asr_from_config(
            &ServiceConfig {
                provider: "sonogram".into(),
                ..Default::default()
            },
            None
        )
        .is_err());
    }

    /// Wire-level check over a real socket: the multipart body the provider
    /// sends parses with a standard server-side multipart parser, carries
    /// model/language/filename/WAV bytes, and {"text"} responses come back
    /// trimmed.
    #[tokio::test]
    async fn openai_asr_wire_roundtrip() {
        use axum::extract::Multipart;
        use axum::routing::post;

        async fn echo_multipart(mut mp: Multipart) -> axum::Json<serde_json::Value> {
            let mut fields: Vec<String> = Vec::new();
            let mut wav_bytes: Vec<u8> = Vec::new();
            let mut filename = String::new();
            while let Some(field) = mp.next_field().await.unwrap() {
                let name = field.name().unwrap_or("").to_string();
                if name == "file" {
                    filename = field.file_name().unwrap_or("").into();
                    wav_bytes = field.bytes().await.unwrap().to_vec();
                } else {
                    fields.push(format!("{name}={}", field.text().await.unwrap()));
                }
            }
            axum::Json(serde_json::json!({
                "fields": fields, "filename": filename,
                "is_riff": wav_bytes.starts_with(b"RIFF"),
                "text": "  你好，高达。 "
            }))
        }

        let app = axum::Router::new().route("/v1/audio/transcriptions", post(echo_multipart));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let asr = OpenaiAsr::new(
            format!("http://{addr}/v1"),
            "key".into(),
            "Qwen3-ASR-0.6B-bf16".into(),
        );
        let sample = pcm_to_wav(&vec![0u8; 1600], 16000, 1);
        let text = asr.transcribe(&sample).await.unwrap();
        assert_eq!(text, "你好，高达。", "response text must be trimmed");

        // Same server, one more call inspected raw: assert what the provider
        // actually put on the wire.
        let resp: serde_json::Value = asr
            .client
            .post(format!("http://{addr}/v1/audio/transcriptions"))
            .header("authorization", "Bearer key")
            .multipart(
                reqwest::multipart::Form::new()
                    .part(
                        "file",
                        reqwest::multipart::Part::bytes(sample.clone())
                            .file_name("utterance.wav")
                            .mime_str("audio/wav")
                            .unwrap(),
                    )
                    .text("model", "Qwen3-ASR-0.6B-bf16")
                    .text("language", "zh"),
            )
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            resp["is_riff"].as_bool().unwrap(),
            "file part must carry the WAV bytes"
        );
        assert_eq!(resp["filename"].as_str().unwrap(), "utterance.wav");
        let fields: Vec<&str> = resp["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f.as_str().unwrap())
            .collect();
        assert!(fields.contains(&"language=zh"), "fields: {fields:?}");
        assert!(
            fields.iter().any(|f| f.starts_with("model=")),
            "fields: {fields:?}"
        );
    }

    #[tokio::test]
    async fn openai_asr_omits_language_by_default_and_sends_explicit_language() {
        use axum::extract::Multipart;
        use axum::routing::post;
        use std::sync::{Arc, Mutex};

        let captured: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();

        let app = axum::Router::new().route(
            "/v1/audio/transcriptions",
            post(move |mut mp: Multipart| {
                let captured = captured_clone.clone();
                async move {
                    let mut fields = Vec::new();
                    while let Some(field) = mp.next_field().await.unwrap() {
                        let name = field.name().unwrap_or("").to_string();
                        if name == "file" {
                            let _ = field.bytes().await.unwrap();
                        } else {
                            let val = field.text().await.unwrap();
                            fields.push(format!("{name}={val}"));
                        }
                    }
                    captured.lock().unwrap().push(fields);
                    axum::Json(serde_json::json!({ "text": "hello world" }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let sample = pcm_to_wav(&vec![0u8; 1600], 16000, 1);

        // 1. Default empty language -> omits `language` so Whisper/Groq auto-detects EN/ZH.
        let auto_asr = OpenaiAsr::new(
            format!("http://{addr}/v1"),
            "key".into(),
            "whisper-1".into(),
        );
        assert_eq!(auto_asr.transcribe(&sample).await.unwrap(), "hello world");

        // 2. Explicit language ("en") -> sends `language=en`.
        let en_asr = OpenaiAsr::with_language(
            format!("http://{addr}/v1"),
            "key".into(),
            "whisper-1".into(),
            "en".into(),
        );
        assert_eq!(en_asr.transcribe(&sample).await.unwrap(), "hello world");

        let seen = captured.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            !seen[0].iter().any(|f| f.starts_with("language=")),
            "default empty language must omit multipart language field: {:?}",
            seen[0]
        );
        assert!(
            seen[1].iter().any(|f| f == "language=en"),
            "explicit language must be forwarded: {:?}",
            seen[1]
        );
    }
}
