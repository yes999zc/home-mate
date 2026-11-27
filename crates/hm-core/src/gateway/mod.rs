//! Device-gateway side of protocol v0 (core ↔ external gateway).
//!
//! The core LISTENS (default 127.0.0.1:8932, `HM_GATEWAY_BIND`); gateways
//! dial out. Frames are newline-delimited JSON; schema authority is
//! docs/contracts-v0.md. This module mirrors the frames deliberately: the
//! two repos share the documented contract and contract tests, never code
//! (see docs/device-gateway-project.md).
//!
//! Ownership (architecture principles §1): the hub owns connections,
//! generations, per-device seq freshness and pending-receipt matching. The
//! voice pipeline owns interaction semantics; nothing here interprets user
//! intent. Late/stale frames are recorded, never acted on.

use crate::id;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};

pub(crate) fn resolve_remote_vad(vad_speech: Option<bool>, vad_prob: Option<f32>) -> Option<bool> {
    vad_speech.or_else(|| {
        vad_prob
            .filter(|p| p.is_finite())
            .map(|p| p >= crate::silero::DEFAULT_THRESHOLD)
    })
}

mod audio_pump;
mod config;
#[cfg(test)]
mod config_tests;
mod frames;
mod negotiation;
mod northbound;
#[cfg(test)]
mod northbound_tests;
mod outbound;
mod stop;
#[cfg(test)]
mod tests;
mod transport_metrics;

pub use audio_pump::{DeviceAudioQueueHandle, DeviceAudioSync, DEVICE_AUDIO_QUEUE_CAP};
pub use config::{ConfigApplyRequest, ConfigValidateRequest};
pub use frames::*;
pub use negotiation::*;
pub use northbound::{NorthboundFault, NorthboundFaultCode, NorthboundStatus};
pub use stop::{StopOutcome, StopSubmission};

/// Soft per-syscall coalescing ceiling for queued outbound NDJSON frames.
/// 16 KiB packs ~6–7 remote media chunks into ~13 MTU-1280 Tailscale packets
/// per write — eliminating split-newline Nagle stalls without blasting 50+
/// packet micro-bursts into high-RTT wireless/cellular queues, while keeping
/// `Control` / `Voice` preemption granular and memory bounded on 128 MB nodes.
const OUTBOUND_WRITER_BATCH_BYTES: usize = 16 * 1024;
/// Kernel unsent-queue low-water mark (`24 KiB`) for non-loopback gateway
/// sockets (`TCP_NOTSENT_LOWAT`). Keeps bulk `Media` chunks in user space
/// instead of packing 200 KiB+ into kernel `SO_SNDBUF` ahead of `Voice` TTS
/// preemption (`#297`), while leaving `SO_SNDBUF` / BDP / LAN Hi-Fi untouched.
const GATEWAY_REMOTE_NOTSENT_LOWAT_BYTES: u32 = 24 * 1024;
/// Reclaim scratch buffer capacity after an unusually large single frame
/// (e.g. a one-shot WAV `Play` payload) so embedded nodes do not pin RAM.
const OUTBOUND_WRITER_MAX_RETAINED_CAP: usize = 64 * 1024;

impl DeviceHub {
    /// Disable Nagle's algorithm on accepted gateway sockets so 40 ms audio
    /// chunks and control frames never stall waiting on TCP Delayed ACK over
    /// Wi-Fi or Tailscale links, and apply `TCP_NOTSENT_LOWAT` on non-loopback
    /// sockets so kernel send queues never trap `Voice` TTS behind bulk media.
    fn configure_gateway_socket(stream: &TcpStream, peer_addr: Option<std::net::SocketAddr>) {
        if let Err(err) = stream.set_nodelay(true) {
            tracing::debug!(error = %err, "failed to enable TCP_NODELAY on gateway socket");
        }
        if peer_addr.is_some_and(|p| !crate::net::is_loopback_ip(p.ip()))
            && !crate::net::set_tcp_notsent_lowat(stream, GATEWAY_REMOTE_NOTSENT_LOWAT_BYTES)
        {
            tracing::debug!("TCP_NOTSENT_LOWAT unsupported or declined on remote gateway socket");
        }
    }

    /// Append a single `\n`-terminated JSON frame into `batch`, rolling back any
    /// partial bytes if serialization fails so corrupted fragments can never be
    /// flushed onto the wire.
    fn append_ndjson_frame<T: serde::Serialize>(batch: &mut Vec<u8>, frame: &T) -> bool {
        let frame_start = batch.len();
        if serde_json::to_writer(&mut *batch, frame).is_err() {
            batch.truncate(frame_start);
            return false;
        }
        batch.push(b'\n');
        true
    }

    async fn run_outbound_writer<W: tokio::io::AsyncWrite + Unpin>(
        mut rx: outbound::OutboundReceiver,
        mut wr: W,
    ) {
        let mut batch: Vec<u8> = Vec::with_capacity(OUTBOUND_WRITER_BATCH_BYTES);
        while let Some(first) = rx.recv().await {
            batch.clear();
            if !Self::append_ndjson_frame(&mut batch, &first) {
                break;
            }

            let mut disconnected = false;
            let mut serialize_err = false;
            while batch.len() < OUTBOUND_WRITER_BATCH_BYTES {
                match rx.try_recv() {
                    Ok(next) => {
                        if !Self::append_ndjson_frame(&mut batch, &next) {
                            serialize_err = true;
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            if wr.write_all(&batch).await.is_err() {
                break;
            }
            if batch.capacity() > OUTBOUND_WRITER_MAX_RETAINED_CAP {
                batch = Vec::with_capacity(OUTBOUND_WRITER_BATCH_BYTES);
            }
            if serialize_err || disconnected {
                break;
            }
        }
    }
}

/// A play submission the caller can await.
pub struct PlayTicket {
    pub play_id: String,
    pub receipt: oneshot::Receiver<GatewayFrame>,
}

#[derive(Debug, Clone)]
pub struct PlayRequest {
    pub device: String,
    pub content_ref: String,
    /// WAV bytes (already containerized) sent inline base64.
    pub audio: Option<Vec<u8>>,
    pub audio_meta: AudioMeta,
    pub channel: String,
    pub text: Option<String>,
    pub simulated_ms: Option<u64>,
}

struct ConnSlot {
    gateway: String,
    gateway_version: Option<String>,
    protocol_operating: u32,
    peer_addr: Option<std::net::SocketAddr>,
    authenticated: bool,
    devices: std::collections::HashSet<String>,
    outbound: outbound::OutboundSender,
    /// Dropping this sender wakes a superseded connection even while its writer
    /// is blocked in `write_all` and its reader remains open.
    retirement: Option<oneshot::Sender<()>>,
}

struct HubState {
    /// Monotonic connection generation allocator (1, 2, ...). Each live gateway
    /// connection owns a unique `conn_id = generation` entry in `connections`.
    generation: u64,
    /// #271: Connection-scoped live gateway slots (`conn_id -> ConnSlot`).
    /// Multiple distinct gateways (e.g. local loopback speaker + LAN phone)
    /// coexist concurrently without superseding each other.
    connections: HashMap<u64, ConnSlot>,
    /// #271: Active device ownership (`device_id -> owning conn_id`).
    device_to_conn: HashMap<String, u64>,
    connected_gateway: Option<String>,
    /// v1 mechanism: the machine-readable gateway/firmware package version
    /// from the live hello (empty for an older gateway), the negotiated
    /// operating protocol version, and the last link-level error the peer
    /// REPORTED (a `ProtocolError` frame). `last_link_error` is cleared by a
    /// successful handshake — it describes the previous rejection, never the
    /// live link.
    connected_gateway_version: Option<String>,
    protocol_operating: Option<u32>,
    last_link_error: Option<serde_json::Value>,
    /// Devices as last announced; seq freshness per device.
    devices: HashMap<String, DeviceSlot>,
    /// play_id → oneshot waiting for a terminal receipt, tagged with the
    /// connection generation that submitted it (a superseded connection's
    /// teardown must release ONLY its own waiters).
    pending: HashMap<String, PendingWaiter>,
    /// Per-device waiters for WifiState replies (v0.2.3 wifi change).
    pending_wifi: HashMap<String, oneshot::Sender<DeviceEvent>>,
    pending_wifi_gen: HashMap<String, u64>,
    config_state_waiters: HashMap<String, config::ConfigOneshotWaiter>,
    config_validate_waiters: HashMap<String, config::ConfigOneshotWaiter>,
    config_apply_waiters: HashMap<String, config::ConfigApplyWaiter>,
    /// volume task_id → device, so an asynchronous `volume_receipt` can be
    /// attributed even though the v0.2 frame carries no device field.
    volume_pending: HashMap<String, String>,
    volume_pending_gen: HashMap<String, u64>,
    /// Every play submitted on a live connection, any channel — the hub is
    /// the single registry of in-flight plays so stops can reach media
    /// (tool-submitted) plays as well as voice plays. Removed on terminal
    /// receipt / stop receipt / play_finished / connection teardown.
    active_plays: HashMap<String, ActivePlay>,
    /// R1j/P0-a: callers that must not put new audio on a channel until the
    /// device has actually FLUSHED a stopped play (the waiting cue's tail
    /// overlapped the answer's first syllable). Resolved by the same frames
    /// that end the play: stop_receipt, play_finished, failed/unknown.
    stop_waiters: HashMap<String, Vec<oneshot::Sender<StopOutcome>>>,
    /// Plays a Stop already left for (M3). `stop_all_plays` skips them: the
    /// wake path pauses media first and then stops every channel, and the
    /// device must not receive two Stops for one play.
    stop_sent: std::collections::HashSet<String>,
    /// M6a: play ids retired by a connection teardown. Bounded (cleared when
    /// it grows past 64 — precision for very old ids is worth less than an
    /// unbounded set). A receipt naming one of these crossed a generation
    /// and must never settle anything.
    retired_plays: std::collections::HashSet<String>,
    /// Monotonic arrival sequence for every frame handled on this hub. Used
    /// by management receipts: "a report that arrived after my command".
    frame_seq: u64,
    /// Diagnostic feed for the console (bounded).
    ring: VecDeque<serde_json::Value>,
}

struct PendingWaiter {
    sender: oneshot::Sender<GatewayFrame>,
    generation: u64,
}

#[derive(Debug, Clone)]
pub struct ActivePlay {
    pub device: String,
    pub channel: String,
    pub generation: u64,
}

struct DeviceSlot {
    descriptor: HelloDevice,
    generation: u64,
    last_seq: u64,
    online: bool,
    /// Last volume the DEVICE reported (linear 0..1), not what we asked for.
    /// Updated by `volume_receipt` — including the boot report and the
    /// firmware's own volume keys (`vol:dev:*`), which is what makes the
    /// console follow physical changes.
    volume: Option<f64>,
    volume_at_ms: Option<u64>,
    /// Hub frame sequence of the last CONFIRMED volume receipt.
    volume_seq: u64,
    /// W1 (#72) & #114 (A5): AEC state and tail length reported in Hello.
    aec_enabled: Option<bool>,
    aec_tail_ms: Option<u32>,
    aec_tail_samples: Option<u32>,
    aec_sample_rate: Option<u32>,
    aec_provenance: Option<String>,
    /// W1 (#71/#74): Acoustic calibration profile version reported by gateway.
    profile_version: Option<String>,
    /// W1 (#72/#73): Latest echo-health state ("healthy" | "degraded" | ...).
    echo_health: Option<String>,
    echo_degraded_ms: Option<u64>,
    echo_health_reason: Option<String>,
    northbound: Option<northbound::NorthboundRecord>,
    gateway_boot_id: Option<String>,
    state_revision: Option<u64>,
    capability_fingerprint: Option<String>,
    config_blocks: HashMap<ConfigBlock, ConfigBlockState>,
    /// Persistent trust-tier reservations (survive `retire_connection`) so an
    /// unauthenticated `lan_gateway_exempt` peer can never hijack a loopback or
    /// authenticated gateway's `stable_id` during a reconnect window.
    reserved_loopback: bool,
    reserved_authenticated: bool,
    /// True when the latest live connection that registered this device was a
    /// non-loopback, unauthenticated LAN gateway (e.g. `lan_gateway_exempt`).
    /// Preserved across brief disconnects so in-flight ASR turns fail closed.
    last_unauth_lan: bool,
}

impl DeviceSlot {
    fn can_be_claimed_by(&self, new_is_loopback: bool, new_authenticated: bool) -> bool {
        if self.reserved_loopback && !new_is_loopback {
            return false;
        }
        if self.reserved_authenticated && !new_authenticated {
            return false;
        }
        true
    }
}

impl HubState {
    fn record(&mut self, kind: &str, mut payload: serde_json::Value) {
        payload["kind"] = json!(kind);
        payload["ts_ms"] = json!(id::now_ms());
        self.ring.push_back(payload);
        while self.ring.len() > 100 {
            self.ring.pop_front();
        }
    }

    fn conn_is_live(&self, generation: u64) -> bool {
        if self.connections.is_empty() {
            self.generation == generation
        } else {
            self.connections.contains_key(&generation)
        }
    }

    fn device_frame_stale(&self, generation: u64, device: &str) -> bool {
        if !self.conn_is_live(generation) {
            return true;
        }
        if let Some(&owner) = self.device_to_conn.get(device) {
            return owner != generation;
        }
        if let Some(conn) = self.connections.get(&generation) {
            // Production connections (`peer_addr.is_some()` or announced devices)
            // must declare their device ids in Hello (populating `conn.devices` and
            // `device_to_conn`). Reject any frame naming an unregistered phantom device,
            // including connections that sent `Hello { devices: [] }`.
            if !conn.devices.is_empty() || conn.peer_addr.is_some() {
                return true;
            }
            if let Some(slot) = self.devices.get(device) {
                return slot.generation != generation;
            }
        }
        false
    }

    fn conn_is_loopback(conn: &ConnSlot) -> bool {
        conn.peer_addr
            .is_some_and(|p| crate::net::is_loopback_ip(p.ip()))
    }

    /// Trust hierarchy for multi-gateway device_id supersession (Phase 3 P0-1):
    /// 1. A non-loopback connection must NEVER supersede a loopback speaker gateway.
    /// 2. An unauthenticated connection (e.g. `lan_gateway_exempt`) must NEVER
    ///    supersede an authenticated connection.
    fn conn_can_supersede(new_is_loopback: bool, new_authenticated: bool, old: &ConnSlot) -> bool {
        if Self::conn_is_loopback(old) && !new_is_loopback {
            return false;
        }
        if old.authenticated && !new_authenticated {
            return false;
        }
        true
    }

    fn refresh_summary_fields(&mut self) {
        if let Some((_, best)) = self
            .connections
            .iter()
            .max_by_key(|(&gen_id, conn)| (Self::conn_is_loopback(conn), gen_id))
        {
            self.connected_gateway = Some(best.gateway.clone());
            self.connected_gateway_version = best.gateway_version.clone();
            self.protocol_operating = Some(best.protocol_operating);
        } else {
            self.connected_gateway = None;
            self.connected_gateway_version = None;
            self.protocol_operating = None;
        }
    }

    fn cleanup_connection_resources(&mut self, generation: u64) {
        let retiring: Vec<String> = self
            .active_plays
            .iter()
            .filter(|(_, p)| p.generation == generation)
            .map(|(id, _)| id.clone())
            .chain(
                self.pending
                    .iter()
                    .filter(|(_, w)| w.generation == generation)
                    .map(|(id, _)| id.clone()),
            )
            .collect();
        for id in retiring {
            DeviceHub::resolve_stop_waiters(self, &id, StopOutcome::unknown("connection_retired"));
            self.stop_sent.remove(&id);
            self.retired_plays.insert(id);
        }
        if self.retired_plays.len() > 64 {
            self.retired_plays.clear();
        }
        self.pending.retain(|_, w| w.generation != generation);
        self.config_state_waiters
            .retain(|_, w| w.generation != generation);
        self.config_validate_waiters
            .retain(|_, w| w.generation != generation);
        self.config_apply_waiters
            .retain(|_, w| w.generation != generation);
        let stale_vol: Vec<String> = self
            .volume_pending_gen
            .iter()
            .filter(|(_, &g)| g == generation)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale_vol {
            self.volume_pending.remove(&k);
            self.volume_pending_gen.remove(&k);
        }
        let stale_wifi: Vec<String> = self
            .pending_wifi_gen
            .iter()
            .filter(|(_, &g)| g == generation)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale_wifi {
            self.pending_wifi.remove(&k);
            self.pending_wifi_gen.remove(&k);
        }
        self.active_plays.retain(|_, p| p.generation != generation);
    }
}

/// Record a device-reported volume. The receipt is the truth (a requested
/// value that failed must not move the console). Only a `device_confirmed`
/// receipt may move the snapshot: `accepted`/`sent` are not the device's
/// resulting state, and a `failed`/`unknown` receipt must never be read as
/// success (R2-BM6 audit — a failed receipt used to update `volume` anyway,
/// which the management tool could then render as 「已调到」).
/// Old v0.2 frames carry no device field: a `vol:boot:`/`vol:dev:` receipt is
/// attributed when the owning connection (or the hub) knows exactly one device;
/// an API task is attributed through `volume_pending`. Returns the device when
/// attribution succeeded AND the stage confirmed it.
#[cfg(test)]
fn record_volume(
    st: &mut HubState,
    task_id: &str,
    effective: f64,
    stage: &str,
    frame_seq: u64,
    hint: Option<&str>,
) -> Option<String> {
    record_volume_on(st, None, task_id, effective, stage, frame_seq, hint)
}

fn record_volume_on(
    st: &mut HubState,
    generation: Option<u64>,
    task_id: &str,
    effective: f64,
    stage: &str,
    frame_seq: u64,
    hint: Option<&str>,
) -> Option<String> {
    if let Some(gen_id) = generation {
        if !st.conn_is_live(gen_id) {
            return None;
        }
    }
    let pending_matches = match (generation, st.volume_pending_gen.get(task_id)) {
        (Some(gen_id), Some(&owner_gen)) => owner_gen == gen_id,
        _ => true,
    };
    let terminal = matches!(stage, "failed" | "unknown");
    if !effective.is_finite() || !(0.0..=1.0).contains(&effective) {
        if pending_matches {
            st.volume_pending.remove(task_id);
            st.volume_pending_gen.remove(task_id);
        }
        return None;
    }
    if stage != "device_confirmed" {
        if terminal && pending_matches {
            st.volume_pending.remove(task_id);
            st.volume_pending_gen.remove(task_id);
        }
        return None;
    }
    let hinted = hint
        .filter(|d| {
            st.devices.contains_key(*d)
                && generation.is_none_or(|gen_id| !st.device_frame_stale(gen_id, d))
        })
        .map(str::to_string);
    // Unconditionally clean up matching pending volume entries on receipt
    // resolution even when `VolumeReceipt.device` is `Some(...)`, preventing
    // unbounded map growth (`MG-P2-02`).
    let pending_device = if pending_matches {
        st.volume_pending_gen.remove(task_id);
        st.volume_pending.remove(task_id)
    } else {
        None
    };
    let device = hinted.or(pending_device).or_else(|| {
        let device_fact = task_id.starts_with("vol:boot:") || task_id.starts_with("vol:dev:");
        if !device_fact {
            return None;
        }
        if let Some(gen_id) = generation {
            if let Some(conn) = st.connections.get(&gen_id) {
                if conn.devices.len() == 1 {
                    return conn.devices.iter().next().cloned();
                }
                return None;
            }
        }
        if st.devices.len() == 1 {
            st.devices.keys().next().cloned()
        } else {
            None
        }
    })?;
    if let Some(gen_id) = generation {
        if st.device_frame_stale(gen_id, &device) {
            return None;
        }
    }
    if let Some(slot) = st.devices.get_mut(&device) {
        slot.volume = Some(effective);
        slot.volume_at_ms = Some(id::now_ms());
        slot.volume_seq = frame_seq;
        Some(device)
    } else {
        None
    }
}

fn ev_device(ev: &DeviceEvent) -> &str {
    match ev {
        DeviceEvent::WifiState { device, .. } => device,
        _ => "",
    }
}

#[derive(Debug, Clone)]
pub struct QueuedDeviceEvent {
    pub seq: u64,
    pub enqueued_at: std::time::Instant,
    /// For `DeviceEvent::Keyword`, the latest `AudioChunk` sequence enqueued on
    /// this device immediately before the keyword arrived on TCP (`0` if none).
    pub audio_barrier_seq: u64,
    pub event: DeviceEvent,
}

fn is_critical_control_event(ev: &DeviceEvent) -> bool {
    matches!(
        ev,
        DeviceEvent::Wake { .. }
            | DeviceEvent::Keyword { .. }
            | DeviceEvent::LocalExitFlushed { .. }
            | DeviceEvent::Button { .. }
            | DeviceEvent::PlayFinished { .. }
    )
}

pub type GatewayHub = DeviceHub;

pub struct DeviceHub {
    transport_tasks: transport_metrics::TransportTasks,
    state: Mutex<HubState>,
    /// Dedicated high-capacity channel for control/lifecycle events so an
    /// `AudioChunk` flood can never lag/drop `Wake`, `Keyword`, `PlayFinished`,
    /// `StopStage`, `LocalExitFlushed`, `MuteState`, etc.
    control_events: broadcast::Sender<DeviceEvent>,
    /// Bounded lossy channel for raw `AudioChunk` frames (may lag/drop under load).
    audio_events: broadcast::Sender<DeviceEvent>,
    /// Timed control channel for the VoicePipeline event pump.
    pump_control_events: broadcast::Sender<QueuedDeviceEvent>,
    /// Per-device bounded FIFO audio queues for the VoicePipeline VAD workers (`MG-P1-03`).
    audio_pump: audio_pump::AudioPumpRouter,
    pump_seq: std::sync::atomic::AtomicU64,
    /// Lossless fallback buffer for critical control events (`Wake`, `Keyword`,
    /// `LocalExitFlushed`, `Button`, `PlayFinished`) when `pump_control_events` lags.
    critical_events: Mutex<VecDeque<QueuedDeviceEvent>>,
    /// Household Bearer token store for authenticating non-loopback LAN gateways (#271).
    auth: Mutex<Option<Arc<crate::auth::AuthStore>>>,
    /// True when the gateway listener is bound to a non-loopback interface.
    voice_gateway_open: std::sync::atomic::AtomicBool,
    pub bind_addr: Mutex<String>,
    /// Media-channel output trim in dB (0 = off). Applied to the WAV payload
    /// of every media play at submit time; voice plays are never trimmed.
    /// Live values come from VoiceSettings via VoicePipeline.
    media_trim_db: Mutex<f64>,
    /// Per-device media-channel trim overrides (multi-gateway isolation).
    media_trim_by_device: Mutex<HashMap<String, f64>>,
}

impl DeviceHub {
    pub fn new() -> Arc<Self> {
        let (control_tx, _) = broadcast::channel(4096);
        let (audio_tx, _) = broadcast::channel(256);
        let (pump_control_tx, _) = broadcast::channel(4096);
        Arc::new(Self {
            transport_tasks: Default::default(),
            state: Mutex::new(HubState {
                generation: 0,
                connections: HashMap::new(),
                device_to_conn: HashMap::new(),
                connected_gateway: None,
                connected_gateway_version: None,
                protocol_operating: None,
                last_link_error: None,
                devices: HashMap::new(),
                pending: HashMap::new(),
                pending_wifi: HashMap::new(),
                pending_wifi_gen: HashMap::new(),
                config_state_waiters: HashMap::new(),
                config_validate_waiters: HashMap::new(),
                config_apply_waiters: HashMap::new(),
                volume_pending: HashMap::new(),
                volume_pending_gen: HashMap::new(),
                frame_seq: 0,
                active_plays: HashMap::new(),
                stop_waiters: HashMap::new(),
                stop_sent: std::collections::HashSet::new(),
                retired_plays: std::collections::HashSet::new(),
                ring: VecDeque::new(),
            }),
            control_events: control_tx,
            audio_events: audio_tx,
            pump_control_events: pump_control_tx,
            audio_pump: audio_pump::AudioPumpRouter::new(),
            pump_seq: std::sync::atomic::AtomicU64::new(0),
            critical_events: Mutex::new(VecDeque::new()),
            auth: Mutex::new(None),
            voice_gateway_open: std::sync::atomic::AtomicBool::new(false),
            bind_addr: Mutex::new(String::new()),
            media_trim_db: Mutex::new(crate::voice::default_media_trim_db()),
            media_trim_by_device: Mutex::new(HashMap::new()),
        })
    }

    pub fn attach_auth(&self, auth: Arc<crate::auth::AuthStore>) {
        *crate::lock_or_recover(&self.auth, "hub.auth") = Some(auth);
    }

    fn auth_store(&self) -> Option<Arc<crate::auth::AuthStore>> {
        crate::lock_or_recover(&self.auth, "hub.auth").clone()
    }

    pub fn set_voice_gateway_open(&self, open: bool) {
        self.voice_gateway_open
            .store(open, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn voice_gateway_open(&self) -> bool {
        self.voice_gateway_open
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    fn attach_test_outbound(&self, generation: u64, tx: outbound::OutboundSender) {
        let mut st = self.lock_state();
        if st.generation < generation {
            st.generation = generation;
        }
        st.connections.insert(
            generation,
            ConnSlot {
                gateway: "test-gw".into(),
                gateway_version: None,
                protocol_operating: crate::DEVICE_PROTOCOL_VERSION,
                peer_addr: None,
                authenticated: true,
                devices: std::collections::HashSet::new(),
                outbound: tx,
                retirement: None,
            },
        );
        st.refresh_summary_fields();
    }

    #[cfg(test)]
    pub(crate) fn detach_test_outbounds(&self) {
        let mut st = self.lock_state();
        st.connections.clear();
        st.device_to_conn.clear();
        st.refresh_summary_fields();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, HubState> {
        crate::lock_or_recover(&self.state, "hub.state")
    }

    /// Accept loop (#271: concurrent multi-gateway connections). Each incoming
    /// gateway connection runs independently; supersession is strictly
    /// per-device so a phone/display gateway never disconnects a local speaker.
    /// Hard-capped at 32 concurrent connections so a LAN peer cannot exhaust
    /// sockets or 8 MiB line buffers.
    pub async fn listen(self: &Arc<Self>, addr: std::net::SocketAddr) -> std::io::Result<()> {
        const MAX_CONCURRENT_GATEWAY_CONNS: usize = 32;
        let listener = TcpListener::bind(addr).await?;
        let conn_sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_GATEWAY_CONNS));
        *crate::lock_or_recover(&self.bind_addr, "hub.bind_addr") = addr.to_string();
        self.set_voice_gateway_open(!addr.ip().is_loopback());
        tracing::info!(%addr, voice_gateway_open = !addr.ip().is_loopback(), "gateway listener up");
        loop {
            let (stream, peer) = listener.accept().await?;
            let Ok(permit) = conn_sem.clone().try_acquire_owned() else {
                tracing::warn!(%peer, max = MAX_CONCURRENT_GATEWAY_CONNS, "rejected gateway connection: max concurrent connections reached");
                drop(stream);
                continue;
            };
            let hub = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = hub.serve_connection_with_peer(stream, Some(peer)).await {
                    tracing::warn!(%peer, error = %e, "gateway connection ended with error");
                }
            });
        }
    }

    /// Subscribe to control/lifecycle events (never carries `AudioChunk`).
    pub fn subscribe_control(&self) -> broadcast::Receiver<DeviceEvent> {
        self.control_events.subscribe()
    }

    /// Subscribe to timed control/lifecycle events for the VoicePipeline pump.
    pub fn subscribe_pump_control(&self) -> broadcast::Receiver<QueuedDeviceEvent> {
        self.pump_control_events.subscribe()
    }

    /// Subscribe to raw `AudioChunk` frames (lossy under backpressure).
    pub fn subscribe_audio(&self) -> broadcast::Receiver<DeviceEvent> {
        self.audio_events.subscribe()
    }

    /// Take the stream of lazily created per-device bounded audio queues (`MG-P1-03`).
    pub fn take_pump_audio_queues(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<DeviceAudioQueueHandle>> {
        self.audio_pump.take_queue_stream()
    }

    /// Total `AudioChunk` frames dropped across all per-device queues.
    pub fn audio_dropped_total(&self) -> u64 {
        self.audio_pump.dropped_total()
    }

    /// Per-device `AudioChunk` drop counters (`device -> dropped_count`).
    pub fn audio_dropped_by_device(&self) -> std::collections::BTreeMap<String, u64> {
        self.audio_pump.dropped_by_device()
    }

    /// Await completion of all per-device `AudioChunk` VAD processing enqueued
    /// up to `up_to_seq`, bounded by `timeout`. Returns `true` when drained or
    /// when no audio queue exists; returns `false` on timeout.
    pub async fn audio_drained_up_to(
        &self,
        device: &str,
        up_to_seq: u64,
        timeout: std::time::Duration,
    ) -> bool {
        if up_to_seq == 0 {
            return true;
        }
        let Some(sync) = self.audio_pump.sync_for(device) else {
            return true;
        };
        let target = sync.enqueued_seq().min(up_to_seq);
        if target == 0 {
            return true;
        }
        tokio::time::timeout(timeout, sync.wait_processed(target))
            .await
            .is_ok()
    }

    /// Recover any critical control events with `after_seq < seq < before_seq`.
    pub fn take_critical_between(&self, after_seq: u64, before_seq: u64) -> Vec<QueuedDeviceEvent> {
        let mut crit = crate::lock_or_recover(&self.critical_events, "hub.critical_events");
        let mut out = Vec::new();
        while let Some(front) = crit.front() {
            if front.seq >= before_seq {
                break;
            }
            let Some(item) = crit.pop_front() else {
                break;
            };
            if item.seq > after_seq {
                out.push(item);
            }
        }
        out
    }

    /// Recover any critical control events with `seq > after_seq`.
    pub fn take_critical_after(&self, after_seq: u64) -> Vec<QueuedDeviceEvent> {
        let mut crit = crate::lock_or_recover(&self.critical_events, "hub.critical_events");
        let mut out = Vec::new();
        while let Some(item) = crit.pop_front() {
            if item.seq > after_seq {
                out.push(item);
            }
        }
        out
    }

    /// Acknowledge critical control events up to `seq` so they can be evicted.
    pub fn ack_critical_up_to(&self, seq: u64) {
        let mut crit = crate::lock_or_recover(&self.critical_events, "hub.critical_events");
        while crit.front().is_some_and(|e| e.seq <= seq) {
            crit.pop_front();
        }
    }

    fn emit_control(&self, ev: DeviceEvent) {
        let _ = self.control_events.send(ev.clone());
        let audio_barrier_seq = match &ev {
            DeviceEvent::Keyword { device, .. } => self.audio_pump.latest_enqueued_seq(device),
            _ => 0,
        };
        // Allocate `seq`, enqueue into `critical_events`, and broadcast on
        // `pump_control_events` under one short mutex so both channels observe
        // strictly monotonic sequence order with no out-of-order critical loss.
        let mut crit = crate::lock_or_recover(&self.critical_events, "hub.critical_events");
        let seq = self
            .pump_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let queued = QueuedDeviceEvent {
            seq,
            enqueued_at: std::time::Instant::now(),
            audio_barrier_seq,
            event: ev,
        };
        if is_critical_control_event(&queued.event) && self.pump_control_events.receiver_count() > 0
        {
            if crit.len() >= 65_536 {
                crit.pop_front();
            }
            crit.push_back(queued.clone());
        }
        let _ = self.pump_control_events.send(queued);
    }

    fn emit_audio(&self, ev: DeviceEvent) {
        if self.audio_events.receiver_count() > 0 {
            let _ = self.audio_events.send(ev.clone());
        }
        let seq = self
            .pump_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let device = match &ev {
            DeviceEvent::AudioChunk { device, .. } => device.clone(),
            _ => String::new(),
        };
        if !device.is_empty() {
            self.audio_pump.push(
                &device,
                QueuedDeviceEvent {
                    seq,
                    enqueued_at: std::time::Instant::now(),
                    audio_barrier_seq: 0,
                    event: ev,
                },
            );
        }
    }

    /// Backwards-compatible alias for `subscribe_control()`.
    pub fn subscribe(&self) -> broadcast::Receiver<DeviceEvent> {
        self.control_events.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn handle_frame_for_test(&self, generation: u64, frame: GatewayFrame) {
        self.handle_frame(generation, frame);
    }

    #[cfg(test)]
    pub(crate) fn set_generation_for_test(&self, generation: u64) {
        self.lock_state().generation = generation;
    }

    #[cfg(test)]
    pub(crate) fn poison_state_for_test(self: &Arc<Self>) {
        let hub = self.clone();
        let _ = std::thread::spawn(move || {
            let _g = hub.state.lock().unwrap();
            panic!("simulated panic while holding hub.state");
        })
        .join();
    }

    #[cfg(test)]
    pub(crate) async fn serve_connection(
        self: Arc<Self>,
        stream: TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let peer_addr = stream.peer_addr().ok();
        self.serve_connection_with_peer(stream, peer_addr).await
    }

    /// Public connection entry that allows integration tests to exercise
    /// non-loopback peer authentication deterministically over loopback TCP.
    pub async fn serve_connection_from(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.serve_connection_with_peer(stream, peer_addr).await
    }

    async fn send_hello_reject(
        wr: &mut tokio::net::tcp::OwnedWriteHalf,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let reject = CoreFrame::HelloReject {
            protocol: crate::DEVICE_PROTOCOL_VERSION,
            min_protocol: crate::DEVICE_PROTOCOL_MIN_VERSION,
            core_version: crate::CORE_VERSION.to_string(),
            reason: reason.to_string(),
        };
        let mut line = serde_json::to_string(&reject)?;
        line.push('\n');
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(REJECT_WRITE_TIMEOUT_MS),
            wr.write_all(line.as_bytes()),
        )
        .await;
        Ok(())
    }

    async fn serve_connection_with_peer(
        self: Arc<Self>,
        stream: TcpStream,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _connection_task =
            transport_metrics::LiveTask::enter(&self.transport_tasks.connections);
        Self::configure_gateway_socket(&stream, peer_addr);
        let (rd, mut wr) = stream.into_split();
        let mut lines = CappedLineReader::new(BufReader::new(rd));
        let (tx, rx) =
            outbound::outbound_channel_with_metrics(self.transport_tasks.outbound.clone());
        let link_health = tx.link_health();

        // Handshake: bounded wait (a connected-but-silent peer must not park
        // this task forever; the line cap applies from the first byte).
        let hello_line =
            match tokio::time::timeout(std::time::Duration::from_secs(10), lines.next_line()).await
            {
                Err(_) => return Err("hello not received within 10s".into()),
                Ok(Err(e)) => return Err(e),
                Ok(Ok(None)) => return Err("connection closed before hello".into()),
                Ok(Ok(Some(l))) => l,
            };
        if let Some(rtt_us) = crate::net::tcp_smoothed_rtt_us(lines.inner.get_ref().as_ref()) {
            link_health.set_rtt_us(rtt_us);
        }
        let gateway_name;
        let gateway_version: Option<String>;
        let operating: u32;
        let announced: Vec<HelloDevice>;
        let hello_auth_token: Option<String>;
        let hello_aec_tail_ms: Option<u32>;
        let hello_aec_enabled: Option<bool>;
        let hello_aec_tail_samples: Option<u32>;
        let hello_aec_sample_rate: Option<u32>;
        let hello_aec_provenance: Option<String>;
        let hello_profile_version: Option<String>;
        let hello_extra: serde_json::Value =
            serde_json::from_str(&hello_line).unwrap_or(serde_json::Value::Null);
        match serde_json::from_str::<GatewayFrame>(&hello_line)? {
            GatewayFrame::Hello {
                protocol,
                min_protocol,
                supported_versions,
                gateway,
                gateway_version: version,
                devices,
                aec_tail_ms,
                profile_version,
                auth_token,
            } => {
                let mine = (
                    crate::DEVICE_PROTOCOL_MIN_VERSION,
                    crate::DEVICE_PROTOCOL_VERSION,
                );
                let peer = (min_protocol, protocol);
                let Some(v) = protocol_operating_version_with_supported(
                    mine,
                    crate::DEVICE_PROTOCOL_SUPPORTED_VERSIONS,
                    peer,
                    &supported_versions,
                ) else {
                    let reason = protocol_no_intersection_reason(mine, peer);
                    {
                        let mut st = self.lock_state();
                        st.last_link_error = Some(json!({
                            "kind": "protocol_error", "reason": reason,
                            "peer_protocol": protocol, "peer_min_protocol": min_protocol,
                        }));
                        st.record("handshake_rejected", json!({ "reason": reason }));
                    }
                    tracing::warn!(%gateway, reason, "refused gateway hello");
                    Self::send_hello_reject(&mut wr, &reason).await?;
                    return Err(format!("protocol rejected: {reason}").into());
                };
                if let Err(reason) = ensure_unique_device_ids(&devices) {
                    {
                        let mut st = self.lock_state();
                        st.last_link_error =
                            Some(json!({ "kind": "protocol_error", "reason": reason }));
                        st.record("handshake_rejected", json!({ "reason": reason }));
                    }
                    tracing::warn!(%gateway, reason, "refused gateway hello");
                    Self::send_hello_reject(&mut wr, &reason).await?;
                    return Err(format!("protocol rejected: {reason}").into());
                }
                const MAX_DEVICES_PER_HELLO: usize = 8;
                if devices.len() > MAX_DEVICES_PER_HELLO {
                    let reason = "too_many_devices";
                    {
                        let mut st = self.lock_state();
                        st.record(
                            "handshake_rejected",
                            json!({ "reason": reason, "count": devices.len() }),
                        );
                    }
                    tracing::warn!(%gateway, count = devices.len(), "refused gateway hello with too many devices");
                    Self::send_hello_reject(&mut wr, reason).await?;
                    return Err(format!("protocol rejected: {reason}").into());
                }
                gateway_name = gateway;
                gateway_version = (!version.is_empty()).then_some(version);
                operating = v;
                announced = devices;
                hello_auth_token = auth_token;
                hello_aec_tail_ms = aec_tail_ms.or_else(|| {
                    hello_extra
                        .pointer("/aec/tail_ms")
                        .and_then(|v| v.as_u64())
                        .map(|v| v as u32)
                });
                hello_aec_enabled = hello_extra
                    .get("aec_enabled")
                    .or_else(|| hello_extra.pointer("/aec/enabled"))
                    .and_then(|v| v.as_bool())
                    .or_else(|| hello_aec_tail_ms.map(|ms| ms > 0));
                hello_aec_sample_rate = hello_extra
                    .get("aec_sample_rate")
                    .or_else(|| hello_extra.pointer("/aec/sample_rate"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .or_else(|| (hello_aec_enabled == Some(true)).then_some(16_000));
                hello_aec_tail_samples = hello_extra
                    .get("effective_tail_samples")
                    .or_else(|| hello_extra.pointer("/aec/effective_tail_samples"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .or_else(|| {
                        hello_aec_tail_ms
                            .zip(hello_aec_sample_rate)
                            .map(|(ms, sr)| ms.saturating_mul(sr) / 1000)
                    });
                hello_aec_provenance = hello_extra
                    .get("aec_provenance")
                    .or_else(|| hello_extra.pointer("/aec/provenance"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                hello_profile_version = profile_version.filter(|s| !s.trim().is_empty());
            }
            f => return Err(format!("expected hello, got {f:?}").into()),
        }

        // #271: Authenticate non-loopback peers against the Core's household
        // Bearer token store when paired. Loopback (127.0.0.1 / ::1) remains
        // zero-config trusted for local speaker gateways. Never log auth_token.
        let is_loopback = peer_addr.is_some_and(|a| crate::net::is_loopback_ip(a.ip()));
        let authenticated = if is_loopback {
            true
        } else if let Some(auth) = self.auth_store() {
            if auth.configured() {
                if let Some(peer_ip) = peer_addr.map(|a| a.ip()) {
                    if auth.is_rate_limited(peer_ip).is_some() {
                        {
                            let mut st = self.lock_state();
                            st.record(
                                "handshake_rejected",
                                json!({ "reason": "unauthorized", "rate_limited": true, "peer": peer_addr.map(|p| p.to_string()) }),
                            );
                        }
                        tracing::warn!(%gateway_name, ?peer_addr, "rejected rate-limited LAN gateway hello");
                        Self::send_hello_reject(&mut wr, "unauthorized").await?;
                        return Err("unauthorized".into());
                    }
                }
                let presented = hello_auth_token
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let lan_exempt_peer = presented.is_none()
                    && auth.lan_gateway_exempt()
                    && peer_addr.is_some_and(|a| crate::net::is_private_lan_ip(a.ip()));
                if lan_exempt_peer {
                    tracing::info!(
                        %gateway_name,
                        ?peer_addr,
                        "accepted private-LAN gateway hello via lan_gateway_exempt"
                    );
                    false
                } else {
                    match presented.and_then(|tok| auth.verify(tok)) {
                        Some(token_id) => {
                            auth.touch(&token_id);
                            if let Some(peer_ip) = peer_addr.map(|a| a.ip()) {
                                auth.clear_auth_failures(peer_ip);
                            }
                            true
                        }
                        None => {
                            if let Some(peer_ip) = peer_addr.map(|a| a.ip()) {
                                auth.record_auth_failure(peer_ip);
                            }
                            {
                                let mut st = self.lock_state();
                                st.record(
                                    "handshake_rejected",
                                    json!({ "reason": "unauthorized", "peer": peer_addr.map(|p| p.to_string()) }),
                                );
                            }
                            tracing::warn!(%gateway_name, ?peer_addr, "rejected unauthorized LAN gateway hello");
                            Self::send_hello_reject(&mut wr, "unauthorized").await?;
                            return Err("unauthorized".into());
                        }
                    }
                }
            } else if self.voice_gateway_open()
                && peer_addr.is_some_and(|a| crate::net::is_private_lan_ip(a.ip()))
            {
                // Unpaired dev/NAS Core with opt-in LAN listener open (private LAN / Tailscale only).
                false
            } else {
                {
                    let mut st = self.lock_state();
                    st.record(
                        "handshake_rejected",
                        json!({ "reason": "unauthorized", "peer": peer_addr.map(|p| p.to_string()) }),
                    );
                }
                tracing::warn!(%gateway_name, ?peer_addr, "rejected non-loopback gateway hello while LAN gateway closed or non-LAN peer");
                Self::send_hello_reject(&mut wr, "unauthorized").await?;
                return Err("unauthorized".into());
            }
        } else if self.voice_gateway_open()
            && peer_addr.is_some_and(|a| crate::net::is_private_lan_ip(a.ip()))
        {
            false
        } else {
            Self::send_hello_reject(&mut wr, "unauthorized").await?;
            return Err("unauthorized".into());
        };

        let (retirement_tx, retirement_rx) = oneshot::channel::<()>();
        let admit = {
            let mut st = self.lock_state();
            st.last_link_error = None;
            st.generation += 1;
            let gen = st.generation;

            // #271: Per-device supersession only. Retire a prior connection if
            // it owned any overlapping device_id (same gateway/device
            // reconnecting), while never touching sibling connections that own
            // distinct device_ids.
            let announced_ids: std::collections::HashSet<&str> =
                announced.iter().map(|d| d.stable_id.as_str()).collect();
            let mut to_retire: Vec<u64> = Vec::new();
            let mut dropped_devices: Vec<String> = Vec::new();
            let mut reserved_device: Option<String> = None;
            if announced.is_empty() {
                for (&old_gen, conn) in &st.connections {
                    if conn.devices.is_empty() && conn.gateway == gateway_name {
                        if HubState::conn_can_supersede(is_loopback, authenticated, conn) {
                            to_retire.push(old_gen);
                        }
                    }
                }
            } else {
                for d in &announced {
                    if let Some(existing_slot) = st.devices.get(&d.stable_id) {
                        if !existing_slot.can_be_claimed_by(is_loopback, authenticated) {
                            reserved_device = Some(d.stable_id.clone());
                            break;
                        }
                    }
                    if let Some(&old_gen) = st.device_to_conn.get(&d.stable_id) {
                        if old_gen != gen {
                            if let Some(old_conn) = st.connections.get(&old_gen) {
                                if !HubState::conn_can_supersede(
                                    is_loopback,
                                    authenticated,
                                    old_conn,
                                ) {
                                    reserved_device = Some(d.stable_id.clone());
                                    break;
                                }
                            }
                            if !to_retire.contains(&old_gen) {
                                to_retire.push(old_gen);
                            }
                        }
                    }
                }
            }
            if let Some(reserved_id) = reserved_device {
                st.record(
                    "handshake_rejected",
                    json!({
                        "reason": "device_id_reserved",
                        "device": reserved_id,
                        "peer": peer_addr.map(|p| p.to_string()),
                    }),
                );
                Err(reserved_id)
            } else {
                for &old_gen in &to_retire {
                    if let Some(mut old_conn) = st.connections.remove(&old_gen) {
                        // Explicitly drop the retirement sender so the old
                        // connection wakes immediately even if blocked in write_all.
                        drop(old_conn.retirement.take());
                        for old_dev in old_conn.devices {
                            if !announced_ids.contains(old_dev.as_str())
                                && st.device_to_conn.get(&old_dev) == Some(&old_gen)
                            {
                                st.device_to_conn.remove(&old_dev);
                                if let Some(slot) = st.devices.get_mut(&old_dev) {
                                    if slot.generation == old_gen {
                                        slot.online = false;
                                    }
                                }
                                st.record(
                                    "device_left",
                                    json!({ "device": old_dev, "generation": old_gen }),
                                );
                                dropped_devices.push(old_dev);
                            }
                        }
                    }
                    st.cleanup_connection_resources(old_gen);
                }

                let mut owned_devices = std::collections::HashSet::new();
                for d in &announced {
                    owned_devices.insert(d.stable_id.clone());
                    st.device_to_conn.insert(d.stable_id.clone(), gen);
                    let (previous_northbound, prev_reserved_loopback, prev_reserved_auth) = st
                        .devices
                        .get(&d.stable_id)
                        .map(|s| {
                            (
                                s.northbound.clone(),
                                s.reserved_loopback,
                                s.reserved_authenticated,
                            )
                        })
                        .unwrap_or((None, false, false));
                    st.devices.insert(
                        d.stable_id.clone(),
                        DeviceSlot {
                            descriptor: d.clone(),
                            generation: gen,
                            last_seq: 0,
                            online: true,
                            volume: None,
                            volume_at_ms: None,
                            volume_seq: 0,
                            aec_enabled: hello_aec_enabled,
                            aec_tail_ms: hello_aec_tail_ms,
                            aec_tail_samples: hello_aec_tail_samples,
                            aec_sample_rate: hello_aec_sample_rate,
                            aec_provenance: hello_aec_provenance.clone(),
                            profile_version: hello_profile_version.clone(),
                            echo_health: None,
                            echo_degraded_ms: None,
                            echo_health_reason: None,
                            northbound: previous_northbound,
                            gateway_boot_id: None,
                            state_revision: None,
                            capability_fingerprint: None,
                            config_blocks: HashMap::new(),
                            reserved_loopback: prev_reserved_loopback || is_loopback,
                            reserved_authenticated: prev_reserved_auth || authenticated,
                            last_unauth_lan: peer_addr.is_some() && !is_loopback && !authenticated,
                        },
                    );
                    st.record(
                        "device_joined",
                        json!({
                            "device": d.stable_id,
                            "generation": gen,
                            "aec_tail_ms": hello_aec_tail_ms,
                            "profile_version": hello_profile_version,
                        }),
                    );
                }
                st.connections.insert(
                    gen,
                    ConnSlot {
                        gateway: gateway_name.clone(),
                        gateway_version: gateway_version.clone(),
                        protocol_operating: operating,
                        peer_addr,
                        authenticated,
                        devices: owned_devices,
                        outbound: tx.clone(),
                        retirement: Some(retirement_tx),
                    },
                );
                st.refresh_summary_fields();
                let active_connections = st.connections.len();
                st.record(
                    "gateway_connected",
                    json!({
                        "gateway": gateway_name,
                        "gateway_version": gateway_version,
                        "protocol_operating": operating,
                        "generation": gen,
                        "peer": peer_addr.map(|p| p.to_string()),
                        "authenticated": authenticated,
                        "active_connections": active_connections,
                    }),
                );
                Ok((gen, to_retire, dropped_devices))
            }
        };
        let (generation, superseded_conns, dropped_devices) = match admit {
            Ok(admitted) => admitted,
            Err(reserved_id) => {
                tracing::warn!(
                    %gateway_name,
                    ?peer_addr,
                    device = %reserved_id,
                    "rejected gateway hello attempting to hijack reserved device_id"
                );
                Self::send_hello_reject(&mut wr, "device_id_reserved").await?;
                return Err("device_id_reserved".into());
            }
        };
        if !superseded_conns.is_empty() {
            tracing::warn!(
                ?superseded_conns,
                generation,
                "gateway connection superseded prior connection(s) owning the same device"
            );
        }
        for dev in dropped_devices {
            self.audio_pump.remove_device(&dev);
            self.emit_control(DeviceEvent::DeviceLeft { device: dev });
        }
        self.emit_control(DeviceEvent::GatewayConnected {
            gateway: gateway_name.clone(),
            generation,
        });
        for d in &announced {
            self.audio_pump.ensure_device(&d.stable_id, is_loopback);
            self.emit_control(DeviceEvent::DeviceJoined {
                device: d.clone(),
                generation,
                aec_tail_ms: hello_aec_tail_ms,
                profile_version: hello_profile_version.clone(),
            });
        }

        // Say hello_ack (and project the host clock — emOS has no time
        // service; G6 replaced-capability #2), then pump both directions.
        // The hub owns the only outbound sender. The independent retirement
        // signal also interrupts a superseded writer blocked on the socket.
        tx.send(CoreFrame::HelloAck {
            protocol: operating,
            min_protocol: crate::DEVICE_PROTOCOL_MIN_VERSION,
            supported_versions: if operating < 2 {
                vec![operating]
            } else {
                crate::DEVICE_PROTOCOL_SUPPORTED_VERSIONS.to_vec()
            },
            capabilities: if operating < 2 {
                Vec::new()
            } else {
                vec!["config_apply_v2".to_string()]
            },
            core_version: crate::CORE_VERSION.to_string(),
            generation,
        })
        .map_err(|_| "outbound channel closed before hello_ack")?;
        let _ = tx.send(CoreFrame::TimeSet {
            ts_ms: crate::id::now_ms(),
        });
        drop(tx);

        let writer_lifetime = transport_metrics::LiveTask::enter(&self.transport_tasks.writers);
        let mut writer = tokio::spawn(async move {
            let _writer_task = writer_lifetime;
            Self::run_outbound_writer(rx, wr).await;
        });

        let hub = self.clone();
        let reader_lifetime = transport_metrics::LiveTask::enter(&self.transport_tasks.readers);
        let reader = async move {
            let _reader_future = reader_lifetime;
            while let Some(line) = lines.next_line().await? {
                if let Some(rtt_us) =
                    crate::net::tcp_smoothed_rtt_us(lines.inner.get_ref().as_ref())
                {
                    link_health.set_rtt_us(rtt_us);
                }
                if line.is_empty() {
                    continue;
                }
                let frame: GatewayFrame = match serde_json::from_str(&line) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(error = %e, "undecodable gateway frame");
                        continue;
                    }
                };
                hub.handle_frame(generation, frame);
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        // Exit when EITHER side ends: reader EOF (gateway gone) or writer
        // task done (superseded by a newer connection closed our channel).
        let (res, writer_joined): (Result<(), Box<dyn std::error::Error + Send + Sync>>, bool) = tokio::select! {
            r = reader => (r, false),
            _ = &mut writer => (Err("outbound channel closed".into()), true),
            _ = retirement_rx => (Err("connection superseded".into()), false),
        };
        // Dropping a JoinHandle detaches it: a half-closed peer that does not
        // read could keep the old writer and queued payloads forever. Retire
        // that actual task and TCP half before reporting connection teardown.
        if !writer_joined {
            writer.abort();
            let _ = writer.await;
        }
        // Connection over. A CURRENT generation tears down its devices,
        // outbound slot and events; a SUPERSEDED generation may only release
        // its own pending waiters and active plays — never the live
        // connection's (they cannot receive receipts anyway, so waiters see
        // a closed channel = unknown outcome).
        self.retire_connection(generation);
        res
    }

    fn retire_connection(&self, generation: u64) {
        let mut st = self.lock_state();
        let was_live = st.connections.remove(&generation).is_some();
        // #271: Only mark offline and release devices still owned by THIS
        // connection. Sibling connections and devices already handed off to a
        // superseding connection remain untouched.
        let left: Vec<String> = st
            .device_to_conn
            .iter()
            .filter(|(_, &owner)| owner == generation)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &left {
            st.device_to_conn.remove(id);
            let prune_slot = if let Some(dev) = st.devices.get_mut(id) {
                if dev.generation == generation {
                    dev.online = false;
                    !dev.reserved_loopback && !dev.reserved_authenticated
                } else {
                    false
                }
            } else {
                false
            };
            if prune_slot {
                st.devices.remove(id);
            }
            st.record(
                "device_left",
                json!({ "device": id, "generation": generation }),
            );
            self.audio_pump.remove_device(id);
            self.emit_control(DeviceEvent::DeviceLeft { device: id.clone() });
        }
        st.cleanup_connection_resources(generation);
        st.refresh_summary_fields();
        if was_live {
            st.record("gateway_disconnected", json!({ "generation": generation }));
            self.emit_control(DeviceEvent::GatewayDisconnected { generation });
        } else {
            st.record(
                "gateway_connection_superseded_teardown",
                json!({ "generation": generation }),
            );
        }
    }

    /// Route one gateway frame: freshness check on events, receipt matching
    /// for play/stop/volume, state updates for mode reports.
    fn handle_frame(&self, generation: u64, frame: GatewayFrame) {
        let mut st = self.lock_state();
        // Monotonic ARRIVAL sequence, not a wall-clock millisecond: a report
        // that arrived before a management command must never be able to
        // confirm it, even when both happen in the same millisecond or the
        // voice loop consumes the queued event later (audit round 2).
        st.frame_seq = st.frame_seq.wrapping_add(1);
        let frame_seq = st.frame_seq;
        let transport = TransportIdentity {
            generation,
            inbound_seq: frame_seq,
        };
        // Events from a retired/superseded connection generation: record only.
        let stale_generation = !st.conn_is_live(generation);
        match frame {
            GatewayFrame::Hello { .. } => {
                st.record("late_hello", json!({ "generation": generation }));
            }
            GatewayFrame::ProtocolError {
                protocol,
                min_protocol,
                gateway_version,
                reason,
            } => {
                // v1 mechanism: the gateway refused the link and said why
                // before disconnecting. A fact about the PREVIOUS handshake
                // attempt — recorded here and surfaced by /status; cleared by
                // the next successful handshake, never by this frame.
                st.last_link_error = Some(json!({
                    "kind": "protocol_error",
                    "reason": reason,
                    "peer_protocol": protocol,
                    "peer_min_protocol": min_protocol,
                    "gateway_version": gateway_version,
                    "ts_ms": id::now_ms(),
                }));
                st.record(
                    "link_protocol_error",
                    json!({ "reason": reason, "peer_protocol": protocol }),
                );
                tracing::warn!(%reason, peer_protocol = protocol, "gateway reported a protocol error");
            }
            GatewayFrame::Wake {
                device,
                ts,
                seq,
                origin,
            } => {
                let provenance = InputProvenance {
                    origin,
                    transport: Some(transport),
                };
                if st.device_frame_stale(generation, &device) || !seq_fresh(&mut st, &device, seq) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "seq": seq, "kind": "wake" }),
                    );
                    return;
                }
                st.record(
                    "wake",
                    json!({ "device": device, "seq": seq, "provenance": provenance }),
                );
                self.emit_control(DeviceEvent::Wake {
                    device,
                    ts,
                    seq,
                    provenance,
                });
            }
            GatewayFrame::Utterance {
                device,
                ts,
                seq,
                source,
                text,
                origin,
            } => {
                let provenance = InputProvenance {
                    origin,
                    transport: Some(transport),
                };
                if st.device_frame_stale(generation, &device) || !seq_fresh(&mut st, &device, seq) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "seq": seq, "kind": "utterance" }),
                    );
                    return;
                }
                st.record("utterance", json!({ "device": device, "seq": seq, "source": source, "text": text, "provenance": provenance }));
                self.emit_control(DeviceEvent::Utterance {
                    device,
                    ts,
                    seq,
                    source,
                    text,
                    provenance,
                });
            }
            GatewayFrame::Button {
                device,
                ts,
                seq,
                action,
            } => {
                if st.device_frame_stale(generation, &device) || !seq_fresh(&mut st, &device, seq) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "seq": seq, "kind": "button" }),
                    );
                    return;
                }
                st.record(
                    "button",
                    json!({ "device": device, "seq": seq, "action": action }),
                );
                self.emit_control(DeviceEvent::Button {
                    device,
                    ts,
                    seq,
                    action,
                });
            }
            GatewayFrame::MuteState {
                device,
                ts,
                seq,
                muted,
                source,
                task_id,
            } => {
                if st.device_frame_stale(generation, &device) || !seq_fresh(&mut st, &device, seq) {
                    // If this stale/mismatched MuteState directly answers a pending
                    // `mute_set` issued on this same connection generation, deliver it
                    // only to that waiter so `device_mute` fails fast with `mismatch`
                    // instead of timing out, without emitting any phantom DeviceEvent.
                    if let Some(tid) = task_id.as_ref() {
                        if st
                            .pending
                            .get(tid)
                            .is_some_and(|w| w.generation == generation)
                        {
                            if let Some(waiter) = st.pending.remove(tid) {
                                let _ = waiter.sender.send(GatewayFrame::MuteState {
                                    device: device.clone(),
                                    ts,
                                    seq,
                                    muted,
                                    source: source.clone(),
                                    task_id: Some(tid.clone()),
                                });
                            }
                        }
                    }
                    st.record(
                        "stale_event",
                        json!({ "device": device, "seq": seq, "kind": "mute_state" }),
                    );
                    return;
                }
                st.record(
                    "mute_state",
                    json!({ "device": device, "muted": muted, "source": source, "task_id": task_id }),
                );
                // v1 mechanism: settle THE command this fact answers. A
                // physical-key press (or a boot report) carries no task_id and
                // must never resolve a remote command's waiter; a fact that
                // crossed a connection generation settles nothing either.
                if let Some(tid) = task_id.clone() {
                    if let Some(waiter) = st.pending.remove(&tid) {
                        if waiter.generation == generation {
                            let _ = waiter.sender.send(GatewayFrame::MuteState {
                                device: device.clone(),
                                ts,
                                seq,
                                muted,
                                source: source.clone(),
                                task_id: Some(tid),
                            });
                        }
                    }
                }
                self.emit_control(DeviceEvent::MuteState {
                    device,
                    ts,
                    seq,
                    muted,
                    source,
                    task_id,
                });
            }
            GatewayFrame::PlayReceipt {
                task_id: play_id,
                stage,
                detail,
                ts,
            } => {
                if Self::receipt_is_stale(
                    &mut st,
                    &play_id,
                    generation,
                    json!({ "frame": "play_receipt", "stage": stage, "detail": detail }),
                ) {
                    return;
                }
                st.record(
                    "play_receipt",
                    json!({ "play_id": play_id, "stage": stage, "detail": detail }),
                );
                let is_terminal =
                    matches!(stage.as_str(), "device_confirmed" | "failed" | "unknown");
                if is_terminal {
                    if let Some(waiter) = st.pending.remove(&play_id) {
                        let _ = waiter.sender.send(GatewayFrame::PlayReceipt {
                            task_id: play_id.clone(),
                            stage: stage.clone(),
                            detail: detail.clone(),
                            ts,
                        });
                    } else {
                        // Late or unknown-play receipt: record only (the ring
                        // entry above is the audit trail).
                        tracing::debug!(%play_id, %stage, "receipt with no waiter");
                    }
                }
                // failed/unknown end the play; device_confirmed means it is
                // RUNNING now (removed later by stop_receipt/play_finished).
                if matches!(stage.as_str(), "failed" | "unknown") {
                    st.active_plays.remove(&play_id);
                    st.stop_sent.remove(&play_id);
                    Self::resolve_stop_waiters(
                        &mut st,
                        &play_id,
                        if stage == "failed" {
                            StopOutcome::Failed {
                                reason: detail.clone().unwrap_or(stage.clone()),
                            }
                        } else {
                            StopOutcome::unknown(detail.clone().unwrap_or(stage.clone()))
                        },
                    );
                }
                self.emit_control(DeviceEvent::PlayStage {
                    play_id,
                    stage,
                    detail,
                });
            }
            GatewayFrame::StopReceipt {
                task_id: play_id,
                stage,
                played_ms,
                consumed_ms,
                progress_basis,
                detail,
            } => {
                if Self::receipt_is_stale(
                    &mut st,
                    &play_id,
                    generation,
                    json!({ "frame": "stop_receipt", "stage": stage, "played_ms": played_ms, "detail": detail }),
                ) {
                    return;
                }
                st.record(
                    "stop_receipt",
                    json!({
                        "play_id": play_id, "stage": stage, "played_ms": played_ms,
                        "consumed_ms": consumed_ms, "progress_basis": progress_basis,
                        "detail": detail,
                    }),
                );
                st.active_plays.remove(&play_id);
                st.stop_sent.remove(&play_id);
                // M5: a stop receipt IS the play's terminal. The waiter used
                // to be released only by `play_finished`, so a caller that
                // waited for ONE play's end (the per-sentence resume cursor)
                // hung until its timeout on every interruption and then had
                // no idea what the DAC had consumed.
                if let Some(waiter) = st.pending.remove(&play_id) {
                    let _ = waiter.sender.send(GatewayFrame::StopReceipt {
                        task_id: play_id.clone(),
                        stage: stage.clone(),
                        played_ms,
                        consumed_ms,
                        progress_basis: progress_basis.clone(),
                        detail: detail.clone(),
                    });
                }
                let outcome = StopOutcome::receipt(
                    &stage,
                    detail.as_deref(),
                    played_ms,
                    consumed_ms,
                    progress_basis.clone(),
                );
                Self::resolve_stop_waiters(&mut st, &play_id, outcome.clone());
                st.record("stop_outcome", json!({"play_id":play_id,"outcome":outcome}));
                self.emit_control(DeviceEvent::StopStage {
                    outcome,
                    play_id,
                    stage,
                    played_ms,
                    consumed_ms,
                    progress_basis,
                });
            }
            GatewayFrame::VolumeReceipt {
                task_id: play_id,
                effective,
                unit,
                stage,
                device,
            } => {
                if stale_generation
                    || device
                        .as_deref()
                        .is_some_and(|d| st.device_frame_stale(generation, d))
                {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "play_id": play_id, "kind": "volume_receipt" }),
                    );
                    return;
                }
                let device = record_volume_on(
                    &mut st,
                    Some(generation),
                    &play_id,
                    effective,
                    &stage,
                    frame_seq,
                    device.as_deref(),
                );
                st.record(
                    "volume_receipt",
                    json!({
                        "play_id": play_id, "effective": effective, "unit": unit,
                        "stage": stage, "device": device,
                    }),
                );
                self.emit_control(DeviceEvent::VolumeStage {
                    play_id,
                    effective,
                    stage,
                });
            }
            GatewayFrame::PlayRejected {
                task_id,
                device,
                reason,
                ts,
            } => {
                if st.device_frame_stale(generation, &device)
                    || Self::receipt_is_stale(
                        &mut st,
                        &task_id,
                        generation,
                        json!({ "frame": "play_rejected", "reason": reason }),
                    )
                {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "play_id": task_id, "kind": "play_rejected" }),
                    );
                    return;
                }
                st.active_plays.remove(&task_id);
                st.stop_sent.remove(&task_id);
                Self::resolve_stop_waiters(
                    &mut st,
                    &task_id,
                    StopOutcome::unknown("play_rejected"),
                );
                if let Some(waiter) = st.pending.remove(&task_id) {
                    let _ = waiter.sender.send(GatewayFrame::PlayRejected {
                        task_id: task_id.clone(),
                        device: device.clone(),
                        reason: reason.clone(),
                        ts,
                    });
                }
                st.record(
                    "play_rejected",
                    json!({ "play_id": task_id, "device": device, "reason": reason }),
                );
                self.emit_control(DeviceEvent::PlayRejected {
                    play_id: task_id,
                    device,
                    reason,
                    ts,
                });
            }
            f @ GatewayFrame::ConfigState { .. } => {
                self.handle_config_state(&mut st, generation, f);
            }
            f @ GatewayFrame::ConfigValidated { .. } => {
                self.handle_config_validated(&mut st, generation, f);
            }
            f @ GatewayFrame::ConfigReceipt { .. } => {
                self.handle_config_receipt(&mut st, generation, f);
            }
            GatewayFrame::ModeReport {
                device,
                duplex,
                aec,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "mode_report" }),
                    );
                    return;
                }
                st.record(
                    "mode_report",
                    json!({ "device": device, "duplex": duplex, "aec": aec }),
                );
                self.emit_control(DeviceEvent::ModeReport {
                    device,
                    duplex,
                    aec,
                    seq: frame_seq,
                });
            }
            GatewayFrame::WifiState {
                device,
                ts,
                ssid,
                state,
                detail,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "wifi_state" }),
                    );
                    return;
                }
                // handle_frame runs under the hub state lock — reuse the
                // `st` guard; re-locking here deadlocks the same thread
                // (std Mutex is not reentrant; caught by wifi_wedge_repro).
                let ev = DeviceEvent::WifiState {
                    device,
                    ts,
                    ssid,
                    state,
                    detail,
                };
                let dev_key = ev_device(&ev).to_string();
                let waiter_matches = st
                    .pending_wifi_gen
                    .get(&dev_key)
                    .is_none_or(|&owner| owner == generation);
                if waiter_matches {
                    st.pending_wifi_gen.remove(&dev_key);
                    if let Some(tx) = st.pending_wifi.remove(&dev_key) {
                        let _ = tx.send(ev.clone());
                    }
                }
                self.emit_control(ev);
            }
            GatewayFrame::PlayFinished {
                device,
                task_id: play_id,
                played_ms,
                consumed_ms,
                progress_basis,
                ts,
            } => {
                if st.device_frame_stale(generation, &device)
                    || Self::receipt_is_stale(
                        &mut st,
                        &play_id,
                        generation,
                        json!({ "frame": "play_finished", "played_ms": played_ms, "consumed_ms": consumed_ms }),
                    )
                {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "play_id": play_id, "kind": "play_finished" }),
                    );
                    return;
                }
                st.record(
                    "play_finished",
                    json!({
                        "device": device, "play_id": play_id, "played_ms": played_ms,
                        "consumed_ms": consumed_ms, "progress_basis": progress_basis,
                    }),
                );
                st.active_plays.remove(&play_id);
                st.stop_sent.remove(&play_id);
                // A finish IS a terminal fact for a parked play waiter: a
                // device that skips the `device_confirmed` receipt must not
                // hold a caller (the wake ack waits on it) for its full bound.
                if let Some(waiter) = st.pending.remove(&play_id) {
                    let _ = waiter.sender.send(GatewayFrame::PlayFinished {
                        device: device.clone(),
                        task_id: play_id.clone(),
                        played_ms,
                        consumed_ms,
                        progress_basis: progress_basis.clone(),
                        ts,
                    });
                }
                Self::resolve_stop_waiters(
                    &mut st,
                    &play_id,
                    StopOutcome::unknown("play_finished_without_flush"),
                );
                self.emit_control(DeviceEvent::PlayFinished {
                    device,
                    play_id,
                    played_ms,
                    consumed_ms,
                    progress_basis,
                    ts,
                });
            }
            GatewayFrame::AudioChunk {
                device,
                ts,
                seq,
                pcm_b64,
                format,
                sample_rate,
                channels,
                layout,
                origin,
                vad_speech,
                vad_prob,
            } => {
                let provenance = InputProvenance {
                    origin,
                    transport: Some(transport),
                };
                let remote_vad = resolve_remote_vad(vad_speech, vad_prob);
                // Steady-cadence frames (dot2-d ≈ 12.5/s): seq-check yes, ring
                // NO — recording every chunk would flush the diagnostic ring
                // in seconds. Loss shows up as a seq gap recorded here.
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "seq": seq, "kind": "audio_chunk" }),
                    );
                    return;
                }
                let has_announced_conn = st
                    .connections
                    .values()
                    .any(|c| !c.devices.is_empty() || c.peer_addr.is_some());
                let seq_check = match st.devices.get_mut(&device) {
                    Some(slot) if seq > slot.last_seq => {
                        let expected = slot.last_seq + 1;
                        slot.last_seq = seq;
                        if seq != expected {
                            Some(format!("gap: seq {seq}, expected {expected}"))
                        } else {
                            None
                        }
                    }
                    Some(_) => {
                        st.record("audio_chunk_stale", json!({ "device": device, "seq": seq }));
                        return;
                    }
                    None if has_announced_conn => {
                        st.record(
                            "audio_chunk_unknown_device",
                            json!({ "device": device, "seq": seq }),
                        );
                        return;
                    }
                    None => {
                        st.record(
                            "audio_chunk_unknown_device",
                            json!({ "device": device, "seq": seq }),
                        );
                        None
                    }
                };
                if let Some(anomaly) = seq_check {
                    st.record(
                        "audio_chunk_gap",
                        json!({ "device": device, "anomaly": anomaly }),
                    );
                }
                if format != "s16le" {
                    st.record(
                        "audio_chunk_bad_format",
                        json!({ "device": device, "format": format }),
                    );
                    return;
                }
                // channels feeds a modulo below: 0 would panic and poison
                // the hub lock (review P2-1). v0.2 layouts are 1|2 only.
                if !matches!(channels, 1 | 2) {
                    st.record(
                        "audio_chunk_bad_channels",
                        json!({ "device": device, "channels": channels }),
                    );
                    return;
                }
                use base64::Engine;
                let raw = match base64::engine::general_purpose::STANDARD.decode(&pcm_b64) {
                    Ok(r) => r,
                    Err(e) => {
                        st.record(
                            "audio_chunk_bad_b64",
                            json!({ "device": device, "error": e.to_string() }),
                        );
                        return;
                    }
                };
                // Whole frames only: 2ch mic_ref must carry whole stereo
                // pairs (odd sample counts would silently lose the last
                // sample in the pipeline's downmix).
                if raw.len() % 2 != 0 || raw.len() % (channels as usize * 2) != 0 {
                    st.record(
                        "audio_chunk_bad_alignment",
                        json!({ "device": device, "len": raw.len(), "channels": channels }),
                    );
                    return;
                }
                let pcm = raw
                    .chunks_exact(2)
                    .map(|b| i16::from_le_bytes([b[0], b[1]]))
                    .collect();
                self.emit_audio(DeviceEvent::AudioChunk {
                    device,
                    ts,
                    seq,
                    pcm,
                    sample_rate,
                    channels,
                    layout,
                    provenance,
                    remote_vad,
                });
            }
            GatewayFrame::MicReport {
                device,
                active,
                layout,
                sample_rate,
                channels,
                detail,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "mic_report" }),
                    );
                    return;
                }
                st.record("mic_report", json!({ "device": device, "active": active, "layout": layout, "sample_rate": sample_rate, "channels": channels, "detail": detail }));
                self.emit_control(DeviceEvent::MicReport {
                    device,
                    active,
                    detail,
                });
            }
            GatewayFrame::DeviceStatus {
                device,
                online,
                reason,
                descriptor,
                northbound,
            } => {
                if stale_generation {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "device_status" }),
                    );
                    return;
                }
                // v0.2 online governance + #271 multi-gateway ownership:
                // use THIS connection's `generation` (never global `st.generation`).
                match descriptor {
                    Some(desc) if desc.stable_id == device => {
                        let (new_loopback, new_auth) = st
                            .connections
                            .get(&generation)
                            .map(|c| (HubState::conn_is_loopback(c), c.authenticated))
                            .unwrap_or((false, false));
                        if let Some(existing_slot) = st.devices.get(&device) {
                            if !existing_slot.can_be_claimed_by(new_loopback, new_auth) {
                                st.record(
                                    "stale_event",
                                    json!({ "device": device, "kind": "device_status_reserved" }),
                                );
                                return;
                            }
                        }
                        if let Some(&old_gen) = st.device_to_conn.get(&device) {
                            if old_gen != generation {
                                // Only a live re-registration (`online == true`)
                                // may take ownership from a different live
                                // connection; a non-owner reporting `online == false`
                                // is rejected as stale.
                                if !online {
                                    st.record(
                                        "stale_event",
                                        json!({ "device": device, "kind": "device_status_non_owner_offline" }),
                                    );
                                    return;
                                }
                                if let Some(old_conn) = st.connections.get(&old_gen) {
                                    if !HubState::conn_can_supersede(
                                        new_loopback,
                                        new_auth,
                                        old_conn,
                                    ) {
                                        st.record(
                                            "stale_event",
                                            json!({ "device": device, "kind": "device_status_reserved" }),
                                        );
                                        return;
                                    }
                                }
                                let retire_old = st
                                    .connections
                                    .get_mut(&old_gen)
                                    .map(|old_conn| {
                                        old_conn.devices.remove(&device);
                                        old_conn.devices.is_empty()
                                    })
                                    .unwrap_or(false);
                                if retire_old {
                                    if let Some(mut old_conn) = st.connections.remove(&old_gen) {
                                        drop(old_conn.retirement.take());
                                    }
                                    st.cleanup_connection_resources(old_gen);
                                    st.refresh_summary_fields();
                                } else {
                                    // Evict active plays / stop waiters belonging to the
                                    // reclaimed `(old_gen, device)` while keeping sibling
                                    // devices on `old_gen` intact.
                                    let reclaimed_plays: Vec<String> = st
                                        .active_plays
                                        .iter()
                                        .filter(|(_, p)| {
                                            p.generation == old_gen && p.device == device
                                        })
                                        .map(|(id, _)| id.clone())
                                        .collect();
                                    for pid in reclaimed_plays {
                                        DeviceHub::resolve_stop_waiters(
                                            &mut st,
                                            &pid,
                                            StopOutcome::unknown("device_reclaimed"),
                                        );
                                        st.stop_sent.remove(&pid);
                                        st.pending.remove(&pid);
                                        st.active_plays.remove(&pid);
                                        st.retired_plays.insert(pid);
                                    }
                                }
                            }
                        }
                        let new_unauth_lan = st.connections.get(&generation).is_some_and(|c| {
                            c.peer_addr.is_some()
                                && !HubState::conn_is_loopback(c)
                                && !c.authenticated
                        });
                        if let Some(conn) = st.connections.get_mut(&generation) {
                            conn.devices.insert(device.clone());
                            st.device_to_conn.insert(device.clone(), generation);
                        }
                        let slot = st.devices.entry(device.clone()).or_insert(DeviceSlot {
                            descriptor: desc.clone(),
                            generation,
                            last_seq: 0,
                            online: false,
                            volume: None,
                            volume_at_ms: None,
                            volume_seq: 0,
                            aec_enabled: None,
                            aec_tail_ms: None,
                            aec_tail_samples: None,
                            aec_sample_rate: None,
                            aec_provenance: None,
                            profile_version: None,
                            echo_health: None,
                            echo_degraded_ms: None,
                            echo_health_reason: None,
                            northbound: None,
                            gateway_boot_id: None,
                            state_revision: None,
                            capability_fingerprint: None,
                            config_blocks: HashMap::new(),
                            reserved_loopback: new_loopback,
                            reserved_authenticated: new_auth,
                            last_unauth_lan: new_unauth_lan,
                        });
                        slot.reserved_loopback |= new_loopback;
                        slot.reserved_authenticated |= new_auth;
                        slot.last_unauth_lan = new_unauth_lan;
                        if slot.generation != generation {
                            slot.generation = generation;
                            slot.last_seq = 0;
                        }
                        slot.descriptor = desc;
                        slot.online = online;
                    }
                    None => {
                        if let Some(slot) = st.devices.get_mut(&device) {
                            if slot.generation != generation {
                                st.record(
                                    "device_status_unregistered_generation",
                                    json!({"device":device}),
                                );
                                return;
                            }
                            slot.online = online;
                        } else {
                            st.record(
                                "device_status_unknown_device",
                                json!({ "device": device, "reason": reason }),
                            );
                            return;
                        }
                    }
                    Some(desc) => {
                        st.record(
                            "device_status_mismatched_descriptor",
                            json!({ "device": device, "descriptor": desc.stable_id }),
                        );
                        return;
                    }
                }
                // Existing descriptor-backed hot join is registration on this
                // connection. Diagnostics attach only after it was accepted.
                if let Some(slot) = st.devices.get_mut(&device) {
                    slot.northbound =
                        Some(northbound::NorthboundRecord::new(northbound, generation));
                }
                st.record(
                    "device_status",
                    json!({ "device": device, "online": online, "reason": reason }),
                );
                self.emit_control(DeviceEvent::DeviceStatus {
                    device: device.clone(),
                    online,
                    reason,
                    descriptor: st.devices.get(&device).map(|d| d.descriptor.clone()),
                });
            }
            // D/T4: device-side keyword spotter. Detections are facts —
            // whether a playback stops is the pipeline's call (voice.rs),
            // so the gateway can never act on its own.
            GatewayFrame::Keyword {
                device,
                ts,
                seq,
                keyword,
                fed_ms,
                self_heard,
                rms_dbfs,
                lag_ms,
                origin,
                bundle_id,
                kws_generation,
                kws_tier,
                wake_dur_ms,
                max_gap_ms,
                l2_passed,
            } => {
                let provenance = InputProvenance {
                    origin,
                    transport: Some(transport),
                };
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "keyword" }),
                    );
                    return;
                }
                if let Some(kw_gen) = kws_generation {
                    let active_kws_gen = st
                        .devices
                        .get(&device)
                        .and_then(|d| d.config_blocks.get(&ConfigBlock::Kws))
                        .filter(|b| b.state == ConfigReceiptStatus::Active)
                        // Config transactions and KWS bundles have independent
                        // counters. Only compare the bundle counter reported
                        // by the gateway with Keyword.kws_generation.
                        .and_then(|b| b.active_values.as_ref())
                        .and_then(|v| v.get("kws_generation"))
                        .and_then(serde_json::Value::as_u64)
                        .filter(|generation| *generation > 0);
                    if let Some(active_gen) = active_kws_gen {
                        if kw_gen < active_gen {
                            st.record(
                                "stale_kws_keyword",
                                json!({
                                    "device": device,
                                    "keyword": keyword,
                                    "kws_generation": kw_gen,
                                    "active_kws_generation": active_gen,
                                    "bundle_id": bundle_id,
                                }),
                            );
                            return;
                        }
                    }
                }
                // seq is informational here (no ordering guarantees are
                // needed for a reflex event), but it IS recorded so the
                // console can line a detection up with the uplink stream.
                st.record(
                    "keyword",
                    json!({ "device": device, "keyword": keyword, "seq": seq, "fed_ms": fed_ms,
                            "self_heard": self_heard, "rms_dbfs": rms_dbfs, "lag_ms": lag_ms,
                            "bundle_id": bundle_id, "kws_generation": kws_generation,
                            "kws_tier": kws_tier, "wake_dur_ms": wake_dur_ms,
                            "max_gap_ms": max_gap_ms, "l2_passed": l2_passed,
                            "provenance": provenance }),
                );
                self.emit_control(DeviceEvent::Keyword {
                    device,
                    ts,
                    keyword,
                    fed_ms,
                    self_heard,
                    rms_dbfs,
                    lag_ms,
                    kws_tier,
                    wake_dur_ms,
                    max_gap_ms,
                    l2_passed,
                    provenance,
                });
            }
            GatewayFrame::KwsReport {
                device,
                state,
                detail,
                restarts,
                detections,
                guarded,
                epoch,
                loaded_hash,
                loaded_ms,
                local_exit,
                threshold,
                score,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "kws_report" }),
                    );
                    return;
                }
                st.record(
                    "kws_report",
                    json!({ "device": device, "state": state, "detail": detail,
                            "restarts": restarts, "detections": detections, "guarded": guarded,
                            "epoch": epoch,
                            // M6b: the spotter's own load proof (empty = not
                            // reported yet), never the installed file hash.
                            "loaded_hash": loaded_hash, "loaded_ms": loaded_ms,
                            // #5: armed local-exit words (a device fact).
                            "local_exit": local_exit,
                            // #4: the applied sensitivity (model's own line).
                            "threshold": threshold, "score": score }),
                );
                self.emit_control(DeviceEvent::KwsReport {
                    device,
                    state,
                    detail,
                    restarts,
                    detections,
                    guarded,
                    epoch,
                    loaded_hash,
                    loaded_ms,
                    local_exit,
                    threshold,
                    score,
                });
            }
            GatewayFrame::KwsDeployReceipt {
                device,
                accepted,
                hash,
                keywords,
                detail,
                local_exit,
                intents_hash,
                threshold,
                score,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "kws_deploy_receipt" }),
                    );
                    return;
                }
                st.record(
                    "kws_deploy_receipt",
                    json!({ "device": device, "accepted": accepted, "hash": hash,
                            "keywords": keywords, "detail": detail,
                            "local_exit": local_exit, "intents_hash": intents_hash,
                            "threshold": threshold, "score": score }),
                );
                self.emit_control(DeviceEvent::KwsDeployReceipt {
                    device,
                    accepted,
                    hash,
                    keywords,
                    detail,
                    local_exit,
                    intents_hash,
                    threshold,
                    score,
                });
            }
            // Contract v2 §2.5 (#5): the device already cut its own audio on
            // a retreat word. Recorded with the device's own numbers; the
            // session is closed by the Keyword frame, never by this fact.
            GatewayFrame::LocalExitFlushed {
                device,
                keyword,
                ts,
                latency_ms,
                channels,
                detail,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "local_exit_flushed" }),
                    );
                    return;
                }
                st.record(
                    "local_exit_flushed",
                    json!({ "device": device, "keyword": keyword, "ts": ts,
                            "latency_ms": latency_ms, "channels": channels,
                            "detail": detail }),
                );
                self.emit_control(DeviceEvent::LocalExitFlushed {
                    device,
                    keyword,
                    ts,
                    latency_ms,
                    channels,
                    detail,
                });
            }
            // R1d: the device already stopped its own voice channel. Record
            // it with the device's own numbers and let the pipeline close its
            // bookkeeping; the semantic decision still rides the BargeIn fact.
            GatewayFrame::StoppedLocally {
                device,
                task_id,
                reason,
                played_ms,
                latency_ms,
                ahead_ms,
                ts,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "stopped_locally" }),
                    );
                    return;
                }
                st.record(
                    "stopped_locally",
                    json!({ "device": device, "task_id": task_id, "reason": reason,
                            "played_ms": played_ms, "latency_ms": latency_ms,
                            "ahead_ms": ahead_ms, "ts": ts }),
                );
                self.emit_control(DeviceEvent::StoppedLocally {
                    device,
                    task_id,
                    reason,
                    played_ms,
                    latency_ms,
                    ahead_ms,
                    ts,
                });
            }
            // D/T5: near-end speech during voice playback. The frame decodes
            // the same way AudioChunk does (bad payloads are recorded, never
            // crash the hub); the fact then flows to the pipeline — this
            // branch takes no other hub state beyond the shared record.
            GatewayFrame::BargeIn {
                device,
                ts,
                seq,
                pcm_b64,
                sample_rate,
                channels,
                corr,
                energy_db,
                ref_db,
                residual_db,
                ref_locked,
            } => {
                if st.device_frame_stale(generation, &device) {
                    st.record(
                        "stale_event",
                        json!({ "device": device, "kind": "barge_in" }),
                    );
                    return;
                }
                st.record(
                    "barge_in",
                    json!({ "device": device, "ts": ts, "seq": seq, "corr": corr,
                            // M6b: the device's `f32::MIN` "no measurement"
                            // sentinel is recorded as MISSING, not as a number.
                            "energy_db": valid_db(energy_db), "ref_db": valid_db(ref_db),
                            "residual_db": valid_db(residual_db), "ref_locked": ref_locked,
                            "samples": pcm_b64.len() / 4 * 3 / 2,
                            "sample_rate": sample_rate, "channels": channels }),
                );
                use base64::Engine;
                let raw = match base64::engine::general_purpose::STANDARD.decode(&pcm_b64) {
                    Ok(r) => r,
                    Err(e) => {
                        st.record(
                            "barge_in_bad_b64",
                            json!({ "device": device, "error": e.to_string() }),
                        );
                        return;
                    }
                };
                if raw.len() % 2 != 0 || channels != 1 {
                    st.record(
                        "barge_in_bad_payload",
                        json!({ "device": device, "len": raw.len(), "channels": channels }),
                    );
                    return;
                }
                let pcm = raw
                    .chunks_exact(2)
                    .map(|b| i16::from_le_bytes([b[0], b[1]]))
                    .collect();
                // R1d attribution: stamped HERE, at decode, so the pipeline
                // can separate "the wire was slow" from "this pump was busy".
                let rx_ms = crate::id::now_ms();
                self.emit_control(DeviceEvent::BargeIn {
                    device,
                    ts,
                    rx_ms,
                    pcm,
                    sample_rate,
                    corr,
                    energy_db,
                    ref_db,
                    residual_db,
                    ref_locked,
                });
            }
            GatewayFrame::EchoHealth {
                device,
                health,
                degraded_ms,
                reason,
            } => {
                if st.device_frame_stale(generation, &device) {
                    return;
                }
                let health_norm = if health.trim().is_empty() {
                    "unknown".to_string()
                } else {
                    health.trim().to_string()
                };
                let changed = if let Some(slot) = st.devices.get_mut(&device) {
                    let prev = slot.echo_health.as_deref();
                    let c = prev != Some(health_norm.as_str());
                    slot.echo_health = Some(health_norm.clone());
                    slot.echo_degraded_ms = Some(degraded_ms);
                    slot.echo_health_reason = reason.clone();
                    c
                } else {
                    true
                };
                if changed || health_norm != "healthy" {
                    st.record(
                        "echo_health",
                        json!({
                            "device": device,
                            "health": health_norm,
                            "degraded_ms": degraded_ms,
                            "reason": reason,
                        }),
                    );
                }
                self.emit_control(DeviceEvent::EchoHealth {
                    device,
                    health: health_norm,
                    degraded_ms,
                    reason,
                });
            }
            GatewayFrame::NearEndPresence {
                device,
                ts_ms,
                duration_ms,
                rms_dbfs,
                noise_floor_dbfs,
                snr_db,
                clipped,
                echo_health,
                profile_version,
            } => {
                if st.device_frame_stale(generation, &device) {
                    return;
                }
                let echo_health = echo_health.filter(|s| !s.trim().is_empty());
                let profile_version = profile_version.filter(|s| !s.trim().is_empty());
                if let Some(slot) = st.devices.get_mut(&device) {
                    if let Some(ref h) = echo_health {
                        slot.echo_health = Some(h.clone());
                    }
                    if let Some(ref pv) = profile_version {
                        slot.profile_version = Some(pv.clone());
                    }
                }
                self.emit_control(DeviceEvent::NearEndPresence {
                    device,
                    ts_ms,
                    duration_ms,
                    rms_dbfs: rms_dbfs.and_then(valid_db),
                    noise_floor_dbfs: noise_floor_dbfs.and_then(valid_db),
                    snr_db: snr_db.filter(|v| v.is_finite()),
                    clipped,
                    echo_health,
                    profile_version,
                });
            }
        }
    }
}

/// Event freshness: seq must be strictly greater than the last accepted.
/// Updates the watermark only for accepted events. Unknown devices are
/// rejected whenever any announced gateway connection exists; synthetic tests
/// that inject frames without a prior `Hello` keep the legacy fallback.
fn seq_fresh(st: &mut HubState, device: &str, seq: u64) -> bool {
    match st.devices.get_mut(device) {
        None => !st
            .connections
            .values()
            .any(|c| !c.devices.is_empty() || c.peer_addr.is_some()),
        Some(slot) => {
            if seq > slot.last_seq {
                slot.last_seq = seq;
                true
            } else {
                false
            }
        }
    }
}

/// Newline-delimited JSON reader with a hard byte cap BEFORE buffering a
/// full frame — an oversized or newline-less peer cannot grow memory.
struct CappedLineReader<R> {
    inner: R,
    buf: Vec<u8>,
    eof: bool,
}

impl<R: tokio::io::AsyncRead + Unpin> CappedLineReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(512),
            eof: false,
        }
    }

    /// Next line without the trailing newline; None on clean EOF. Errors
    /// when a single frame exceeds MAX_FRAME_BYTES.
    async fn next_line(
        &mut self,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
                while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            if self.eof {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                let line = String::from_utf8_lossy(&self.buf).into_owned();
                self.buf.clear();
                return Ok(Some(line));
            }
            if self.buf.len() > MAX_FRAME_BYTES {
                return Err(format!("frame exceeds {MAX_FRAME_BYTES} bytes").into());
            }
            let mut chunk = [0u8; 8192];
            let n = self.inner.read(&mut chunk).await?;
            if n == 0 {
                self.eof = true;
            } else {
                self.buf.extend_from_slice(&chunk[..n]);
            }
        }
    }
}

#[cfg(test)]
mod phase5_tests {
    use super::*;

    #[test]
    fn real_wav_playback_streams_play_begin_chunk_end_and_never_whole_wav_play() {
        let hub = DeviceHub::new();
        let (tx, mut rx) = outbound::outbound_channel();
        hub.attach_test_outbound(1, tx);

        // Build a valid 16kHz mono WAV with 20,000 bytes of PCM (>1 chunk).
        let pcm = vec![0x2Au8; 20_000];
        let mut wav = Vec::with_capacity(44 + pcm.len());
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&((36 + pcm.len()) as u32).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&16000u32.to_le_bytes());
        wav.extend_from_slice(&32000u32.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
        wav.extend_from_slice(&pcm);

        let _ticket = hub
            .play(PlayRequest {
                device: "dot2-d".into(),
                content_ref: "tts:test".into(),
                audio_meta: AudioMeta {
                    format: "wav".into(),
                    sample_rate: 16000,
                    channels: 1,
                },
                audio: Some(wav),
                channel: "voice".into(),
                text: Some("测试".into()),
                simulated_ms: None,
            })
            .unwrap();

        let mut frames = Vec::new();
        while let Ok(f) = rx.try_recv() {
            frames.push(f);
        }
        assert!(
            !frames.iter().any(|f| matches!(
                f,
                CoreFrame::Play {
                    audio_b64: Some(_),
                    ..
                }
            )),
            "whole-WAV CoreFrame::Play must never be emitted: {frames:?}"
        );
        assert!(matches!(frames.first(), Some(CoreFrame::PlayBegin { .. })));
        assert!(matches!(frames.last(), Some(CoreFrame::PlayEnd { .. })));
        let chunk_count = frames
            .iter()
            .filter(|f| matches!(f, CoreFrame::PlayChunk { .. }))
            .count();
        assert!(
            chunk_count >= 2,
            "expected multiple PlayChunk frames, got {chunk_count}"
        );
    }

    #[test]
    fn led_config_frame_serializes_and_sends_via_hub() {
        let hub = DeviceHub::new();
        let (tx, mut rx) = outbound::outbound_channel();
        hub.attach_test_outbound(1, tx);

        hub.send_led_config("dot2-d", true, 1320, 420, 25, 3, true)
            .unwrap();
        let frame = rx.try_recv().unwrap();
        match &frame {
            CoreFrame::LedConfig {
                device,
                night_enabled,
                night_start_minute,
                night_end_minute,
                night_brightness_pct,
                orientation_rotation,
                orientation_reverse,
            } => {
                assert_eq!(device, "dot2-d");
                assert!(*night_enabled);
                assert_eq!(*night_start_minute, 1320);
                assert_eq!(*night_end_minute, 420);
                assert_eq!(*night_brightness_pct, 25);
                assert_eq!(*orientation_rotation, 3);
                assert!(*orientation_reverse);
            }
            other => panic!("expected LedConfig, got {other:?}"),
        }
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(wire["type"], "led_config");
        assert_eq!(wire["device"], "dot2-d");
        assert_eq!(wire["night_brightness_pct"], 25);
        assert_eq!(wire["orientation_rotation"], 3);
    }
}
