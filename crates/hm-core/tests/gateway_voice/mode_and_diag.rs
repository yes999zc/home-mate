use super::*;

/// G6 replaced-capability #2: right after hello_ack the core projects its
/// clock (emOS has no time service; its clock sits in 2010 — TLS breaks
/// there in stage C). The frame must arrive on every connection.
#[tokio::test]
async fn time_set_projects_host_clock_after_hello() {
    let (base, _state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    let client = reqwest::Client::new();

    // The gateway is already connected by the harness; a fresh connect
    // (generation bump) must produce hello_ack followed by time_set with a
    // sane wall-clock value.
    let frame = gw.until(|f| matches!(f, CoreFrame::TimeSet { .. })).await;
    match frame {
        CoreFrame::TimeSet { ts_ms } => {
            let now = hm_core::id::now_ms();
            assert!(
                ts_ms.abs_diff(now) < 120_000,
                "time_set {ts_ms} vs now {now} — stale or bogus clock projection"
            );
        }
        _ => unreachable!(),
    }

    // Passive snapshot unaffected; the projection is not a device action.
    let d: Value = client
        .get(format!("{base}/api/v0/diagnostics"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(d["gateway"]["connected"], true);
}

/// The fixed-asset play probe (G6 ERLE stimulus path): the EXACT archived
/// WAV bytes ride the Play frame, an optional volume set precedes it, the
/// receipt stage is honest, and path tricks are rejected without any read.
#[tokio::test]
async fn diagnostics_play_asset_fixed_wav_wire_and_guards() {
    let (base, state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    let client = reqwest::Client::new();

    // A tiny canonical WAV (44-byte header + 50ms of 16k mono silence).
    let assets = state.data_dir.join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let mut wav: Vec<u8> = Vec::new();
    wav.extend_from_slice(b"RIFF");
    let data: Vec<u8> = vec![0u8; 1600];
    wav.extend_from_slice(&((36 + data.len()) as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&1u16.to_le_bytes()); // mono
    wav.extend_from_slice(&16000u32.to_le_bytes());
    wav.extend_from_slice(&32000u32.to_le_bytes()); // byte rate
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data.len() as u32).to_le_bytes());
    wav.extend_from_slice(&data);
    let asset = assets.join("erle-stimulus.wav");
    std::fs::write(&asset, &wav).unwrap();

    let probe_post = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/v0/diagnostics/tests/play_asset"))
                .json(&json!({ "asset": "erle-stimulus.wav", "volume": 0.79 }))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    // Volume first, then the play carrying the archive bytes verbatim.
    // Manual loop (not until) so a missing frame surfaces the probe's own
    // response instead of a bare timeout.
    let mut saw_volume = false;
    let mut play_frame = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut streamed_id: Option<String> = None;
    let mut streamed_pcm: Vec<u8> = Vec::new();
    while (!saw_volume || play_frame.is_none()) && tokio::time::Instant::now() < deadline {
        let f = tokio::time::timeout_at(deadline, gw.recv()).await;
        let Ok(f) = f else { break };
        match f {
            CoreFrame::VolumeSet { value, .. } => {
                assert!((value - 0.79).abs() < 1e-6, "{value}");
                saw_volume = true;
            }
            CoreFrame::PlayBegin {
                task_id,
                text,
                channel,
                ..
            } => {
                assert_eq!(text, None, "asset plays carry no text");
                assert_eq!(channel.as_deref(), Some("voice"));
                streamed_id = Some(task_id);
            }
            CoreFrame::PlayChunk {
                task_id, audio_b64, ..
            } => {
                if streamed_id.as_deref() == Some(&task_id) {
                    use base64::Engine;
                    let chunk = base64::engine::general_purpose::STANDARD
                        .decode(audio_b64)
                        .unwrap();
                    streamed_pcm.extend_from_slice(&chunk);
                }
            }
            CoreFrame::PlayEnd { task_id } => {
                if streamed_id.as_deref() == Some(&task_id) {
                    assert_eq!(
                        streamed_pcm,
                        &wav[44..],
                        "the wire carries the archived PCM verbatim"
                    );
                    gw.confirm_play(&task_id).await;
                    play_frame = Some(task_id);
                }
            }
            CoreFrame::Play {
                audio_b64: Some(_), ..
            } => {
                panic!("real WAV asset must stream via PlayBegin/PlayChunk/PlayEnd, not whole-WAV Play");
            }
            _ => {}
        }
    }
    let resp = probe_post.await.unwrap();
    assert!(
        saw_volume,
        "VolumeSet never hit the wire; probe said: {resp:?}"
    );
    assert!(
        play_frame.is_some(),
        "Play never hit the wire; probe said: {resp:?}"
    );
    assert_eq!(resp["ok"], true, "{resp:?}");
    assert_eq!(resp["stage"], "device_confirmed");
    assert_eq!(resp["asset"], "erle-stimulus.wav");

    // Audit trail.
    let log: Value = client
        .get(format!("{base}/api/v0/voice/log"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        log["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "diag_play_asset"),
        "the asset play must be recorded"
    );

    // Guards: traversal and missing assets fail without touching the wire.
    let guard = client
        .post(format!("{base}/api/v0/diagnostics/tests/play_asset"))
        .json(&json!({ "asset": "../Cargo.toml" }))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(guard["ok"], false, "{guard:?}");
    let missing = client
        .post(format!("{base}/api/v0/diagnostics/tests/play_asset"))
        .json(&json!({ "asset": "nope.wav" }))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(missing["ok"], false, "{missing:?}");

    std::fs::remove_file(&asset).ok();
}

/// The active voice test fails HONESTLY when no device is connected (the
/// text-only configuration stays usable; nothing is faked).
#[tokio::test]
async fn voice_play_test_without_device_is_honest() {
    let state = AppState::with_overrides(
        Arc::new(hm_core::store::Store::open_in_memory().unwrap()),
        PathBuf::from("./data-test"),
        MockInject::default(),
        Some(Arc::new(FakeLlm::new(vec![]))),
        Some(Arc::new(MockTts::default())),
        None,
        None,
    );
    enable_follow_window(&state);
    let app = hm_core::api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    let r: Value = reqwest::Client::new()
        .post(format!("{base}/api/v0/diagnostics/tests/voice_play"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["ok"], false, "{r:?}");
    assert!(
        r["error"].as_str().unwrap().contains("audio_out"),
        "honest reason, not a fake stage: {r:?}"
    );
}
/// Config restore (M3) over a CONNECTED gateway: restoring a voice-settings
/// history row re-validates and re-projects the request (ModeSet on the
/// wire) — and replays ZERO device actions: no Play, no Stop, no Volume,
/// even though plays happened around the original change.
#[tokio::test]
async fn config_restore_replays_config_only_not_device_actions() {
    let llm = FakeLlm::new(vec![
        Scripted::Calls {
            calls: vec![("music.search".into(), r#"{"query":"晚风"}"#.into())],
        },
        Scripted::Calls {
            calls: vec![(
                "music.play".into(),
                r#"{"source":"library","id":"t01"}"#.into(),
            )],
        },
        Scripted::Text {
            chunks: vec!["在放了。".into()],
        },
    ]);
    let (base, _state, gw, _music) = spawn_app_with_gateway_media(llm).await;
    let client = reqwest::Client::new();

    // A real task action happens (media play + spoken answer), both confirmed.
    explicit_request(&_state, &gw, 1, "放首歌").await;
    let (_media_id, _) = gw.until_media_play().await;
    let (voice_id, _) = gw.until_any_voice_play().await;
    gw.confirm_play(&voice_id).await;

    // Two mode changes: full/proactive-on (row A), then half/proactive-off
    // (row B). The device honestly reports half capability throughout.
    for body in [
        json!({ "duplex_requested": "full", "proactive_speech": true }),
        json!({ "duplex_requested": "half", "proactive_speech": false }),
    ] {
        let _: Value = client
            .put(format!("{base}/api/v0/mode/dot2-d"))
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }

    // Quiet the wire before restoring, so every frame below is caused by
    // the restore itself.
    let mut quiet = 0;
    while quiet < 3 {
        match tokio::time::timeout(Duration::from_millis(200), gw.recv()).await {
            Ok(_) => quiet = 0,
            Err(_) => quiet += 1,
        }
    }

    // Find row A (the full/proactive snapshot) in the config history.
    let hist: Value = client
        .get(format!("{base}/api/v0/config/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row_a = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["scope"] == "voice"
                && r["payload"]["duplex_requested"].get("dot2-d") == Some(&json!("full"))
        })
        .expect("voice history row with the full-duplex snapshot")
        .clone();
    let row_id = row_a["id"].as_i64().unwrap();

    // Restore it.
    let restore: Value = client
        .post(format!("{base}/api/v0/config/history/{row_id}/restore"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restore["state"], "applied", "{restore:?}");

    // Mode snapshot reflects the restored request and proactive flag.
    let mode: Value = client
        .get(format!("{base}/api/v0/mode"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let d = &mode["devices"][0];
    assert_eq!(d["duplex"]["requested"], "full");
    assert_eq!(d["proactive_speech"], true);

    // The wire: ONLY ModeSet frames. No Play/Stop/Volume may be replayed.
    let frames = collect_frames_until(&gw, || async { true }).await;
    assert!(
        frames
            .iter()
            .all(|f| matches!(f, CoreFrame::ModeSet { .. })),
        "restore must replay config projection only, saw: {frames:?}"
    );
    assert!(
        frames.iter().any(|f| matches!(f, CoreFrame::ModeSet { duplex_requested, .. } if duplex_requested == "full")),
        "the restored request must be re-projected: {frames:?}"
    );

    // And the restore is in the history with its own actor.
    let hist: Value = client
        .get(format!("{base}/api/v0/config/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hist["history"][0]["actor"], "restore");
    assert_eq!(hist["history"][0]["scope"], "voice");
}

/// The wire must carry the semantic presentation state at each transition:
/// idle at join, wake flash on the pinyin hit, thinking when the turn owns
/// the device, speaking while the answer plays, listening after the answer
/// finished inside the conversation window (wake flash already expired).
#[tokio::test]
async fn assistant_state_frames_track_interaction() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["在呢。".into()],
    }]);
    // ASR transcript "你好高达" = wake ONLY (strip leaves nothing) — the flash
    // must survive alone before the follow-up turn starts. The follow-up then
    // says a real command: a wake-only transcript inside the window is now
    // IGNORED (R1h fix, user report 2026-11-12), so it cannot be the turn.
    let (asr, reply) = SwitchableAsr::new("你好高达");
    let (base, state, gw) = spawn_mic_app_asr(llm, std::sync::Arc::new(asr)).await;

    // Join: the forced push announces the current (idle) state.
    match gw
        .until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "idle"))
        .await
    {
        CoreFrame::AssistantState { device, state } => {
            assert_eq!(device, "dot2-d");
            assert_eq!(state, "idle");
        }
        f => panic!("expected idle assistant_state, got {f:?}"),
    }

    // Wake-only utterance ("你好，高达" → rest empty): wake flash first.
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    match gw
        .until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "wake"))
        .await
    {
        CoreFrame::AssistantState { device, .. } => assert_eq!(device, "dot2-d"),
        f => panic!("expected wake assistant_state, got {f:?}"),
    }

    // The flash must retire into listening on its own (ticker, no event).
    match gw
        .until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "listening"))
        .await
    {
        CoreFrame::AssistantState { state, .. } => assert_eq!(state, "listening"),
        f => panic!("expected listening after flash expiry, got {f:?}"),
    }

    // A wake-only transcript inside the open window is NOT a command (R1h
    // fix: KWS opens the window before ASR delivers the same breath; running
    // a greeting turn there closed the mic gate and swallowed the question).
    for seq in 21..=30 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 31..=40 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let log = state.voice.recent(100);
    assert!(
        log.iter().any(|e| e["kind"] == "mic_wake_echo_in_window"),
        "the wake echo must be named, not answered: {log:?}"
    );

    // A real command inside the window becomes a turn: thinking then speaking.
    *reply.lock().unwrap() = "讲个故事".to_string();
    for seq in 41..=50 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 51..=60 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    gw.until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "thinking"))
        .await;
    let (play_id, _) = gw.until_play("voice").await;
    gw.until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "speaking"))
        .await;

    // Answer finished inside the conversation window: back to listening —
    // but only after the (already expired) wake flash can no longer win.
    tokio::time::sleep(Duration::from_millis(1600)).await; // flash from the earlier wake is long gone
    gw.send(&GatewayFrame::PlayFinished {
        device: "dot2-d".into(),
        task_id: play_id,
        played_ms: 900,
        ts: hm_core::id::now_ms(),

        consumed_ms: None,
        progress_basis: None,
    })
    .await;
    gw.until(|f| matches!(f, CoreFrame::AssistantState { state, .. } if state == "listening"))
        .await;

    // Snapshot shows the last SENT state (console debt: page-visible).
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dev = &mode["devices"][0];
    assert_eq!(dev["assistant_state"].as_str(), Some("listening"));
    assert_eq!(
        dev["mic"]["session"],
        Value::Null,
        "no mic_report sent yet in this test"
    );
}

/// Wake words are view/edit-able through the mode API (G3 console debt):
/// validation rejects garbage, writes persist, history records the snapshot,
/// and restore brings the previous list back.
#[tokio::test]
async fn wake_words_editable_via_mode_api() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["好".into()],
    }]);
    let (base, _state, _gw) = spawn_app_with_gateway(llm).await;
    let client = reqwest::Client::new();

    // Default is visible (user decision 2026-11-13: 你好高达 everywhere).
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["wake_words"], json!(["你好高达"]));

    // Invalid: empty list, punctuation-only word, overlong.
    for bad in [
        json!([]),
        json!(["？？？"]),
        json!(["一二三四五六七八九十一二三四五六七"]),
    ] {
        let r = client
            .put(format!("{base}/api/v0/mode/dot2-d"))
            .json(&json!({ "wake_words": bad }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "wake_words {bad} must be rejected");
    }

    // Valid write replaces the list.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "wake_words": ["小达", "你好高达"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["wake_words"], json!(["小达", "你好高达"]));

    // History carries the full snapshot (restorable), scope voice.
    let hist: Value = reqwest::get(format!("{base}/api/v0/config/history"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entry = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e["scope"] == json!("voice")
                && e["payload"]["device_overrides"]["dot2-d"]["wake_words"]
                    == json!(["小达", "你好高达"])
        })
        .expect("voice history row with the new wake words");
    let id = entry["id"].as_i64().unwrap();

    // Overwrite, then restore: the restored snapshot wins.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "wake_words": ["备选"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = client
        .post(format!("{base}/api/v0/config/history/{id}/restore"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        mode["wake_words"],
        json!(["小达", "你好高达"]),
        "restore must reapply the snapshot wake words"
    );
}

/// VAD thresholds are editable through the same chain and apply to RUNNING
/// processors (G4 calibration): a block level that opened speech under the
/// default threshold must stop opening after a calibrated raise.
#[tokio::test]
async fn vad_thresholds_apply_live() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["好".into()],
    }]);
    let (base, _state, gw) = spawn_mic_app(llm, "你好，高达").await;
    let client = reqwest::Client::new();

    // ~-33 dB blocks: loud under the default start_db=-37.
    let mid = block_at_db(-33.0);
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &mid)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dev = &mode["devices"][0];
    assert!(
        dev["mic"]["counters"]["speech_started"]
            .as_u64()
            .unwrap_or(0)
            >= 1,
        "mid-level speech must open under defaults: {dev:?}"
    );
    assert!(
        dev["mic"]["levels"].is_object(),
        "level telemetry must be visible: {dev:?}"
    );

    // Invalid thresholds are rejected.
    for bad in [
        json!({ "start_db": -35, "end_db": -30 }),
        json!({ "start_db": 5, "end_db": -30 }),
    ] {
        let r = client
            .put(format!("{base}/api/v0/mode/dot2-d"))
            .json(&json!({ "vad": bad }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "vad {bad} must be rejected");
    }

    // Raise the start threshold above the block level: live processors must
    // adopt it without a restart — no new speech_started.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "vad": { "start_db": -30.0, "end_db": -34.0 } }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let before: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let before = before["devices"][0]["mic"]["counters"]["speech_started"]
        .as_u64()
        .unwrap();
    for seq in 21..=30 {
        gw.send(&audio_chunk(seq, &mid)).await;
    }
    for seq in 31..=40 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let after: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dev = &after["devices"][0];
    assert_eq!(
        dev["mic"]["counters"]["speech_started"].as_u64().unwrap(),
        before,
        "calibrated threshold must suppress the same level: {dev:?}"
    );
    assert_eq!(dev["mic"]["vad"]["start_db"].as_f64(), Some(-30.0));
    assert_eq!(
        after["vad"]["start_db"].as_f64(),
        Some(-30.0),
        "persisted settings echo back"
    );
}

/// tail_blind_ms (T8 echo governance) rides the same mode API chain as the
/// other voice settings: visible in the snapshot, range-checked, persisted
/// into restorable history, and a restore reapplies the snapshot value.
#[tokio::test]
async fn tail_blind_ms_editable_via_mode_api() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["好".into()],
    }]);
    let (base, _state, _gw) = spawn_app_with_gateway(llm).await;
    let client = reqwest::Client::new();

    // Default (echo fix v2) is visible.
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["tail_blind_ms"], json!(400));

    // Out of range is rejected (0 disables, 2000 max — same clamp the
    // playback path applies).
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "tail_blind_ms": 4000 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "tail_blind_ms > 2000 must be rejected");

    // Valid write persists (T8 acceptance runs this at 0 to retire the
    // window once the device AEC holds the line).
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "tail_blind_ms": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["tail_blind_ms"], json!(0));

    // History carries the snapshot (restorable).
    let hist: Value = reqwest::get(format!("{base}/api/v0/config/history"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entry = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e["scope"] == json!("voice")
                && e["payload"]["device_overrides"]["dot2-d"]["tail_blind_ms"] == json!(0)
        })
        .expect("voice history row with tail_blind_ms=0");
    let id = entry["id"].as_i64().unwrap();

    // Overwrite, then restore: the snapshot value wins.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "tail_blind_ms": 200 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = client
        .post(format!("{base}/api/v0/config/history/{id}/restore"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        mode["tail_blind_ms"],
        json!(0),
        "restore must reapply the snapshot tail_blind_ms"
    );
}

/// The device's actual failure (2026-11-13): a room floor ABOVE the configured
/// offset threshold (−44 dB room vs the default −42). The old VAD never saw a
/// quiet block, so a 2 s utterance ran to the 150-block cap (11.68 s) and two
/// utterances merged. The endpoint must follow the room, name the close
/// reason, and expose both threshold sets plus the floor in the console
/// snapshot — "why did my sentence take 12 s" must be answerable from /mode.
#[tokio::test]
async fn a_noisy_room_closes_the_segment_and_names_the_reason() {
    let llm = FakeLlm::new(vec![]);
    let (base, state, gw) = spawn_mic_app(llm, "你好，高达").await;

    let room = tone_block(-44.0);
    let speech = tone_block(-20.0);
    let mut seq = 0u64;
    for _ in 0..25 {
        seq += 1;
        gw.send(&audio_chunk(seq, &room)).await;
    }
    for _ in 0..25 {
        seq += 1;
        gw.send(&audio_chunk(seq, &speech)).await;
    }
    for _ in 0..25 {
        seq += 1;
        gw.send(&audio_chunk(seq, &room)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    let log = state.voice.recent(200);
    let closed = log
        .iter()
        .find(|e| e["kind"] == "mic_segment_closed")
        .unwrap_or_else(|| panic!("the segment must close on the room, not the cap: {log:?}"));
    assert_eq!(
        closed["close_reason"],
        json!("quiet"),
        "a floor-relative close is a sentence end: {closed:?}"
    );
    let duration = closed["duration_ms"].as_u64().unwrap_or(0);
    assert!(
        (1_500..=3_500).contains(&duration),
        "a ~2 s utterance closed at {duration} ms — the room must not stretch it to the 9.6 s cap"
    );
    assert!(
        closed["end_db"].as_f64().unwrap_or(-99.0) > -42.0,
        "the offset threshold must follow the room up: {closed:?}"
    );

    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mic = &mode["devices"][0]["mic"];
    let floor = mic["vad"]["noise_db"]
        .as_f64()
        .expect("the room floor must be visible");
    assert!(
        (floor + 44.0).abs() < 1.0,
        "floor {floor} is not the room the device heard"
    );
    assert_eq!(
        mic["vad"]["end_db"],
        json!(-42.0),
        "the configured ceiling is unchanged"
    );
    assert!(
        mic["vad"]["effective_end_db"].as_f64().unwrap_or(-99.0) > -42.0,
        "the effective threshold is the one in force: {mic:?}"
    );
    assert_eq!(mic["vad"]["last_close"]["reason"], json!("quiet"));
    assert!(mic["counters"]["close_quiet"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        mic["counters"]["close_cap"],
        json!(0),
        "no cap close on a 2 s utterance"
    );
}

/// Mic session telemetry (console debt): the achieved session state from
/// mic_report, flow liveness (chunks/s) and the wake counters are all
/// page-visible in the mode snapshot.
#[tokio::test]
async fn mic_telemetry_visible_in_mode_snapshot() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["好".into()],
    }]);
    let (base, _state, gw) = spawn_mic_app(llm, "你好，高达").await;

    // The gateway's achieved truth arrives as mic_report.
    gw.send(&GatewayFrame::MicReport {
        device: "dot2-d".into(),
        active: true,
        layout: Some("mono".into()),
        sample_rate: Some(16000),
        channels: Some(1),
        detail: None,
    })
    .await;
    let loud = loud_block();
    for seq in 1..=12 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dev = &mode["devices"][0];
    assert_eq!(dev["mic"]["session"], json!(true));
    assert!(
        dev["mic"]["chunks_per_sec"].as_f64().unwrap_or(0.0) > 0.0,
        "flow rate must be visible: {dev:?}"
    );
    assert!(dev["mic"]["last_chunk_ms"].is_u64());
    assert!(dev["mic"]["in_speech"].is_boolean());
    assert!(
        dev["mic"]["counters"]["speech_started"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
}

/// A gateway that re-introduces a device via device_status on a NEW
/// connection starts a fresh seq epoch (adapter restarts announce an empty
/// hello then introduce live). The old watermark must not drop the whole
/// new stream as stale (caught live on the on-device adapter, G5-1).
#[tokio::test]
async fn re_introduced_device_gets_fresh_seq_epoch() {
    let llm = FakeLlm::new(vec![]);
    let (base, state, gw) = spawn_mic_app(llm, "你好，高达").await;
    let port: u16 = state
        .hub
        .bind_addr
        .lock()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap()
        .port();
    let _ = base;

    // Push the watermark up on the first connection.
    let loud = loud_block();
    for seq in 100..=120 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Drop and reconnect: hello with NO devices, then device_status with a
    // descriptor (the on-device adapter restart shape).
    gw.close().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let gw2 = connect_gateway_audio_in(port).await;
    gw2.send(&GatewayFrame::DeviceStatus {
        northbound: None,
        device: "dot2-d".into(),
        online: true,
        reason: "registered".into(),
        descriptor: Some(HelloDevice {
            stable_id: "dot2-d".into(),
            model: "AEOBC".into(),
            firmware: None,
            capabilities: vec!["audio_in".into()],
            audio_profile: "fireos_builtin_speaker".into(),
            wifi_ssid: None,
        }),
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Low seqs from the new epoch MUST be accepted (speech_started fires).
    for seq in 1..=8 {
        gw2.send(&audio_chunk(seq, &loud)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dev = &mode["devices"][0];
    assert!(
        dev["mic"]["blocks_seen"].as_u64().unwrap_or(0) >= 8,
        "new-epoch frames must be accepted, not dropped as stale: {dev:?}"
    );
}

// --- local wake ack + retreat intent (user directive 2026-11-09) ------------------

/// A bare wake ("你好，高达" → greeting residue) must answer from the local
/// pre-synthesized cache: an ack play on the voice channel, NO LLM call
/// (the FakeLlm has no script — a call would surface as an error event).
#[tokio::test]
async fn bare_wake_answers_with_local_ack() {
    let llm = FakeLlm::new(vec![]);
    let (base, state, gw) = spawn_mic_app(llm, "你好，高达").await;
    let _ = base;

    // Enable the ack and pre-synthesize with the mock TTS.
    let _ = state.voice.set_mode(
        "dot2-d",
        None,
        None,
        None,
        None,
        None,
        Some(hm_core::voice::WakeAckSettings {
            enabled: true,
            phrases: vec!["在呢".into()],
            resynth: None,
        }),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(no_filler()),
        None,
        None,
        "test",
    );
    state.voice.synthesize_wake_acks(false).await;

    // Wake audio closes a segment → ASR "你好，高达" → greeting residue.
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }

    // The ack play arrives on the voice channel with the local asset ref.
    let ack = gw
        .until(|f| {
            matches!(
                f,
                CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. }
                    if content_ref.starts_with("ack:")
            )
        })
        .await;
    match ack {
        CoreFrame::PlayBegin {
            content_ref, text, ..
        } => {
            assert!(content_ref.starts_with("ack:在呢"), "got {content_ref}");
            assert!(text.is_none(), "no LLM text on the ack path");
        }
        CoreFrame::Play {
            content_ref,
            audio_b64,
            text,
            ..
        } => {
            assert!(content_ref.starts_with("ack:在呢"), "got {content_ref}");
            assert!(audio_b64.is_some(), "local WAV must be inline");
            assert!(text.is_none(), "no LLM text on the ack path");
        }
        f => panic!("expected ack play, got {f:?}"),
    }
    // R2-BM5/BM5-6: the ack is its own output class.
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let outs = mode["devices"][0]["outputs"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        outs.iter().any(|o| o["kind"] == "ack"),
        "the ack must be kind=ack: {outs:?}"
    );

    // The wake still opened the follow window (the user may now command).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["devices"][0]["participation"], "conversation");
}

/// The retreat intent is EXECUTED, not chatted about: stop requests go out,
/// the follow window closes immediately (no LLM turn consumes the script).
#[tokio::test]
async fn retreat_intent_stops_and_closes_window() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["不该出现".into()],
    }]);
    let (base, state, gw) = spawn_mic_app(llm, "你好高达，退下吧").await;

    let _ = state.voice.set_mode(
        "dot2-d",
        None,
        None,
        None,
        None,
        None,
        Some(hm_core::voice::WakeAckSettings {
            enabled: true,
            phrases: vec!["在呢".into()],
            resynth: None,
        }),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(no_filler()),
        None,
        None,
        "test",
    );
    state.voice.synthesize_wake_acks(false).await;

    // Seed the conversation context: 退下 must clear it (2026-11-12: a wrong
    // self-description survived the close and the model imitated it).
    let session = state.sessions.device_session_id("dot2-d").unwrap();
    state.sessions.append(&session, "user", "讲个故事").unwrap();
    state
        .sessions
        .append(&session, "assistant", "从前……")
        .unwrap();

    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }

    // The retreat ack (fixed phrase) plays locally — NOT the scripted LLM
    // answer, which must never be consumed.
    let ack = gw
        .until(|f| {
            matches!(
                f,
                CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. }
                    if content_ref.starts_with("ack:")
            )
        })
        .await;
    match ack {
        CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. } => {
            assert!(
                content_ref.contains("好嘞"),
                "retreat ack expected, got {content_ref}"
            );
        }
        f => panic!("expected retreat ack, got {f:?}"),
    }

    // Window CLOSED right away (participation idle), and the session_retreat
    // fact is in the voice log with stop semantics recorded.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        mode["devices"][0]["participation"], "idle",
        "retreat closes the window"
    );
    let log: Value = reqwest::get(format!("{base}/api/v0/voice/log?limit=50"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let retreat = log["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "session_retreat")
        .unwrap_or_else(|| panic!("session_retreat event missing: {log:?}"));
    assert!(
        retreat["session_messages_cleared"].as_u64().unwrap_or(0) >= 2,
        "退下 must clear the conversation context: {retreat:?}"
    );
    let current_session = state.sessions.device_session_id("dot2-d").unwrap();
    assert_ne!(
        current_session, session,
        "退下 must rotate the active context so the next turn starts clean"
    );
    assert!(
        state
            .sessions
            .messages(&current_session)
            .unwrap()
            .is_empty(),
        "the model's next turn must start with no history"
    );
    assert!(
        state
            .sessions
            .history(&current_session, 50)
            .unwrap()
            .is_empty(),
        "the model's next turn must start with 0 carried-over turns"
    );
    assert_eq!(
        state.sessions.messages(&session).unwrap().len(),
        2,
        "archived conversation history must be preserved across retreat"
    );
    assert!(
        state
            .sessions
            .device_contexts("dot2-d", None, 10)
            .unwrap()
            .iter()
            .any(|c| c.session_id == session),
        "retreated session must appear in archived device_contexts"
    );
}

/// Echo fix v2 regression (2026-11-09 incident): the self-echo gate must
/// see DIRECT hub plays (the local ack), and after any play finishes the
/// tail blind window swallows reverb-tail segments; real speech after the
/// window flows again.
#[tokio::test]
async fn self_echo_gate_covers_direct_plays_and_tail() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["跟随回答。".into()],
    }]);
    let (asr, reply) = SwitchableAsr::new("你好，高达");
    let (base, state, gw) = spawn_mic_app_asr(llm, std::sync::Arc::new(asr)).await;
    let _ = state.voice.set_mode(
        "dot2-d",
        None,
        None,
        None,
        None,
        None,
        Some(hm_core::voice::WakeAckSettings {
            enabled: true,
            phrases: vec!["在呢".into()],
            resynth: None,
        }),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(no_filler()),
        None,
        None,
        "test",
    );
    state.voice.synthesize_wake_acks(false).await;

    // 1) Wake → ack play is submitted.
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    let play_id = match gw
        .until(|f| {
            matches!(
                f,
                CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. }
                    if content_ref.starts_with("ack:")
            )
        })
        .await
    {
        CoreFrame::Play { task_id, .. } | CoreFrame::PlayBegin { task_id, .. } => task_id,
        f => panic!("expected ack play, got {f:?}"),
    };

    *reply.lock().unwrap() = "在呢".into();
    // 2) Chunks DURING the live ack play: gate must drop them (no VAD open,
    //    no second play of any kind).
    for seq in 21..=30 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;

    // 3) Play finishes → blind window (400 ms default) swallows the tail.
    gw.send(&GatewayFrame::PlayFinished {
        device: "dot2-d".into(),
        task_id: play_id.clone(),
        played_ms: 500,
        ts: hm_core::id::now_ms(),

        consumed_ms: None,
        progress_basis: None,
    })
    .await;
    for seq in 31..=42 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 43..=50 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let log: Value = reqwest::get(format!("{base}/api/v0/voice/log?limit=50"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mid_play_plays = log["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == "mic_speech_started")
        .count();
    assert!(mid_play_plays >= 2, "playback control PCM must reach VAD");
    // D2b→48 amendment (2026-11-15, dot2a field): a segment whose only
    // playback overlap is the WAKE ACK is admitted as an ordinary in-window
    // request (the ack invites the question; see the ack_overlap_only fix).
    // Self-echo protection during a real ANSWER still marks control-only and
    // still forbids requests — asserted by the answer-overlap tests below.
    assert!(
        !log["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "mic_control_only_rejected"),
        "ack overlap must not control-gate the invited question"
    );

    // 4) After the blind window expires, the same speech flows again —
    //    inside the still-open follow window it becomes a real turn (the
    //    transcript is a command now: a wake-only one would be ignored).
    *reply.lock().unwrap() = "讲个故事".to_string();
    for seq in 51..=62 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 63..=70 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    let answer = gw.until_play("voice").await;
    assert!(
        answer.1["has_audio"].as_bool().unwrap(),
        "the scripted answer must play"
    );
}

// --- v0.3 streamed answer speech (D/T3) ---------------------------------------------

/// A multi-sentence answer streamed in chunks: the first speakable segment
/// opens ONE device stream, every segment's PCM rides contiguous seq
/// chunks, the stream closes at Answer — and no whole-WAV Play is sent.
#[tokio::test]
async fn streamed_answer_rides_one_stream_with_contiguous_seq() {
    let sentences = ["今天天气不错。", "适合出去走走。", "记得多喝水。"];
    let mut expected_pcm_bytes = 0usize;
    for sentence in sentences {
        let audio =
            hm_core::speech::TtsClient::synthesize(&hm_core::speech::MockTts::default(), sentence)
                .await
                .unwrap();
        expected_pcm_bytes += hm_core::speech::wav_pcm(&audio.wav).unwrap().2.len();
    }
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec![
            "今天天气不错。".into(),
            "适合出去走走。".into(),
            "记得多喝水。".into(),
        ],
    }]);
    let (base, state, gw) = spawn_mic_app_stream(llm, "你好高达，今天怎么样").await;
    let _ = base;

    let _ = gw
        .until(|f| matches!(f, CoreFrame::MicSession { action, .. } if action == "start"))
        .await;
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }

    // First sentence opens the stream (its text rides the begin frame).
    let begin = match tokio::time::timeout(
        Duration::from_secs(5),
        gw.until(|f| matches!(f, CoreFrame::PlayBegin { .. })),
    )
    .await
    {
        Ok(f) => f,
        Err(_) => {
            let log = state.voice.recent(100);
            panic!("no play_begin; voice log: {log:?}");
        }
    };
    match begin {
        CoreFrame::PlayBegin {
            task_id,
            device,
            audio,
            channel,
            text,
            ..
        } => {
            assert_eq!(device, "dot2-d");
            assert_eq!(channel.as_deref(), Some("voice"));
            assert_eq!(audio.format, "pcm_s16le");
            assert_eq!(audio.channels, 1);
            assert_eq!(text.as_deref(), Some("今天天气不错。"));
            // One stream for the whole answer: collect until its end.
            let mut expect_seq = 0u64;
            let mut received_pcm_bytes = 0usize;
            loop {
                match gw
                    .until(|f| match f {
                        CoreFrame::PlayChunk { task_id: t, .. } => t == &task_id,
                        CoreFrame::PlayEnd { task_id: t } => t == &task_id,
                        _ => false,
                    })
                    .await
                {
                    CoreFrame::PlayChunk {
                        task_id: t,
                        seq,
                        audio_b64,
                        ..
                    } => {
                        assert_eq!(t, task_id);
                        assert_eq!(seq, expect_seq, "seq must be contiguous from 0");
                        expect_seq += 1;
                        use base64::Engine;
                        let pcm = base64::engine::general_purpose::STANDARD
                            .decode(audio_b64)
                            .expect("chunk pcm b64");
                        assert!(!pcm.is_empty(), "chunks carry real audio");
                        assert!(pcm.len() <= 8192, "fallback PCM chunks stay bounded");
                        received_pcm_bytes += pcm.len();
                    }
                    CoreFrame::PlayEnd { task_id: t } => {
                        assert_eq!(t, task_id);
                        break;
                    }
                    f => panic!("unexpected frame {f:?}"),
                }
            }
            // Sentence boundaries need not be transport chunk boundaries.
            // Every sample from all three segments must arrive, in one stream.
            assert_eq!(received_pcm_bytes, expected_pcm_bytes);
        }
        f => panic!("expected play_begin, got {f:?}"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    let log = state.voice.recent(100);
    assert!(
        log.iter().any(|e| e["kind"] == "tts_streamed"),
        "tts_streamed must be logged: {log:?}"
    );
    assert!(
        !log.iter().any(|e| e["kind"] == "tts_played"),
        "the whole-path must not also speak: {log:?}"
    );
}

/// Without `play_stream` the streamed path is not taken at all: the
/// classic whole-WAV play answers (regression guard for the fallback).
#[tokio::test]
async fn without_play_stream_capability_whole_play_answers() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["一句话答案。".into(), "第二句。".into()],
    }]);
    let (base, state, gw) = spawn_mic_app(llm, "你好高达，说两句").await;
    let _ = base;

    let _ = gw
        .until(|f| matches!(f, CoreFrame::MicSession { action, .. } if action == "start"))
        .await;
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }

    let (play_id, meta) = gw.until_play("voice").await;
    assert_eq!(meta["text"].as_str(), Some("一句话答案。第二句。"));
    gw.confirm_play(&play_id).await;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let log = state.voice.recent(100);
    assert!(
        log.iter().any(|e| e["kind"] == "tts_played"),
        "whole-path must speak: {log:?}"
    );
    assert!(
        !log.iter().any(|e| e["kind"] == "tts_streamed"),
        "streamed path must stay off without the capability: {log:?}"
    );
}

/// Tool phases are not the answer: a streamed sentence already on the
/// wire is cut (Stop) when ToolStart fires, and the final answer opens a
/// fresh stream.
#[tokio::test]
async fn the_sentence_in_flight_finishes_before_the_tool_phase() {
    let llm = FakeLlm::new(vec![
        Scripted::TextThenCalls {
            chunks: vec!["让我看看榜单。".into()],
            calls: vec![("hotlist.query".into(), "{}".into())],
        },
        Scripted::Text {
            chunks: vec!["今天榜首是晚风信笺。".into()],
        },
    ]);
    let (base, state, gw) = spawn_mic_app_stream(llm, "你好高达，看看热榜").await;
    let _ = base;

    let _ = gw
        .until(|f| matches!(f, CoreFrame::MicSession { action, .. } if action == "start"))
        .await;
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }

    // The transition sentence opens a stream and is CLOSED (PlayEnd), not
    // stopped: the device drains the audio it already has, so the person
    // hears the whole sentence (R1h fix, user report 2026-11-12: a Stop here
    // cut it at 469 ms — 「我先为你查」 then silence).
    let first = gw.until(|f| matches!(f, CoreFrame::PlayBegin { .. })).await;
    let CoreFrame::PlayBegin {
        task_id: first_id, ..
    } = first
    else {
        unreachable!()
    };
    let _ = gw.until(|f| matches!(&f, CoreFrame::PlayChunk { task_id, seq, .. } if task_id == &first_id && seq == &0)).await;
    let close = gw
        .until(|f| {
            matches!(&f, CoreFrame::PlayEnd { task_id } if task_id == &first_id)
                || matches!(&f, CoreFrame::Stop { task_id, .. } if task_id == &first_id)
        })
        .await;
    assert!(
        matches!(close, CoreFrame::PlayEnd { .. }),
        "the sentence in flight must be drained, not cut: {close:?}"
    );

    // The final answer opens its OWN stream and completes it.
    let second = gw
        .until(|f| matches!(&f, CoreFrame::PlayBegin { task_id, .. } if task_id != &first_id))
        .await;
    let CoreFrame::PlayBegin {
        task_id: second_id,
        text,
        ..
    } = second
    else {
        unreachable!()
    };
    assert_eq!(text.as_deref(), Some("今天榜首是晚风信笺。"));
    let _ = gw
        .until(|f| matches!(&f, CoreFrame::PlayEnd { task_id } if task_id == &second_id))
        .await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    let log = state.voice.recent(100);
    assert!(
        log.iter()
            .any(|e| e["kind"] == "tts_stream_closed_for_tools"),
        "the close must be logged: {log:?}"
    );
    assert!(
        !log.iter().any(|e| e["kind"] == "tts_stream_cut_for_tools"),
        "no mid-word cut for the tool phase: {log:?}"
    );
}

/// The sentence splitter: hard boundaries first, soft valve at 60 chars,
/// nothing speakable until one of them lands.
#[test]
fn speak_segment_splitter() {
    let mut b = "今天天气".to_string();
    assert!(hm_core::voice::VoicePipeline::take_speak_segment(&mut b).is_none());
    b.push_str("不错。然后呢");
    assert_eq!(
        hm_core::voice::VoicePipeline::take_speak_segment(&mut b).unwrap(),
        "今天天气不错。"
    );
    assert_eq!(b, "然后呢");

    // Soft valve: 60+ chars of comma-spliced text with no hard boundary.
    let mut b = "这".to_string() + &"，一".repeat(40);
    let seg = hm_core::voice::VoicePipeline::take_speak_segment(&mut b).unwrap();
    assert!(
        !seg.is_empty() && seg.chars().count() <= 61,
        "valve length {seg:?}"
    );
    assert!(b.chars().count() > 0, "valve leaves the remainder buffered");

    // Whitespace-only segments are skipped honestly.
    let mut b = "  \n".to_string();
    assert!(hm_core::voice::VoicePipeline::take_speak_segment(&mut b).is_none());
}

/// Retreat words are console-editable (user report 2026-11-10: no place
/// to configure them): validation rejects garbage, writes persist into
/// restorable history, a CUSTOM word actually ends the session locally,
/// and a removed default no longer matches.
#[tokio::test]
async fn retreat_ack_editable_via_mode_api() {
    let llm = FakeLlm::new(vec![]);
    let (base, state, _gw) = spawn_mic_app(llm, "你好高达，退下").await;
    let client = reqwest::Client::new();

    // Default is visible.
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["retreat_ack"], json!("好嘞"));

    // Invalid: empty, overlong, quotes.
    for bad in [
        json!(""),
        json!("一二三四五六七八九"),
        json!("a\"b"),
        json!("a/b"),
    ] {
        let r = client
            .put(format!("{base}/api/v0/mode/dot2-d"))
            .json(&json!({ "retreat_ack": bad }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "retreat_ack {bad} must be rejected");
    }

    // Valid edit persists into the snapshot and the settings store.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "retreat_ack": "得令" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["retreat_ack"], json!("得令"));

    // Config history carries it (restore chain).
    let hist: Value = reqwest::get(format!("{base}/api/v0/config/history"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let last_voice = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|e| e["scope"] == "voice")
        .unwrap()
        .clone();
    assert!(last_voice["summary"]
        .as_str()
        .unwrap()
        .contains("retreat_ack"));
    let _ = state;
}

#[tokio::test]
async fn retreat_words_editable_and_functional() {
    let llm = FakeLlm::new(vec![Scripted::Text {
        chunks: vec!["不该出现".into()],
    }]);
    let (base, state, gw) = spawn_mic_app(llm, "你好高达，拜拜了您内").await;
    let client = reqwest::Client::new();

    // Default list is visible.
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(mode["retreat_words"]
        .as_array()
        .unwrap()
        .contains(&json!("退下")));

    // Invalid: empty, overlong entry, quotes.
    for bad in [
        json!([]),
        json!(["一二三四五六七八九十一"]),
        json!(["\"引号\""]),
    ] {
        let r = client
            .put(format!("{base}/api/v0/mode/dot2-d"))
            .json(&json!({ "retreat_words": bad }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "retreat_words {bad} must be rejected");
    }

    // Valid custom set replaces the family.
    let r = client
        .put(format!("{base}/api/v0/mode/dot2-d"))
        .json(&json!({ "retreat_words": ["拜拜了您内"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let mode: Value = reqwest::get(format!("{base}/api/v0/mode"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mode["retreat_words"], json!(["拜拜了您内"]));

    // Functional: one utterance "wake + custom word" ends the session
    // LOCALLY (retreat ack plays; the scripted LLM answer stays unused).
    let _ = gw
        .until(|f| matches!(f, CoreFrame::MicSession { action, .. } if action == "start"))
        .await;
    let loud = loud_block();
    let quiet = quiet_block();
    for seq in 1..=10 {
        gw.send(&audio_chunk(seq, &loud)).await;
    }
    for seq in 11..=20 {
        gw.send(&audio_chunk(seq, &quiet)).await;
    }
    let ack = gw
        .until(|f| {
            matches!(
                f,
                CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. }
                    if content_ref.starts_with("ack:")
            )
        })
        .await;
    assert!(
        matches!(
            &ack,
            CoreFrame::Play { content_ref, .. } | CoreFrame::PlayBegin { content_ref, .. }
                if content_ref.contains("好嘞")
        ),
        "retreat ack expected, got {ack:?}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let log = state.voice.recent(100);
    assert!(
        log.iter().any(|e| e["kind"] == "session_retreat"),
        "custom word must retreat: {log:?}"
    );

    // Restore the default via history; the custom word stops matching and
    // the default word matches again (function-level truth).
    let hist: Value = reqwest::get(format!("{base}/api/v0/config/history"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entry = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e["scope"] == json!("voice")
                && e["payload"]["retreat_words"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("退下"))
        })
        .expect("history row with the default retreat set");
    let id = entry["id"].as_i64().unwrap();
    let r = client
        .post(format!("{base}/api/v0/config/history/{id}/restore"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let defaults = hm_core::voice::default_retreat_words();
    assert!(!hm_core::voice::retreat_match("拜拜了您内", &defaults));
    assert!(hm_core::voice::retreat_match("退下", &defaults));
}

/// Task B (2026-11-10): the ch8 loopback diagnostic plays a known asset
/// through the real media path and compares the firmware's DAC-side track
/// with the ch8 track. Wire-level: the Play frame carries the probe asset;
/// result-level: the report names the lag and verdict instead of guessing.
#[tokio::test]
async fn diagnostics_playback_loopback_reports_lag_and_verdict() {
    let (base, state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    let client = reqwest::Client::new();

    // Canonical WAV in the assets dir (the endpoint refuses missing assets).
    let assets = state.data_dir.join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let mut wav: Vec<u8> = Vec::new();
    wav.extend_from_slice(b"RIFF");
    let data: Vec<u8> = vec![0u8; 3200];
    wav.extend_from_slice(&((36 + data.len()) as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&16000u32.to_le_bytes());
    wav.extend_from_slice(&32000u32.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data.len() as u32).to_le_bytes());
    wav.extend_from_slice(&data);
    std::fs::write(assets.join("loopback-probe.wav"), &wav).unwrap();

    // Pretend the firmware wrote both probe tracks: broadband DAC signal,
    // capture = 300-sample delayed copy (the real codec lag shape).
    let loop_dir = std::env::temp_dir().join(format!("hm-loop-test-{}", std::process::id()));
    std::fs::create_dir_all(&loop_dir).unwrap();
    std::env::set_var("HM_LOOP_DIR", &loop_dir);
    let mut lfsr: u32 = 0xACE1_2345;
    let write: Vec<i16> = (0..4096)
        .map(|_| {
            let bit = ((lfsr ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5)) & 1) as i16;
            lfsr >>= 1;
            bit * 12000 - 6000
        })
        .collect();
    let mut capture = vec![0i16; 300];
    capture.extend_from_slice(&write);
    let pcm_bytes = |v: &[i16]| -> Vec<u8> { v.iter().flat_map(|s| s.to_le_bytes()).collect() };
    std::fs::write(loop_dir.join("write.pcm"), pcm_bytes(&write)).unwrap();
    std::fs::write(loop_dir.join("capture.pcm"), pcm_bytes(&capture)).unwrap();

    let probe_post = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/v0/diagnostics/tests/playback_loopback"))
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    let mut play_frame = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while play_frame.is_none() && tokio::time::Instant::now() < deadline {
        let Ok(f) = tokio::time::timeout_at(deadline, gw.recv()).await else {
            break;
        };
        if let CoreFrame::Play {
            task_id, channel, ..
        }
        | CoreFrame::PlayBegin {
            task_id, channel, ..
        } = f
        {
            assert_eq!(
                channel.as_deref(),
                Some("media"),
                "loopback probe uses the media path"
            );
            gw.confirm_play(&task_id).await;
            play_frame = Some(task_id);
        }
    }
    let resp = probe_post.await.unwrap();
    assert!(
        play_frame.is_some(),
        "Play never hit the wire; probe said: {resp:?}"
    );
    assert_eq!(resp["ok"], true, "{resp:?}");
    let lb = &resp["loopback"];
    assert_eq!(lb["verdict"], "loopback_intact", "{resp:?}");
    assert_eq!(lb["lag_samples"], 300, "{resp:?}");
    assert!(lb["correlation"].as_f64().unwrap() > 0.99, "{resp:?}");
}

/// Media trim (2026-11-10): a media-channel play is scaled by the persisted
/// trim before it hits the wire; voice plays are never scaled; the mode API
/// validates the range and exposes the value in the snapshot.
#[tokio::test]
async fn media_trim_applies_to_media_only_and_validates() {
    let (base, state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    let client = reqwest::Client::new();
    let dev = state
        .hub
        .primary_audio_out()
        .expect("device announced")
        .stable_id;

    // Canonical WAV with a known constant amplitude.
    let assets = state.data_dir.join("assets");
    std::fs::create_dir_all(&assets).unwrap();
    let mut wav: Vec<u8> = Vec::new();
    let data: Vec<u8> = (0..800).flat_map(|_| 10000i16.to_le_bytes()).collect();
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&((36 + data.len()) as u32).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&16000u32.to_le_bytes());
    wav.extend_from_slice(&32000u32.to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(data.len() as u32).to_le_bytes());
    wav.extend_from_slice(&data);
    std::fs::write(assets.join("trim-probe.wav"), &wav).unwrap();

    // Range validation: 0 and -12 accepted, outside rejected.
    for bad in [
        json!({"media_trim_db": 3.0}),
        json!({"media_trim_db": -13.0}),
    ] {
        let r = client
            .put(format!("{base}/api/v0/mode/{dev}"))
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "must reject {bad}");
    }
    let r = client
        .put(format!("{base}/api/v0/mode/{dev}"))
        .json(&json!({"media_trim_db": -6.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let snap: Value = client
        .get(format!("{base}/api/v0/mode"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snap["media_trim_db"], -6.0);

    // Media play: amplitude scaled by 10^(-6/20)=0.5012.
    let media_post = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/v0/diagnostics/tests/play_asset"))
                .json(&json!({"asset": "trim-probe.wav", "channel": "media"}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut saw = false;
    let mut media_id: Option<String> = None;
    let mut media_pcm: Vec<u8> = Vec::new();
    while !saw && tokio::time::Instant::now() < deadline {
        let Ok(f) = tokio::time::timeout_at(deadline, gw.recv()).await else {
            break;
        };
        match f {
            CoreFrame::PlayBegin {
                task_id, channel, ..
            } => {
                assert_eq!(channel.as_deref(), Some("media"));
                media_id = Some(task_id);
            }
            CoreFrame::PlayChunk {
                task_id, audio_b64, ..
            } => {
                if media_id.as_deref() == Some(&task_id) {
                    use base64::Engine;
                    media_pcm.extend_from_slice(
                        &base64::engine::general_purpose::STANDARD
                            .decode(audio_b64)
                            .unwrap(),
                    );
                }
            }
            CoreFrame::PlayEnd { task_id } => {
                if media_id.as_deref() == Some(&task_id) {
                    let sample = i16::from_le_bytes([media_pcm[0], media_pcm[1]]) as f64;
                    let expected = 10000.0 * 10f64.powf(-6.0 / 20.0);
                    assert!(
                        (sample - expected).abs() < 30.0,
                        "media sample {sample} vs {expected}"
                    );
                    gw.confirm_play(&task_id).await;
                    saw = true;
                }
            }
            _ => {}
        }
    }
    assert!(saw, "media play never hit the wire");
    let _ = media_post.await;

    // Restore trim to 0 and verify a voice play is untouched.
    let r = client
        .put(format!("{base}/api/v0/mode/{dev}"))
        .json(&json!({"media_trim_db": 0.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let voice_post = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/v0/diagnostics/tests/play_asset"))
                .json(&json!({"asset": "trim-probe.wav", "channel": "voice"}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut saw_voice = false;
    let mut voice_id: Option<String> = None;
    let mut voice_pcm: Vec<u8> = Vec::new();
    while !saw_voice && tokio::time::Instant::now() < deadline {
        let Ok(f) = tokio::time::timeout_at(deadline, gw.recv()).await else {
            break;
        };
        match f {
            CoreFrame::PlayBegin {
                task_id, channel, ..
            } => {
                assert_eq!(channel.as_deref(), Some("voice"));
                voice_id = Some(task_id);
            }
            CoreFrame::PlayChunk {
                task_id, audio_b64, ..
            } => {
                if voice_id.as_deref() == Some(&task_id) {
                    use base64::Engine;
                    voice_pcm.extend_from_slice(
                        &base64::engine::general_purpose::STANDARD
                            .decode(audio_b64)
                            .unwrap(),
                    );
                }
            }
            CoreFrame::PlayEnd { task_id } => {
                if voice_id.as_deref() == Some(&task_id) {
                    assert_eq!(
                        i16::from_le_bytes([voice_pcm[0], voice_pcm[1]]),
                        10000,
                        "voice must be unscaled"
                    );
                    gw.confirm_play(&task_id).await;
                    saw_voice = true;
                }
            }
            _ => {}
        }
    }
    assert!(saw_voice, "voice play never hit the wire");
    let _ = voice_post.await;
}

/// The acceptance behaviour: a keyword hit during voice playback stops it —
/// exactly once — and the follow window opens so the next sentence is a
/// command without a second wake phrase.
#[tokio::test]
async fn keyword_stops_voice_playback_once_and_opens_follow_window() {
    let long_answer = "这是".to_string() + &"很长".repeat(400);
    let llm = FakeLlm::new(vec![
        Scripted::Text {
            chunks: vec![long_answer],
        },
        Scripted::Text {
            chunks: vec!["短故事讲完了。".into()],
        },
    ]);
    let (_base, state, gw) = spawn_app_with_gateway(llm).await;

    let play_id = start_long_voice_play(&state, &gw, 1, "讲个长故事").await;

    gw.send(&GatewayFrame::Keyword {
        origin: hm_core::gateway::InputOrigin::Legacy,
        device: "dot2-d".into(),
        ts: hm_core::id::now_ms(),
        seq: 2,
        keyword: "你好高达".into(),
        fed_ms: Some(2720),
        self_heard: false,
        rms_dbfs: None,
        lag_ms: None,
        bundle_id: None,
        kws_generation: None,
        kws_tier: None,
        wake_dur_ms: None,
        max_gap_ms: None,
        l2_passed: None,
    })
    .await;

    let stopped = stops_within(&gw, Duration::from_millis(600)).await;
    assert_eq!(
        stopped,
        vec![play_id.clone()],
        "keyword must stop the live voice play exactly once, saw {stopped:?}"
    );

    // Device truth comes back; the follow window is open, so a bare
    // follow-up sentence (no wake word) starts a new turn.
    gw.send(&GatewayFrame::StopReceipt {
        task_id: play_id,
        stage: "device_confirmed".into(),
        played_ms: Some(0),
        consumed_ms: None,
        progress_basis: None,
        detail: None,
    })
    .await;
    explicit_request(&state, &gw, 3, "换个短的").await;
    let (play2, _) = gw.until_play("voice").await;
    gw.confirm_play(&play2).await;

    let log = state.voice.recent(200);
    assert!(
        log.iter().any(|e| e["kind"] == "keyword_stop"),
        "keyword_stop must be logged: {log:?}"
    );
}

#[tokio::test]
async fn mute_hold_records_wifi_provisioning_event_and_never_resets() {
    let (_base, state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    state
        .config
        .save_device_meta("dot2-d", "客厅小达", "客厅", "test")
        .unwrap();

    for (seq, action) in [(1, "mute_hold_wifi_setup"), (2, "113_hold:3200ms")] {
        gw.send(&GatewayFrame::Button {
            device: "dot2-d".into(),
            ts: hm_core::id::now_ms(),
            seq,
            action: action.into(),
        })
        .await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut buttons = 0;
    while tokio::time::Instant::now() < deadline {
        let log = state.voice.recent(50);
        buttons = log.iter().filter(|e| e["kind"] == "button").count();
        if buttons >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(buttons, 2, "both button events must be logged");
    let log = state.voice.recent(50);
    let found = log
        .iter()
        .filter(|e| e["kind"] == "wifi_provisioning_requested")
        .count();
    assert_eq!(
        found, 1,
        "only mute_hold_wifi_setup must record wifi_provisioning_requested, not 113_hold*"
    );
    assert_eq!(
        state.voice.wifi_provisioning_devices(),
        vec!["dot2-d".to_string()]
    );
    assert_eq!(
        state
            .config
            .device_meta()
            .unwrap()
            .get("dot2-d")
            .map(|m| m.room.as_str()),
        Some("客厅"),
        "physical button hold must NEVER clear device_meta / trigger factory reset"
    );
}

#[tokio::test]
async fn two_stable_ids_hold_independent_volume_and_meta_with_echomuse_preset() {
    let (base, state, gw) = spawn_app_with_gateway(FakeLlm::new(vec![])).await;
    let client = reqwest::Client::new();

    // Verify echomuse EQ preset remains intact in config.
    let eq = state.config.audio_eq().unwrap();
    assert_eq!(eq.curve, "echomuse");
    assert!(hm_core::config::AUDIO_CURVES.contains(&"echomuse"));

    // Announce a second Dot2 unit on the gateway.
    gw.send(&GatewayFrame::DeviceStatus {
        northbound: None,
        device: "dot2-e".into(),
        online: true,
        reason: "registered".into(),
        descriptor: Some(hm_core::gateway::HelloDevice {
            stable_id: "dot2-e".into(),
            model: "dot2".into(),
            firmware: Some("0.1.0".into()),
            capabilities: vec!["audio_in".into(), "audio_out".into()],
            audio_profile: "dot2-unit-e".into(),
            wifi_ssid: Some("Home".into()),
        }),
    })
    .await;

    // Wait until dot2-e is visible on the hub.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snap = state.hub.snapshot();
        if snap["devices"]
            .as_array()
            .map(|a| a.iter().any(|d| d["stable_id"] == "dot2-e"))
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dot2-e did not appear"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Set independent device_meta (name & room) on dot2-d vs dot2-e.
    state
        .config
        .save_device_meta("dot2-d", "客厅音箱", "客厅", "test")
        .unwrap();
    state
        .config
        .save_device_meta("dot2-e", "儿童房音箱", "儿童房", "test")
        .unwrap();

    // Report independent volumes via VolumeReceipt for dot2-d and dot2-e,
    // plus independent MuteState on dot2-d vs dot2-e.
    gw.send(&GatewayFrame::VolumeReceipt {
        task_id: "v-d".into(),
        effective: 0.75,
        unit: "scalar".into(),
        stage: "device_confirmed".into(),
        device: Some("dot2-d".into()),
    })
    .await;
    gw.send(&GatewayFrame::VolumeReceipt {
        task_id: "v-e".into(),
        effective: 0.35,
        unit: "scalar".into(),
        stage: "device_confirmed".into(),
        device: Some("dot2-e".into()),
    })
    .await;
    gw.send(&GatewayFrame::MuteState {
        device: "dot2-d".into(),
        ts: hm_core::id::now_ms(),
        seq: 1,
        muted: true,
        source: "button".into(),
        task_id: None,
    })
    .await;
    gw.send(&GatewayFrame::MuteState {
        device: "dot2-e".into(),
        ts: hm_core::id::now_ms(),
        seq: 2,
        muted: false,
        source: "button".into(),
        task_id: None,
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snap = state.hub.snapshot();
    let devs = snap["devices"].as_array().unwrap();
    let d_vol = devs.iter().find(|d| d["stable_id"] == "dot2-d").unwrap()["volume"]
        .as_f64()
        .unwrap();
    let e_vol = devs.iter().find(|d| d["stable_id"] == "dot2-e").unwrap()["volume"]
        .as_f64()
        .unwrap();
    assert!((d_vol - 0.75).abs() < 0.01, "dot2-d volume={d_vol}");
    assert!((e_vol - 0.35).abs() < 0.01, "dot2-e volume={e_vol}");

    let meta = state.config.device_meta().unwrap();
    assert_eq!(meta.get("dot2-d").map(|m| m.room.as_str()), Some("客厅"));
    assert_eq!(meta.get("dot2-e").map(|m| m.room.as_str()), Some("儿童房"));

    let mode: Value = client
        .get(format!("{base}/api/v0/mode"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let d_mode = mode["devices"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["device"] == "dot2-d")
        .unwrap();
    let e_mode = mode["devices"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["device"] == "dot2-e")
        .unwrap();
    assert_eq!(d_mode["muted"], json!(true));
    assert_eq!(e_mode["muted"], json!(false));
}
