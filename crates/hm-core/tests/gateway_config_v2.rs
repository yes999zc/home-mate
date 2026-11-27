//! Integration tests for Core Gateway config_apply_v2 wiring,
//! ConfigApplyCoordinator preflight/dispatch/retry/rollback, readback evidence,
//! and the exact-unit acoustic gate.

use hm_core::api::AppState;
use hm_core::config_delivery::{
    Block, Commit, Compatibility, ConfigApplyCoordinator, ConfigDelivery, Operation, Package,
    Purpose, Source, Target,
};
use hm_core::gateway::{ConfigBlock, ConfigReceiptStatus, CoreFrame, GatewayFrame, HelloDevice};
use hm_core::store::Store;
use hm_core::tool::mocks::MockInject;
use serde_json::json;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

struct TempDir(std::path::PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn make_app() -> (TempDir, Arc<AppState>) {
    let dir = std::env::temp_dir().join(format!("hm_gw_v2_test_{}", hm_core::id::next("T")));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir.join("hm.db")).unwrap());
    let state = AppState::with_overrides(
        store,
        dir.clone(),
        MockInject::default(),
        None,
        None,
        None,
        None,
    );
    (TempDir(dir), state)
}

fn make_pkg(
    household_id: &str,
    purpose: Purpose,
    source_node: &str,
    target_node: Option<&str>,
    blocks: Vec<Block>,
) -> Package {
    Package {
        format: "home-mate-config".into(),
        version: 2,
        package_id: "00000000-0000-4000-8000-000000000001".into(),
        package_revision: 1,
        created_ms: 1,
        purpose,
        source: Source {
            node_id: source_node.into(),
            config_revision: 0,
            core_version: "0.1.0".into(),
            schema_version: 2,
        },
        target: Target {
            household_id: household_id.into(),
            node_id: target_node.map(str::to_string),
        },
        compatibility: Compatibility {
            core_api_schema: 2,
            gateway_protocol_min: 0,
            gateway_protocol_max: 2,
            required_capabilities: vec!["config_apply_v2".into()],
        },
        change_reason: "integration test".into(),
        blocks,
        bindings: vec![],
        content_hash: None,
    }
}

async fn connect_mock_gateway(
    state: &Arc<AppState>,
) -> (
    tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    tokio::net::tcp::OwnedWriteHalf,
) {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let hub = state.hub.clone();
    tokio::spawn(async move {
        let _ = hub.listen(addr).await;
    });

    let stream = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();

    let hello = GatewayFrame::Hello {
        protocol: 2,
        min_protocol: 0,
        supported_versions: vec![0, 2],
        gateway: "mock-gw".into(),
        gateway_version: "0.2.0".into(),
        devices: vec![HelloDevice {
            stable_id: "dot2-d".into(),
            model: "AEOBC".into(),
            firmware: None,
            capabilities: vec![
                "audio_out".into(),
                "config_apply_v2".into(),
                "config_block_kws".into(),
                "config_block_audio_eq".into(),
                "config_block_led".into(),
                "config_block_duplex".into(),
                "config_block_acoustics".into(),
            ],
            audio_profile: "p".into(),
            wifi_ssid: None,
        }],
        aec_tail_ms: None,
        profile_version: None,
        auth_token: None,
    };
    wr.write_all(serde_json::to_string(&hello).unwrap().as_bytes())
        .await
        .unwrap();
    wr.write_all(b"\n").await.unwrap();
    let _ack = lines.next_line().await.unwrap().unwrap();
    let _time = lines.next_line().await.unwrap().unwrap();
    for _ in 0..50 {
        if state.hub.device_exists("dot2-d") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (lines, wr)
}

async fn send_gw(wr: &mut tokio::net::tcp::OwnedWriteHalf, frame: &GatewayFrame) {
    wr.write_all(serde_json::to_string(frame).unwrap().as_bytes())
        .await
        .unwrap();
    wr.write_all(b"\n").await.unwrap();
}

#[tokio::test]
async fn exact_unit_acoustic_gate_rejects_family_policy_foreign_unit_and_cross_node_restore() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::for_app(&state);
    let enrolled = delivery.enroll("dot2-d").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    // 1. FamilyPolicy cannot write unit_acoustic_profiles.* or mode.kws_threshold
    let fam_pkg = make_pkg(
        &hh,
        Purpose::FamilyPolicy,
        "dot2-d",
        None,
        vec![Block {
            name: "unit_acoustic_profiles".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "unit_acoustic_profiles.dot2-d.volume_cap".into(),
                value: json!(0.8),
            }],
        }],
    );
    let err = delivery.preflight("admin", fam_pkg, json!({})).unwrap_err();
    assert_eq!(err.code, "FORBIDDEN_SCOPE");

    let fam_kws = make_pkg(
        &hh,
        Purpose::FamilyPolicy,
        "dot2-d",
        None,
        vec![Block {
            name: "mode".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "mode.kws_threshold".into(),
                value: json!(0.15),
            }],
        }],
    );
    let err = delivery.preflight("admin", fam_kws, json!({})).unwrap_err();
    assert_eq!(err.code, "FORBIDDEN_SCOPE");

    // 2. NodeConfig for dot2-d cannot write foreign unit dot2-e
    let foreign_pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "unit_acoustic_profiles".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "unit_acoustic_profiles.dot2-e.volume_cap".into(),
                value: json!(0.8),
            }],
        }],
    );
    let err = delivery
        .preflight("admin", foreign_pkg, json!({}))
        .unwrap_err();
    assert_eq!(err.code, "FORBIDDEN_SCOPE");

    // 3. Cross-node SameNodeRestore is rejected
    let cross_restore = make_pkg(
        &hh,
        Purpose::SameNodeRestore,
        "dot2-e",
        Some("dot2-d"),
        vec![Block {
            name: "unit_acoustic_profiles".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "unit_acoustic_profiles.dot2-d.volume_cap".into(),
                value: json!(0.8),
            }],
        }],
    );
    let err = delivery
        .preflight("admin", cross_restore, json!({}))
        .unwrap_err();
    assert_eq!(err.code, "TARGET_MISMATCH");

    // 4. NodeConfig for exact enrolled unit dot2-d succeeds
    let valid_pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "unit_acoustic_profiles".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "unit_acoustic_profiles.dot2-d.volume_cap".into(),
                value: json!(0.8),
            }],
        }],
    );
    assert!(delivery.preflight("admin", valid_pkg, json!({})).is_ok());
}

#[tokio::test]
async fn coordinator_dispatches_audio_eq_persisted_to_active_and_surfaces_readback() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::new(state.store.clone());
    let enrolled = delivery.enroll("dot2-d").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    let (mut lines, mut wr) = connect_mock_gateway(&state).await;
    let _worker = ConfigApplyCoordinator::start_for_app(&state);

    let pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "audio_eq".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "audio_eq.curve".into(),
                value: json!("official"),
            }],
        }],
    );
    let preflight = delivery.preflight("admin", pkg, json!({})).unwrap();
    let app_id = "11111111-1111-4111-8111-111111111111";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: preflight["plan_id"].as_str().unwrap().into(),
                plan_hash: preflight["plan_hash"].as_str().unwrap().into(),
                expected_revision: preflight["expected_revision"].as_u64().unwrap(),
                application_id: app_id.into(),
            },
        )
        .unwrap();

    // Serve Gateway side: ConfigStateGet -> ConfigValidate -> ConfigApply (Persisted -> Active)
    let gw_task = tokio::spawn(async move {
        let mut saw_apply = false;
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: Some("fp-1".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: true,
                            supported: true,
                            blockers: Vec::new(),
                            state: ConfigReceiptStatus::Validated,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: None,
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    assert_eq!(device, "dot2-d");
                    assert_eq!(block, ConfigBlock::AudioEq);
                    assert_eq!(values["curve"], json!("official"));
                    saw_apply = true;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device: device.clone(),
                            core_node_id: core_node_id.clone(),
                            data_epoch,
                            application_id: application_id.clone(),
                            operation_id: operation_id.clone(),
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash: block_hash.clone(),
                            state: ConfigReceiptStatus::Persisted,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(2),
                            persisted_hash: Some("ph-1".into()),
                            active_hash: None,
                            active_values: None,
                            evidence: None,
                            ts: 102,
                        },
                    )
                    .await;
                    tokio::time::sleep(Duration::from_millis(45)).await;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            data_epoch,
                            application_id,
                            operation_id,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash,
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(2),
                            persisted_hash: Some("ph-1".into()),
                            active_hash: Some("ah-1".into()),
                            active_values: Some(values),
                            evidence: Some(json!({"rebuild_ms": 42})),
                            ts: 147,
                        },
                    )
                    .await;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_apply);
    });

    for _ in 0..80 {
        let app = delivery.application("admin", app_id).unwrap();
        if app["blocks"][0]["state"] == "applied" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    gw_task.await.unwrap();

    let rb = delivery.readback("admin", app_id).unwrap();
    assert_eq!(rb["publication"]["state"], json!("applied"));
    assert_eq!(
        rb["publication"]["gateway_receipt"][0]["active_hash"],
        json!("ah-1")
    );
    assert_eq!(state.config.audio_eq().unwrap().curve, "official");
}

#[tokio::test]
async fn coordinator_retries_on_retryable_boot_id_mismatch_and_rolls_back_on_partial_failure() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::new(state.store.clone());
    let enrolled = delivery.enroll("dot2-d").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    let (mut lines, mut wr) = connect_mock_gateway(&state).await;
    let _worker = ConfigApplyCoordinator::start_for_app(&state);

    // Part A: retryable state_revision mismatch on first ConfigValidate succeeds on retry
    let pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "unit_acoustic_profiles".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "unit_acoustic_profiles.dot2-d.volume_cap".into(),
                value: json!(0.75),
            }],
        }],
    );
    let preflight = delivery.preflight("admin", pkg, json!({})).unwrap();
    let app_retry = "22222222-2222-4222-8222-222222222222";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: preflight["plan_id"].as_str().unwrap().into(),
                plan_hash: preflight["plan_hash"].as_str().unwrap().into(),
                expected_revision: preflight["expected_revision"].as_u64().unwrap(),
                application_id: app_retry.into(),
            },
        )
        .unwrap();

    let validate_calls = Arc::new(AtomicU64::new(0));
    let validate_calls_c = validate_calls.clone();
    let gw_task = tokio::spawn(async move {
        let mut rev = 10u64;
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    rev += 1;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some(format!("boot-{rev}")),
                            state_revision: Some(rev),
                            capability_fingerprint: Some("fp".into()),
                            blocks: vec![],
                            ts: 200,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    let n = validate_calls_c.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // First validate returns retryable boot/revision mismatch
                        send_gw(
                            &mut wr,
                            &GatewayFrame::ConfigValidated {
                                request_id,
                                device,
                                block,
                                schema_version,
                                block_hash,
                                ok: false,
                                supported: false,
                                blockers: Vec::new(),
                                state: ConfigReceiptStatus::Failed,
                                reason: Some("state_revision_mismatch".into()),
                                retryable: true,
                                gateway_boot_id: Some("boot-new".into()),
                                state_revision: Some(rev + 1),
                                capability_fingerprint: None,
                                evidence: None,
                                ts: 201,
                            },
                        )
                        .await;
                    } else {
                        send_gw(
                            &mut wr,
                            &GatewayFrame::ConfigValidated {
                                request_id,
                                device,
                                block,
                                schema_version,
                                block_hash,
                                ok: true,
                                supported: true,
                                blockers: Vec::new(),
                                state: ConfigReceiptStatus::Validated,
                                reason: None,
                                retryable: false,
                                gateway_boot_id: Some(format!("boot-{rev}")),
                                state_revision: Some(rev),
                                capability_fingerprint: None,
                                evidence: None,
                                ts: 202,
                            },
                        )
                        .await;
                    }
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    assert_eq!(device, "dot2-d");
                    assert_eq!(block, ConfigBlock::Acoustics);
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            data_epoch,
                            application_id,
                            operation_id,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash,
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some(format!("boot-{rev}")),
                            state_revision: Some(rev + 1),
                            persisted_hash: Some("ph".into()),
                            active_hash: Some("ah".into()),
                            active_values: None,
                            evidence: None,
                            ts: 203,
                        },
                    )
                    .await;
                    break;
                }
                _ => {}
            }
        }
    });

    for _ in 0..80 {
        let app = delivery.application("admin", app_retry).unwrap();
        if app["blocks"][0]["state"] == "applied" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    gw_task.await.unwrap();
    assert_eq!(validate_calls.load(Ordering::SeqCst), 2);
    assert!(
        (state
            .config
            .unit_acoustic_profile("dot2-d")
            .unwrap()
            .volume_cap
            - 0.75)
            .abs()
            < 1e-6
    );
}

#[tokio::test]
async fn coordinator_marks_unsupported_duplex_full_as_failed_and_preserves_local_state() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::for_app(&state);
    let enrolled = delivery.enroll("dot2-d").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    let (mut lines, mut wr) = connect_mock_gateway(&state).await;
    let _worker = ConfigApplyCoordinator::start_for_app(&state);

    let pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "mode".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "mode.duplex_requested.dot2-d".into(),
                value: json!("full"),
            }],
        }],
    );
    let preflight = delivery.preflight("admin", pkg, json!({})).unwrap();
    let app_fail = "33333333-3333-4333-8333-333333333333";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: preflight["plan_id"].as_str().unwrap().into(),
                plan_hash: preflight["plan_hash"].as_str().unwrap().into(),
                expected_revision: preflight["expected_revision"].as_u64().unwrap(),
                application_id: app_fail.into(),
            },
        )
        .unwrap();

    let gw_task = tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: Some("fp-1".into()),
                            blocks: vec![],
                            ts: 300,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: false,
                            supported: false,
                            blockers: Vec::new(),
                            state: ConfigReceiptStatus::Unsupported,
                            reason: Some("full_duplex_unsupported".into()),
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: None,
                            evidence: None,
                            ts: 301,
                        },
                    )
                    .await;
                    break;
                }
                _ => {}
            }
        }
    });

    for _ in 0..80 {
        let app = delivery.application("admin", app_fail).unwrap();
        if app["blocks"][0]["state"] == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    gw_task.await.unwrap();

    let rb = delivery.readback("admin", app_fail).unwrap();
    assert_eq!(rb["publication"]["state"], json!("failed"));
    assert_eq!(rb["publication"]["error"], json!("full_duplex_unsupported"));
    // VoiceSettings must NOT have been mutated to "full"
    assert!(state
        .voice
        .settings_snapshot()
        .duplex_requested
        .get("dot2-d")
        .is_none());
}

#[tokio::test]
async fn coordinator_dispatches_node_wake_phrases_as_full_compiled_kws_and_led_with_timezone() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::for_app(&state);
    let enrolled = delivery.enroll("dot2-d").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    let (mut lines, mut wr) = connect_mock_gateway(&state).await;
    let _worker = ConfigApplyCoordinator::start_for_app(&state);

    let pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![
            Block {
                name: "node".into(),
                schema_version: 1,
                operations: vec![
                    Operation::Replace {
                        path: "node.wake.phrases".into(),
                        value: json!(["你好小智"]),
                    },
                    Operation::Set {
                        path: "node.led.timezone".into(),
                        value: json!("Asia/Shanghai"),
                    },
                ],
            },
            Block {
                name: "device_led_configs".into(),
                schema_version: 1,
                operations: vec![
                    Operation::Set {
                        path: "device_led_configs.dot2-d.night_enabled".into(),
                        value: json!(true),
                    },
                    Operation::Set {
                        path: "device_led_configs.dot2-d.night_start_minute".into(),
                        value: json!(1320),
                    },
                    Operation::Set {
                        path: "device_led_configs.dot2-d.night_end_minute".into(),
                        value: json!(420),
                    },
                    Operation::Set {
                        path: "device_led_configs.dot2-d.night_brightness_pct".into(),
                        value: json!(10),
                    },
                ],
            },
        ],
    );
    let preflight = delivery.preflight("admin", pkg, json!({})).unwrap();
    let app_id = "44444444-4444-4444-8444-444444444444";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: preflight["plan_id"].as_str().unwrap().into(),
                plan_hash: preflight["plan_hash"].as_str().unwrap().into(),
                expected_revision: preflight["expected_revision"].as_u64().unwrap(),
                application_id: app_id.into(),
            },
        )
        .unwrap();

    let gw_task = tokio::spawn(async move {
        let mut applied_blocks = Vec::new();
        let mut kws_generations = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: Some("fp-1".into()),
                            blocks: vec![],
                            ts: 400,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: true,
                            supported: true,
                            blockers: Vec::new(),
                            state: ConfigReceiptStatus::Validated,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(1),
                            capability_fingerprint: None,
                            evidence: None,
                            ts: 401,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    match block {
                        ConfigBlock::Kws => {
                            let kw = values["keywords"].as_str().unwrap();
                            assert!(
                                kw.contains("n ǐ h ǎo x iǎo zh ì @你好小智\n"),
                                "KWS keywords must be compiled with pinyin tokens: {kw}"
                            );
                            for non_wake in [
                                "@暂停播放",
                                "@继续播放",
                                "@下一首",
                                "@停止播放",
                                "@退下",
                                "@退下吧",
                            ] {
                                assert!(
                                    kw.contains(non_wake),
                                    "KWS must preserve non-wake reflex words ({non_wake}): {kw}"
                                );
                            }
                            assert_eq!(
                                values["retreat_words"],
                                json!(["停止播放", "退下", "退下吧"])
                            );
                            assert!(values["intents_hash"]
                                .as_str()
                                .is_some_and(|s| !s.is_empty()));
                            assert!(values["threshold"].as_f64().is_some());
                            assert!(values["score"].as_f64().is_some());
                            if let Some(&prev_gen) = kws_generations.last() {
                                assert!(
                                    block_generation > prev_gen,
                                    "KWS block_generation must increase monotonically ({block_generation} <= {prev_gen})"
                                );
                            }
                            kws_generations.push(block_generation);
                        }
                        ConfigBlock::Led => {
                            assert_eq!(values["timezone"], json!("Asia/Shanghai"));
                            assert_eq!(values["night_enabled"], json!(true));
                            assert_eq!(values["night_start_minute"], json!(1320));
                            assert_eq!(values["night_end_minute"], json!(420));
                            assert_eq!(values["night_brightness_pct"], json!(10));
                        }
                        other => panic!("unexpected block {other:?}"),
                    }
                    applied_blocks.push(block);
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            data_epoch,
                            application_id,
                            operation_id,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash,
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-1".into()),
                            state_revision: Some(2),
                            persisted_hash: Some("ph".into()),
                            active_hash: Some("ah".into()),
                            active_values: Some(values),
                            evidence: None,
                            ts: 402,
                        },
                    )
                    .await;
                    if kws_generations.len() >= 2 && applied_blocks.contains(&ConfigBlock::Led) {
                        return (lines, wr);
                    }
                }
                _ => {}
            }
        }
        panic!("gateway loop exited early");
    });

    for _ in 0..80 {
        let rb = delivery.readback("admin", app_id).unwrap();
        if rb["publications"]
            .as_array()
            .is_some_and(|arr| arr.iter().all(|p| p["state"] == "applied"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let rb = delivery.readback("admin", app_id).unwrap();
    assert!(
        rb["publications"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["state"] == "applied"),
        "{rb}"
    );

    // Follow-up mode.kws_threshold apply must use a strictly higher Kws block_generation
    // and preserve `node.wake.phrases` ("你好小智").
    let mode_pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "dot2-d",
        Some("dot2-d"),
        vec![Block {
            name: "mode".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "mode.kws_threshold".into(),
                value: json!(0.12),
            }],
        }],
    );
    let preflight2 = delivery.preflight("admin", mode_pkg, json!({})).unwrap();
    let app_id2 = "55555555-5555-4555-8555-555555555555";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: preflight2["plan_id"].as_str().unwrap().into(),
                plan_hash: preflight2["plan_hash"].as_str().unwrap().into(),
                expected_revision: preflight2["expected_revision"].as_u64().unwrap(),
                application_id: app_id2.into(),
            },
        )
        .unwrap();

    let (_lines, mut wr) = gw_task.await.unwrap();
    for _ in 0..80 {
        let rb2 = delivery.readback("admin", app_id2).unwrap();
        if rb2["publication"]["state"] == "applied" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Verify VoicePipeline recognizes "你好小智" on both KWS (Keyword) and ASR (Utterance) paths.
    send_gw(
        &mut wr,
        &GatewayFrame::Keyword {
            origin: hm_core::gateway::InputOrigin::Legacy,
            device: "dot2-d".into(),
            ts: hm_core::id::now_ms(),
            seq: 10,
            keyword: "你好小智".into(),
            fed_ms: None,
            self_heard: false,
            rms_dbfs: None,
            lag_ms: None,
            bundle_id: None,
            kws_generation: None,
            kws_tier: None,
            wake_dur_ms: None,
            max_gap_ms: None,
            l2_passed: None,
        },
    )
    .await;
    for _ in 0..40 {
        if state
            .voice
            .recent(50)
            .iter()
            .any(|e| e["kind"] == "keyword_wake" && e["keyword"] == "你好小智")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        state
            .voice
            .recent(50)
            .iter()
            .any(|e| e["kind"] == "keyword_wake" && e["keyword"] == "你好小智"),
        "KWS Keyword path must recognize node.wake.phrases '你好小智'"
    );

    send_gw(
        &mut wr,
        &GatewayFrame::Utterance {
            origin: hm_core::gateway::InputOrigin::Legacy,
            device: "dot2-d".into(),
            ts: hm_core::id::now_ms(),
            seq: 11,
            source: "asr".into(),
            text: "你好小智，退下吧".into(),
        },
    )
    .await;
    for _ in 0..40 {
        if state
            .voice
            .recent(50)
            .iter()
            .any(|e| e["kind"] == "session_retreat" && e["text"] == "你好小智，退下吧")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        state
            .voice
            .recent(50)
            .iter()
            .any(|e| e["kind"] == "session_retreat" && e["text"] == "你好小智，退下吧"),
        "ASR exit_text path must recognize node.wake.phrases '你好小智'"
    );
}

#[tokio::test]
async fn device_profile_put_dispatches_acoustics_v2_and_reports_honest_states() {
    let (_dir, state) = make_app();
    let app = hm_core::api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();

    // 1. Offline device: saves SQLite desired state but honestly reports saved_pending (never applied).
    let off: serde_json::Value = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({
            "acoustic": {
                "volume_cap": 0.75,
                "mix_gain": 1.25,
                "vad_threshold": 0.5,
                "onset_threshold_dbfs": -36.5,
                "mic_gain_offset_db": 2.0
            }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(off["state"], "saved_pending", "{off}");
    assert_eq!(off["acoustics_state"], "saved_pending", "{off}");
    assert_eq!(off["acoustics_pushed"], false, "{off}");

    // 2. Out-of-range mix_gain (> 2.0) and onset_threshold_dbfs (> 0.0) are rejected synchronously.
    let bad_mix = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({ "mix_gain": 2.5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_mix.status(), reqwest::StatusCode::BAD_REQUEST);
    let bad_onset = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({ "onset_threshold_dbfs": 5.0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_onset.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        (state
            .config
            .unit_acoustic_profile("dot2-d")
            .unwrap()
            .mix_gain
            - 1.25)
            .abs()
            < 1e-6,
        "failed validation must leave stored mix_gain unchanged"
    );

    // 3. Connected v2 Gateway: automatically reconciles the offline saved_pending profile
    // upon DeviceJoined with non-zero monotonic block_generation, then handles online
    // applied, Gateway validation failure, and Superseded rollback cleanly.
    let (mut lines, mut wr) = connect_mock_gateway(&state).await;
    let captured_applies = Arc::new(std::sync::Mutex::new(Vec::<(serde_json::Value, u64)>::new()));
    let cap_clone = captured_applies.clone();
    let reconciled = Arc::new(tokio::sync::Notify::new());
    let reconciled_tx = reconciled.clone();
    let gw_task = tokio::spawn(async move {
        let mut validate_step = 0usize;
        let mut apply_step = 0usize;
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-ac".into()),
                            state_revision: Some(validate_step as u64 + 1),
                            capability_fingerprint: Some("fp-ac".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    let reject = validate_step == 2;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: !reject,
                            supported: !reject,
                            blockers: if reject {
                                vec!["ACOUSTICS_REJECTED".into()]
                            } else {
                                vec![]
                            },
                            state: if reject {
                                ConfigReceiptStatus::Failed
                            } else {
                                ConfigReceiptStatus::Validated
                            },
                            reason: reject.then(|| "mock rejection".into()),
                            retryable: false,
                            gateway_boot_id: Some("boot-ac".into()),
                            state_revision: Some(validate_step as u64 + 1),
                            capability_fingerprint: Some("fp-ac".into()),
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                    validate_step += 1;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    cap_clone
                        .lock()
                        .unwrap()
                        .push((values.clone(), block_generation));
                    let is_superseded = apply_step == 2;
                    let receipt_state = if is_superseded {
                        ConfigReceiptStatus::Superseded
                    } else {
                        ConfigReceiptStatus::Active
                    };
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            gateway_boot_id: Some("boot-ac".into()),
                            operation_id,
                            application_id,
                            data_epoch,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash,
                            state_revision: Some(apply_step as u64 + 2),
                            state: receipt_state,
                            reason: is_superseded.then(|| "superseded by newer generation".into()),
                            persisted_hash: Some("ph".into()),
                            active_hash: (!is_superseded).then(|| "ah".into()),
                            active_values: (!is_superseded).then_some(values),
                            evidence: None,
                            retryable: false,
                            ts: 102,
                        },
                    )
                    .await;
                    if apply_step == 0 {
                        reconciled_tx.notify_one();
                    }
                    apply_step += 1;
                    if is_superseded {
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), reconciled.notified())
        .await
        .expect("offline saved_pending acoustics must reconcile upon connect");
    let recon_gen = {
        let applies = captured_applies.lock().unwrap();
        assert_eq!(
            applies.len(),
            1,
            "expected initial reconnect reconciliation"
        );
        let (ref recon_vals, recon_gen) = applies[0];
        assert!(
            recon_gen > 0,
            "block_generation must enforce Gateway high-water fence"
        );
        assert!((recon_vals["volume_cap"].as_f64().unwrap() - 0.75).abs() < 1e-6);
        assert!((recon_vals["mix_gain"].as_f64().unwrap() - 1.25).abs() < 1e-6);
        assert!((recon_vals["onset_threshold_dbfs"].as_f64().unwrap() - (-36.5)).abs() < 1e-6);
        assert!((recon_vals["mic_gain_offset_db"].as_f64().unwrap() - 2.0).abs() < 1e-6);
        recon_gen
    };

    // 4. Online PUT updates values including volume_cap=0.0 mute and advances block_generation.
    let applied: serde_json::Value = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({
            "volume_cap": 0.0,
            "mix_gain": 1.4,
            "onset_threshold_dbfs": -38.0,
            "mic_gain_offset_db": 1.5
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(applied["state"], "applied", "{applied}");
    assert_eq!(applied["acoustics_state"], "applied", "{applied}");
    assert_eq!(applied["acoustics_pushed"], true, "{applied}");
    assert_eq!(
        applied["acoustics_receipt"]["block"], "acoustics",
        "{applied}"
    );

    {
        let applies = captured_applies.lock().unwrap();
        assert_eq!(applies.len(), 2);
        let (ref vals, online_gen) = applies[1];
        assert!(
            online_gen > recon_gen,
            "new PUT block_generation ({online_gen}) must exceed reconnect reconciliation ({recon_gen})"
        );
        assert_eq!(vals["volume_cap"], json!(0.0));
        assert!((vals["mix_gain"].as_f64().unwrap() - 1.4).abs() < 1e-6);
        assert!((vals["onset_threshold_dbfs"].as_f64().unwrap() - (-38.0)).abs() < 1e-6);
        assert!((vals["mic_gain_offset_db"].as_f64().unwrap() - 1.5).abs() < 1e-6);
    }

    // 5. Gateway validation rejection reports "failed" honestly and preserves SQLite.
    let failed: serde_json::Value = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({ "volume_cap": 0.5 }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(failed["state"], "failed", "{failed}");
    assert_eq!(failed["acoustics_state"], "failed", "{failed}");
    assert_eq!(failed["acoustics_pushed"], false, "{failed}");
    assert_eq!(failed["acoustic"]["volume_cap"], json!(0.0), "{failed}");
    assert!(
        (state
            .config
            .unit_acoustic_profile("dot2-d")
            .unwrap()
            .volume_cap
            - 0.0)
            .abs()
            < 1e-6,
        "Gateway rejection must not overwrite persisted unit_acoustic_profile"
    );

    // 6. Gateway Superseded receipt reports "superseded" honestly and preserves SQLite.
    let superseded: serde_json::Value = client
        .put(format!("{base}/api/v0/devices/dot2-d/profile"))
        .json(&json!({ "volume_cap": 0.4 }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(superseded["state"], "superseded", "{superseded}");
    assert_eq!(superseded["acoustics_state"], "superseded", "{superseded}");
    assert_eq!(superseded["acoustics_pushed"], false, "{superseded}");
    assert_eq!(
        superseded["acoustic"]["volume_cap"],
        json!(0.0),
        "{superseded}"
    );
    assert!(
        (state
            .config
            .unit_acoustic_profile("dot2-d")
            .unwrap()
            .volume_cap
            - 0.0)
            .abs()
            < 1e-6,
        "Gateway Superseded must not overwrite persisted unit_acoustic_profile"
    );

    gw_task.await.unwrap();
    server.abort();
}

#[tokio::test]
async fn deploy_reflex_auto_dispatches_kws_over_config_apply_v2() {
    let (_tmp, state) = make_app();
    let (mut lines, mut wr) = connect_mock_gateway(&state).await;

    let captured_block = Arc::new(std::sync::Mutex::new(None::<ConfigBlock>));
    let captured_ref = captured_block.clone();

    let gw_task = tokio::spawn(async move {
        let mut rev = 1u64;
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-kws-1".into()),
                            state_revision: Some(rev),
                            capability_fingerprint: Some("fp-kws".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: true,
                            supported: true,
                            blockers: vec![],
                            state: ConfigReceiptStatus::Validated,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-kws-1".into()),
                            state_revision: Some(rev),
                            capability_fingerprint: Some("fp-kws".into()),
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    *captured_ref.lock().unwrap() = Some(block);
                    rev += 1;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            gateway_boot_id: Some("boot-kws-1".into()),
                            operation_id,
                            application_id,
                            data_epoch,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash: block_hash.clone(),
                            state_revision: Some(rev),
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            persisted_hash: Some(block_hash.clone()),
                            active_hash: Some(block_hash),
                            active_values: Some(values),
                            evidence: None,
                            retryable: false,
                            ts: 102,
                        },
                    )
                    .await;
                    break;
                }
                _ => {}
            }
        }
    });

    let res = state
        .voice
        .deploy_reflex_auto("dot2-d")
        .await
        .expect("deploy_reflex_auto should succeed over config_apply_v2");
    assert_eq!(res["via"], "config_apply_v2");
    assert_eq!(res["state"], "applied");
    assert_eq!(*captured_block.lock().unwrap(), Some(ConfigBlock::Kws));

    gw_task.await.unwrap();
}

#[tokio::test]
async fn deploy_reflex_auto_recovers_after_invalid_kws_rejection_and_fences_stale_keyword_generation(
) {
    let (_tmp, state) = make_app();
    let (mut lines, mut wr) = connect_mock_gateway(&state).await;

    let applied_gen = Arc::new(AtomicU64::new(0));
    let applied_gen_ref = applied_gen.clone();

    let gw_task = tokio::spawn(async move {
        let mut attempt = 0usize;
        let mut rev = 1u64;
        while let Ok(Some(line)) = lines.next_line().await {
            match serde_json::from_str::<CoreFrame>(&line).unwrap() {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-kws-rec".into()),
                            state_revision: Some(rev),
                            capability_fingerprint: Some("fp-kws".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    attempt += 1;
                    let reject = attempt == 1;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: !reject,
                            supported: !reject,
                            blockers: if reject {
                                vec!["INVALID_KWS_TOKENS".into()]
                            } else {
                                vec![]
                            },
                            state: if reject {
                                ConfigReceiptStatus::Failed
                            } else {
                                ConfigReceiptStatus::Validated
                            },
                            reason: reject.then(|| "invalid_kws_bundle".into()),
                            retryable: false,
                            gateway_boot_id: Some("boot-kws-rec".into()),
                            state_revision: Some(rev),
                            capability_fingerprint: Some("fp-kws".into()),
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    // The gateway's bundle counter is independent of Core's
                    // config transaction counter, and lives in active_values.
                    let bundle_generation = block_generation + 100;
                    let mut values = values;
                    values["kws_generation"] = json!(bundle_generation);
                    applied_gen_ref.store(bundle_generation, Ordering::SeqCst);
                    rev += 1;
                    send_gw(
                        &mut wr,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            gateway_boot_id: Some("boot-kws-rec".into()),
                            operation_id,
                            application_id,
                            data_epoch,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash: block_hash.clone(),
                            state_revision: Some(rev),
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            persisted_hash: Some(block_hash.clone()),
                            active_hash: Some(block_hash),
                            active_values: Some(values),
                            evidence: None,
                            retryable: false,
                            ts: 102,
                        },
                    )
                    .await;
                    return wr;
                }
                _ => {}
            }
        }
        panic!("gateway loop ended early");
    });

    // 1. First deploy fails validation ("invalid_kws_bundle") -> error surfaced honestly.
    let err = state
        .voice
        .deploy_reflex_auto("dot2-d")
        .await
        .expect_err("rejected KWS bundle must return Err");
    assert!(
        err.contains("invalid_kws_bundle") || err.contains("INVALID_KWS_TOKENS"),
        "unexpected error: {err}"
    );
    assert_eq!(applied_gen.load(Ordering::SeqCst), 0);

    // 2. Second deploy succeeds and activates KWS bundle generation `g`.
    let ok = state
        .voice
        .deploy_reflex_auto("dot2-d")
        .await
        .expect("recovery KWS deploy should succeed");
    assert_eq!(ok["state"], "applied");
    let mut wr = gw_task.await.unwrap();
    let g = applied_gen.load(Ordering::SeqCst);
    assert!(g > 0);

    // 3. Stale Keyword frame from `g - 1` is fenced as `stale_kws_keyword`; current `g` hits.
    send_gw(
        &mut wr,
        &GatewayFrame::Keyword {
            origin: hm_core::gateway::InputOrigin::Legacy,
            device: "dot2-d".into(),
            ts: hm_core::id::now_ms(),
            seq: 20,
            keyword: "你好高达".into(),
            fed_ms: None,
            self_heard: false,
            rms_dbfs: Some(-20.0),
            lag_ms: None,
            bundle_id: Some("old-bundle".into()),
            kws_generation: Some(g - 1),
            kws_tier: None,
            wake_dur_ms: None,
            max_gap_ms: None,
            l2_passed: None,
        },
    )
    .await;
    send_gw(
        &mut wr,
        &GatewayFrame::Keyword {
            origin: hm_core::gateway::InputOrigin::Legacy,
            device: "dot2-d".into(),
            ts: hm_core::id::now_ms(),
            seq: 21,
            keyword: "你好高达".into(),
            fed_ms: None,
            self_heard: false,
            rms_dbfs: Some(-20.0),
            lag_ms: None,
            bundle_id: Some("current-bundle".into()),
            kws_generation: Some(g),
            kws_tier: None,
            wake_dur_ms: None,
            max_gap_ms: None,
            l2_passed: None,
        },
    )
    .await;

    for _ in 0..40 {
        if state
            .hub
            .recent(20)
            .iter()
            .any(|e| e["kind"] == "keyword" && e["seq"] == 21)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let hub_events = state.hub.recent(20);
    assert!(
        hub_events
            .iter()
            .any(|e| e["kind"] == "stale_kws_keyword" && e["kws_generation"] == g - 1),
        "expected stale_kws_keyword for generation {}: {hub_events:?}",
        g - 1
    );
    assert!(
        hub_events
            .iter()
            .any(|e| e["kind"] == "keyword" && e["kws_generation"] == g),
        "expected active keyword hit for generation {g}: {hub_events:?}"
    );
}

async fn connect_v2_device_with_caps(
    state: &Arc<AppState>,
    device_id: &str,
    caps: Vec<&str>,
) -> (
    tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    tokio::net::tcp::OwnedWriteHalf,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hub = state.hub.clone();
    tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        let _ = hub.serve_connection_from(stream, Some(peer)).await;
    });
    let stream = TcpStream::connect(addr).await.unwrap();
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();
    let hello = GatewayFrame::Hello {
        protocol: 2,
        min_protocol: 0,
        supported_versions: vec![0, 2],
        gateway: format!("gw-{device_id}"),
        gateway_version: "2.3.5".into(),
        devices: vec![HelloDevice {
            stable_id: device_id.into(),
            model: "android".into(),
            firmware: Some("2.3.5".into()),
            capabilities: caps.into_iter().map(String::from).collect(),
            audio_profile: "p".into(),
            wifi_ssid: None,
        }],
        aec_tail_ms: None,
        profile_version: None,
        auth_token: None,
    };
    send_gw(&mut wr, &hello).await;
    let _ack = lines.next_line().await.unwrap().unwrap();
    for _ in 0..40 {
        if state
            .hub
            .device_online_with_cap(device_id, "config_apply_v2")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (lines, wr)
}

/// Wave 2 Core P1-4: Multi-device `run_gateway_jobs` must:
/// 1. Cleanly skip unsupported blocks (`skipped_unsupported`) when online v2 devices
///    lack the requested block capability (`MG-P1-02` #4: never fail an Android-only
///    fleet on `audio_eq`/`led`).
/// 2. Isolate sibling failure (`MG-P1-02` #1): when `android-b` fails, healthy sibling
///    `android-a` is NEVER rolled back, and a follow-up `rev+1` forward apply uses a
///    strictly higher `block_generation`.
#[tokio::test]
async fn multi_device_worker_unsupported_block_and_monotonic_rollback_generation() {
    let (_dir, state) = make_app();
    let delivery = ConfigDelivery::for_app(&state);
    let enrolled = delivery.enroll("nas-core").unwrap();
    let hh = enrolled["household_id"].as_str().unwrap().to_string();

    // Connect two Android v2 gateways that support KWS but NOT audio_eq.
    let (mut lines_a, mut wr_a) = connect_v2_device_with_caps(
        &state,
        "android-a",
        vec!["audio_out", "config_apply_v2", "config_block_kws"],
    )
    .await;
    let (mut lines_b, mut wr_b) = connect_v2_device_with_caps(
        &state,
        "android-b",
        vec!["audio_out", "config_apply_v2", "config_block_kws"],
    )
    .await;
    let _worker = ConfigApplyCoordinator::start_for_app(&state);

    // 1. audio_eq apply when NEITHER online v2 gateway advertises config_block_audio_eq
    // must cleanly skip unsupported devices (`MG-P1-02` #4) without failing the plan.
    let eq_pkg = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "nas-core",
        Some("nas-core"),
        vec![Block {
            name: "audio_eq".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "audio_eq.curve".into(),
                value: json!("official"),
            }],
        }],
    );
    let eq_pre = delivery.preflight("admin", eq_pkg, json!({})).unwrap();
    let eq_app_id = "22222222-2222-4222-8222-222222222201";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: eq_pre["plan_id"].as_str().unwrap().into(),
                plan_hash: eq_pre["plan_hash"].as_str().unwrap().into(),
                expected_revision: eq_pre["expected_revision"].as_u64().unwrap(),
                application_id: eq_app_id.into(),
            },
        )
        .unwrap();
    let mut eq_rb = json!({});
    for _ in 0..80 {
        eq_rb = delivery.readback("admin", eq_app_id).unwrap();
        if eq_rb["publication"]["state"] == "failed" || eq_rb["publication"]["state"] == "applied" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(eq_rb["publication"]["state"], json!("applied"), "{eq_rb}");
    let eq_receipts = eq_rb["publication"]["gateway_receipt"]
        .as_array()
        .expect("gateway_receipt array");
    assert_eq!(eq_receipts.len(), 2, "{eq_rb}");
    assert!(
        eq_receipts
            .iter()
            .all(|r| r["state"] == "skipped_unsupported"),
        "{eq_rb}"
    );

    // 2. Multi-device mode.kws_threshold apply where one device fails -> rollback uses
    // strictly monotonic `rev*10+2`, and the follow-up `rev+1` forward apply (`(rev+1)*10+1`)
    // strictly exceeds the rollback generation and succeeds on both devices!
    let gens_a = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let gens_a_task = gens_a.clone();
    let gw_a = tokio::spawn(async move {
        let mut state_rev = 1u64;
        let mut last_gen = 0u64;
        let mut applies = 0usize;
        while let Ok(Some(line)) = tokio::time::timeout(Duration::from_secs(5), lines_a.next_line())
            .await
            .unwrap_or(Ok(None))
        {
            let Ok(frame) = serde_json::from_str::<CoreFrame>(&line) else {
                continue;
            };
            match frame {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr_a,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-a".into()),
                            state_revision: Some(state_rev),
                            capability_fingerprint: Some("fp-a".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr_a,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: true,
                            supported: true,
                            blockers: Vec::new(),
                            state: ConfigReceiptStatus::Validated,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-a".into()),
                            state_revision: Some(state_rev),
                            capability_fingerprint: None,
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    assert!(
                        block_generation > last_gen,
                        "android-a block_generation must strictly increase ({block_generation} <= {last_gen})"
                    );
                    last_gen = block_generation;
                    gens_a_task.lock().unwrap().push(block_generation);
                    state_rev += 1;
                    send_gw(
                        &mut wr_a,
                        &GatewayFrame::ConfigReceipt {
                            device: device.clone(),
                            core_node_id: core_node_id.clone(),
                            data_epoch,
                            application_id: application_id.clone(),
                            operation_id: operation_id.clone(),
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash: block_hash.clone(),
                            state: ConfigReceiptStatus::Persisted,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-a".into()),
                            state_revision: Some(state_rev),
                            persisted_hash: Some(block_hash.clone()),
                            active_hash: None,
                            active_values: None,
                            evidence: None,
                            ts: 102,
                        },
                    )
                    .await;
                    send_gw(
                        &mut wr_a,
                        &GatewayFrame::ConfigReceipt {
                            device,
                            core_node_id,
                            data_epoch,
                            application_id,
                            operation_id,
                            desired_revision,
                            block_generation,
                            block,
                            schema_version,
                            block_hash: block_hash.clone(),
                            state: ConfigReceiptStatus::Active,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-a".into()),
                            state_revision: Some(state_rev),
                            persisted_hash: Some(block_hash.clone()),
                            active_hash: Some(block_hash),
                            active_values: Some(values),
                            evidence: None,
                            ts: 103,
                        },
                    )
                    .await;
                    applies += 1;
                    if applies == 2 {
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    let gw_b = tokio::spawn(async move {
        let mut state_rev = 1u64;
        let mut fail_first = true;
        while let Ok(Some(line)) = tokio::time::timeout(Duration::from_secs(5), lines_b.next_line())
            .await
            .unwrap_or(Ok(None))
        {
            let Ok(frame) = serde_json::from_str::<CoreFrame>(&line) else {
                continue;
            };
            match frame {
                CoreFrame::ConfigStateGet { request_id, device } => {
                    send_gw(
                        &mut wr_b,
                        &GatewayFrame::ConfigState {
                            request_id: Some(request_id),
                            device,
                            gateway_boot_id: Some("boot-b".into()),
                            state_revision: Some(state_rev),
                            capability_fingerprint: Some("fp-b".into()),
                            blocks: vec![],
                            ts: 100,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigValidate {
                    request_id,
                    device,
                    block,
                    schema_version,
                    block_hash,
                    ..
                } => {
                    send_gw(
                        &mut wr_b,
                        &GatewayFrame::ConfigValidated {
                            request_id,
                            device,
                            block,
                            schema_version,
                            block_hash,
                            ok: true,
                            supported: true,
                            blockers: Vec::new(),
                            state: ConfigReceiptStatus::Validated,
                            reason: None,
                            retryable: false,
                            gateway_boot_id: Some("boot-b".into()),
                            state_revision: Some(state_rev),
                            capability_fingerprint: None,
                            evidence: None,
                            ts: 101,
                        },
                    )
                    .await;
                }
                CoreFrame::ConfigApply {
                    device,
                    core_node_id,
                    data_epoch,
                    application_id,
                    operation_id,
                    desired_revision,
                    block_generation,
                    block,
                    schema_version,
                    block_hash,
                    values,
                    ..
                } => {
                    if fail_first {
                        fail_first = false;
                        send_gw(
                            &mut wr_b,
                            &GatewayFrame::ConfigReceipt {
                                device,
                                core_node_id,
                                data_epoch,
                                application_id,
                                operation_id,
                                desired_revision,
                                block_generation,
                                block,
                                schema_version,
                                block_hash,
                                state: ConfigReceiptStatus::Failed,
                                reason: Some("simulated_fail".into()),
                                retryable: false,
                                gateway_boot_id: Some("boot-b".into()),
                                state_revision: Some(state_rev),
                                persisted_hash: None,
                                active_hash: None,
                                active_values: None,
                                evidence: None,
                                ts: 102,
                            },
                        )
                        .await;
                    } else {
                        state_rev += 1;
                        send_gw(
                            &mut wr_b,
                            &GatewayFrame::ConfigReceipt {
                                device: device.clone(),
                                core_node_id: core_node_id.clone(),
                                data_epoch,
                                application_id: application_id.clone(),
                                operation_id: operation_id.clone(),
                                desired_revision,
                                block_generation,
                                block,
                                schema_version,
                                block_hash: block_hash.clone(),
                                state: ConfigReceiptStatus::Persisted,
                                reason: None,
                                retryable: false,
                                gateway_boot_id: Some("boot-b".into()),
                                state_revision: Some(state_rev),
                                persisted_hash: Some(block_hash.clone()),
                                active_hash: None,
                                active_values: None,
                                evidence: None,
                                ts: 102,
                            },
                        )
                        .await;
                        send_gw(
                            &mut wr_b,
                            &GatewayFrame::ConfigReceipt {
                                device,
                                core_node_id,
                                data_epoch,
                                application_id,
                                operation_id,
                                desired_revision,
                                block_generation,
                                block,
                                schema_version,
                                block_hash: block_hash.clone(),
                                state: ConfigReceiptStatus::Active,
                                reason: None,
                                retryable: false,
                                gateway_boot_id: Some("boot-b".into()),
                                state_revision: Some(state_rev),
                                persisted_hash: Some(block_hash.clone()),
                                active_hash: Some(block_hash),
                                active_values: Some(values),
                                evidence: None,
                                ts: 103,
                            },
                        )
                        .await;
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    // Apply #1: android-a succeeds, android-b fails -> android-a receives rollback!
    let kws_pkg1 = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "nas-core",
        Some("nas-core"),
        vec![Block {
            name: "mode".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "mode.kws_threshold".into(),
                value: json!(0.24),
            }],
        }],
    );
    let pre1 = delivery.preflight("admin", kws_pkg1, json!({})).unwrap();
    let app1_id = "22222222-2222-4222-8222-222222222202";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: pre1["plan_id"].as_str().unwrap().into(),
                plan_hash: pre1["plan_hash"].as_str().unwrap().into(),
                expected_revision: pre1["expected_revision"].as_u64().unwrap(),
                application_id: app1_id.into(),
            },
        )
        .unwrap();
    let mut rb1 = json!({});
    for _ in 0..80 {
        rb1 = delivery.readback("admin", app1_id).unwrap();
        if rb1["publication"]["state"] == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(rb1["publication"]["state"], json!("failed"), "{rb1}");

    // Apply #2: next revision succeeds on BOTH devices with strictly higher generation!
    let kws_pkg2 = make_pkg(
        &hh,
        Purpose::NodeConfig,
        "nas-core",
        Some("nas-core"),
        vec![Block {
            name: "mode".into(),
            schema_version: 1,
            operations: vec![Operation::Set {
                path: "mode.kws_threshold".into(),
                value: json!(0.26),
            }],
        }],
    );
    let pre2 = delivery.preflight("admin", kws_pkg2, json!({})).unwrap();
    let app2_id = "22222222-2222-4222-8222-222222222203";
    delivery
        .commit(
            "admin",
            &Commit {
                plan_id: pre2["plan_id"].as_str().unwrap().into(),
                plan_hash: pre2["plan_hash"].as_str().unwrap().into(),
                expected_revision: pre2["expected_revision"].as_u64().unwrap(),
                application_id: app2_id.into(),
            },
        )
        .unwrap();
    let mut rb2 = json!({});
    for _ in 0..80 {
        rb2 = delivery.readback("admin", app2_id).unwrap();
        if rb2["publication"]["state"] == "applied" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(rb2["publication"]["state"], json!("applied"), "{rb2}");

    gw_a.await.unwrap();
    gw_b.await.unwrap();
    let recorded = gens_a.lock().unwrap().clone();
    assert_eq!(
        recorded.len(),
        2,
        "healthy sibling android-a must NOT be rolled back when android-b fails: expected rev1 forward + rev2 forward only: {recorded:?}"
    );
    assert!(
        recorded[0] < recorded[1],
        "block_generation must be strictly monotonic across revisions: {recorded:?}"
    );
}
