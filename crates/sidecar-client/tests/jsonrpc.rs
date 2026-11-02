//! JSON-RPC framing/correlation tests against the in-process MockSidecar
//! and a hand-scripted server for out-of-order/error edge cases.

use pds_sidecar_client::types::error_codes;
use pds_sidecar_client::types::TrashResult;
use pds_sidecar_client::{JsonRpcClient, MockSidecar, Notification, SidecarApi};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn call_response_round_trip() {
    let (client, _handle) = MockSidecar::new().connect();

    let root = client.get_root().await.unwrap();
    assert_eq!(root.uid, "root");
    assert!(root.is_folder());

    let children = client.list_children("root").await.unwrap();
    let names: Vec<_> = children.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, ["Documents", "hello.txt"]);
}

#[tokio::test]
async fn protocol_error_maps_code() {
    let (client, _handle) = MockSidecar::new().connect();
    // "Documents" already exists under root → createFolder must fail with 1005.
    let err = client.create_folder("root", "Documents").await.unwrap_err();
    assert_eq!(err.code(), Some(error_codes::CONFLICT));
}

/// Trash → list → restore round trip, plus per-node error results for
/// unknown uids (`drive.restore` mirrors `drive.trash`'s result shape).
#[tokio::test]
async fn restore_round_trip() {
    let (client, handle) = MockSidecar::new().connect();

    let uid = handle.find_uid("root", "hello.txt").unwrap();
    assert!(client.trash(std::slice::from_ref(&uid)).await.unwrap()[0].ok);
    assert_eq!(handle.child_names("root"), ["Documents"]);

    let trashed = client.list_trashed().await.unwrap();
    assert_eq!(trashed.len(), 1);
    assert_eq!(trashed[0].uid, uid);

    let results = client.restore(&[uid.clone(), "no-such".to_string()]).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], TrashResult { uid: uid.clone(), ok: true, error: None });
    assert!(!results[1].ok);
    assert!(results[1].error.is_some());

    // Restored node is visible as a child again.
    assert_eq!(handle.child_names("root"), ["Documents", "hello.txt"]);
    assert!(client.list_trashed().await.unwrap().is_empty());
    assert_eq!(handle.restored_uids(), vec![uid, "no-such".to_string()]);
}

#[tokio::test]
async fn concurrent_calls_are_correlated() {
    let (client, _handle) = MockSidecar::new().connect();
    let (a, b, c) = tokio::join!(
        client.list_children("root"),
        client.get_node("node-hello"),
        client.auth_status(),
    );
    assert_eq!(a.unwrap().len(), 2);
    assert_eq!(b.unwrap().name, "hello.txt");
    assert!(c.unwrap().logged_in);
}

#[tokio::test]
async fn notifications_are_dispatched() {
    let (mut client, _handle) = MockSidecar::new().connect();
    let mut rx = client.take_notifications();

    // events.start resolves, then a fast_forward drive.event arrives.
    client.events_start("scope-1", None).await.unwrap();
    match rx.recv().await.unwrap() {
        Notification::DriveEvent(ev) => {
            assert_eq!(ev.kind, pds_sidecar_client::EventKind::FastForward);
            assert_eq!(ev.scope_id, "scope-1");
        }
        other => panic!("expected drive.event, got {other:?}"),
    }
}

#[tokio::test]
async fn login_fork_emits_sign_in_url_then_resolves() {
    let (mut client, _handle) = MockSidecar::new().connect();
    let mut rx = client.take_notifications();

    let email = client.login_fork().await.unwrap();
    assert_eq!(email, "mock@proton.me");
    match rx.recv().await.unwrap() {
        Notification::AuthSignInUrl { url, user_code } => {
            assert!(url.contains("account.proton.me"));
            assert_eq!(user_code, "MOCK-CODE");
        }
        other => panic!("expected auth.signInUrl, got {other:?}"),
    }
}

/// Hand-scripted server: responds to requests out of order to prove the
/// client correlates by `id`, not arrival order.
#[tokio::test]
async fn out_of_order_responses_are_correlated() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (cr, cw) = tokio::io::split(client_io);
    let (sr, mut sw) = tokio::io::split(server_io);

    tokio::spawn(async move {
        let mut lines = BufReader::new(sr).lines();
        let mut pending = Vec::new();
        // Read two requests, then answer them in reverse id order.
        for _ in 0..2 {
            let line = lines.next_line().await.unwrap().unwrap();
            let msg: serde_json::Value = serde_json::from_str(&line).unwrap();
            pending.push((
                msg["id"].clone(),
                msg["method"].as_str().unwrap().to_string(),
            ));
        }
        pending.reverse();
        for (id, method) in pending {
            let resp = json!({"jsonrpc":"2.0","id":id,"result":{"echo": method}});
            sw.write_all(resp.to_string().as_bytes()).await.unwrap();
            sw.write_all(b"\n").await.unwrap();
            sw.flush().await.unwrap();
        }
    });

    let (rpc, _notifs) = JsonRpcClient::new(cr, cw);
    let (r1, r2) = tokio::join!(rpc.call("first", json!({})), rpc.call("second", json!({})));
    assert_eq!(r1.unwrap()["echo"], "first");
    assert_eq!(r2.unwrap()["echo"], "second");
}

/// Server closing the connection must fail pending and future calls.
#[tokio::test]
async fn closed_connection_fails_calls() {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (cr, cw) = tokio::io::split(client_io);
    drop(server_io); // close immediately

    let (rpc, _notifs) = JsonRpcClient::new(cr, cw);
    let err = rpc.call("anything", json!({})).await.unwrap_err();
    assert!(matches!(
        err,
        pds_sidecar_client::Error::Closed | pds_sidecar_client::Error::Rpc(_)
    ));
}
