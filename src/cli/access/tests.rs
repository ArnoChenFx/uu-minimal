use super::*;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify, RwLock};

#[test]
fn non_preemptive_flag_does_not_consume_positional_arguments() {
    for tail in [
        vec!["43425", "127.0.0.1", "3425"],
        vec!["127.0.0.1", "22"],
        vec!["user@host"],
    ] {
        for index in 0..=tail.len() {
            let mut items = vec!["--device".to_owned(), "synthetic-device".to_owned()];
            let mut positional = tail.clone();
            positional.insert(index, "--no-force-join");
            items.extend(positional.into_iter().map(str::to_owned));
            let args = Args::new(&items);
            assert!(args.flag("--no-force-join"));
            assert_eq!(args.require_device().unwrap(), "synthetic-device");
            assert_eq!(args.positionals(), tail);
            if tail.len() == 3 {
                assert_eq!(parse_portmap_rules(&args).unwrap(), vec![(43425, "127.0.0.1".into(), 3425)]);
            }
        }
    }
}

#[test]
fn sftp_passes_non_preemptive_mode_to_tunnel() {
    for force_join in [true, false] {
        let proxy = sftp_proxy(std::path::Path::new("uu-minimal"), "synthetic-device", force_join);
        let items: Vec<String> = proxy.split_whitespace().skip(2)
            .map(|item| item.replace("%h", "host").replace("%p", "22")).collect();
        let args = Args::new(&items);
        assert_eq!(!args.flag("--no-force-join"), force_join);
        assert_eq!(args.positionals(), ["host", "22"]);
        assert_eq!(args.require_device().unwrap(), "synthetic-device");
    }
}

// Synthetic HTTP failures let us inspect the real signed join requests without
// credentials, a cloud connection, or a successful WebRTC negotiation.
async fn rejected_joins() -> (Api, mpsc::Receiver<(Value, Instant)>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = Api::with_test_url(format!("http://{}", listener.local_addr().unwrap()));
    let (tx, rx) = mpsc::channel(8);
    let job = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (header_end, length) = loop {
                let mut chunk = [0u8; 1024];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0 && request.len() + n <= 16 * 1024);
                request.extend_from_slice(&chunk[..n]);
                if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    assert!(headers.starts_with("POST /api/v1/room/join/by_device/synthetic-device HTTP/1.1"));
                    assert!(headers.to_ascii_lowercase().contains("x-param-sign:"));
                    let length: usize = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                    }).unwrap();
                    break (end + 4, length);
                }
            };
            while request.len() < header_end + length {
                let mut chunk = [0u8; 1024];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0 && request.len() + n <= 16 * 1024);
                request.extend_from_slice(&chunk[..n]);
            }
            let body = serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
            let response = r#"{"code":9999}"#;
            socket.write_all(format!("HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            tx.send((body, Instant::now())).await.unwrap();
        }
    });
    (api, rx, job)
}

#[tokio::test]
async fn viewer_join_sends_selected_policy_without_forced_fallback() {
    let (api, mut requests, server) = rejected_joins().await;
    for options in [vec![], vec!["--no-force-join".to_owned()]] {
        let force_join = !Args::new(&options).flag("--no-force-join");
        let result = tokio::time::timeout(Duration::from_secs(2),
            viewer::join(&api, "synthetic-device", viewer::signal::CAPTURE_PORT_MAP, force_join)).await.unwrap();
        assert!(matches!(result, Err(error) if error == "join http 409 code 9999"));
        let (body, _) = requests.recv().await.unwrap();
        assert_eq!(body, json!({"force_join": force_join}));
        assert!(tokio::time::timeout(Duration::from_millis(50), requests.recv()).await.is_err());
    }
    server.abort();
}

#[tokio::test]
async fn portmap_retries_preserve_non_preemptive_policy_and_back_off() {
    let (api, mut requests, server) = rejected_joins().await;
    let connect = tokio::spawn(async move {
        connect_portmap(&api, "synthetic-device", &RwLock::new(None), &Notify::new(), false).await;
    });
    let (first, first_at) = tokio::time::timeout(Duration::from_secs(2), requests.recv()).await.unwrap().unwrap();
    let (second, second_at) = tokio::time::timeout(Duration::from_secs(2), requests.recv()).await.unwrap().unwrap();
    assert_eq!(first, json!({"force_join": false}));
    assert_eq!(second, first);
    assert!(second_at.duration_since(first_at) >= Duration::from_millis(450));
    assert!(tokio::time::timeout(Duration::from_millis(100), requests.recv()).await.is_err());
    connect.abort();
    server.abort();
}
