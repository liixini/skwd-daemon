use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{info, warn};

use super::auth::{exchange_code, redirect_listen_addr, StoredTokens};
use super::state::MusicState;

pub async fn run_one_shot(music: Arc<MusicState>) -> Result<()> {
    let listener = TcpListener::bind(redirect_listen_addr()).await?;
    let (done_tx, mut done_rx) = oneshot::channel::<()>();
    let done_tx = Arc::new(tokio::sync::Mutex::new(Some(done_tx)));

    info!(
        "music: oauth loopback listening on {}",
        redirect_listen_addr()
    );

    let timeout = tokio::time::sleep(super::auth::loopback_timeout());
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                warn!("music: oauth timeout, closing loopback");
                break;
            }
            _ = &mut done_rx => { break; }
            accept = listener.accept() => {
                match accept {
                    Ok((mut socket, _)) => {
                        let music = music.clone();
                        let done_tx = done_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(&mut socket, &music, &done_tx).await {
                                warn!("oauth loopback conn error: {e:#}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("oauth accept error: {e:#}");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_conn(
    socket: &mut tokio::net::TcpStream,
    music: &MusicState,
    done_tx: &Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 4096];
    let n = socket.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or_default();
    let path = first_line.split_whitespace().nth(1).unwrap_or_default();

    let (status, body) = if path.starts_with("/callback") {
        let qs = path.split_once('?').map(|(_, q)| q).unwrap_or("");
        let mut code = None;
        let mut state = None;
        let mut err = None;
        for kv in qs.split('&') {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            let v = url_decode(v);
            match k {
                "code" => code = Some(v),
                "state" => state = Some(v),
                "error" => err = Some(v),
                _ => {}
            }
        }
        if let Some(e) = err {
            (
                "400 Bad Request",
                format!("<html><body>Spotify returned error: {e}. You can close this tab.</body></html>"),
            )
        } else if code.is_some() && state.is_some() {
            let pending = music.pending_oauth.lock().await.clone();
            match pending {
                Some(p) if Some(&p.state) == state.as_ref() => {
                    match exchange_code(&p.client_id, &code.unwrap(), &p.verifier).await {
                        Ok(tokens) => {
                            if let Err(e) = music.auth.store(tokens.clone()).await {
                                warn!("token persist failed: {e:#}");
                            }
                            broadcast_auth_done(music, &tokens).await;
                            *music.pending_oauth.lock().await = None;
                            if let Some(tx) = done_tx.lock().await.take() {
                                let _ = tx.send(());
                            }
                            (
                                "200 OK",
                                "<html><body><h2>Authentication complete</h2><p>You can close this tab.</p></body></html>".to_string(),
                            )
                        }
                        Err(e) => {
                            warn!("code exchange failed: {e:#}");
                            (
                                "500 Internal Server Error",
                                format!("<html><body>Token exchange failed: {e}</body></html>"),
                            )
                        }
                    }
                }
                _ => (
                    "400 Bad Request",
                    "<html><body>state mismatch or no pending oauth</body></html>".to_string(),
                ),
            }
        } else {
            (
                "400 Bad Request",
                "<html><body>missing code/state</body></html>".to_string(),
            )
        }
    } else {
        (
            "404 Not Found",
            "<html><body>not found</body></html>".to_string(),
        )
    };

    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.shutdown().await.ok();
    Ok(())
}

async fn broadcast_auth_done(music: &MusicState, tokens: &StoredTokens) {
    let payload = serde_json::json!({
        "event": "skwd.music.auth.done",
        "data": {
            "authenticated": true,
            "expires_at_secs": tokens.expires_at_secs,
        }
    });
    let _ = music.event_tx.send(payload.to_string());
}

fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
                out.push('%');
                i += 1;
            }
            b'+' => {
                out.push(' ');
                i += 1;
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}
