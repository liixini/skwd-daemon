use anyhow::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

const SPOTIFY_AUTHORIZE_URL: &str = "https://accounts.spotify.com/authorize";
const SPOTIFY_TOKEN_URL: &str = "https://accounts.spotify.com/api/token";
const REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";

const SCOPES: &str = "user-read-playback-state user-modify-playback-state \
    user-read-currently-playing playlist-read-private playlist-read-collaborative \
    user-library-read user-library-modify user-read-recently-played \
    user-top-read streaming app-remote-control";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_secs: u64,
    pub scope: String,
}

#[derive(Debug, Default)]
pub struct AuthState {
    pub tokens: Option<StoredTokens>,
}

#[derive(Default)]
pub struct AuthStore {
    inner: Mutex<AuthState>,
}

impl AuthStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn load_from_disk(&self) {
        let Some(path) = token_path() else { return };
        let Ok(raw) = tokio::fs::read_to_string(&path).await else {
            return;
        };
        if let Ok(tokens) = serde_json::from_str::<StoredTokens>(&raw) {
            self.inner.lock().await.tokens = Some(tokens);
        }
    }

    pub async fn store(&self, tokens: StoredTokens) -> Result<()> {
        let Some(path) = token_path() else {
            anyhow::bail!("no XDG_DATA_HOME / HOME for token storage");
        };
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        let json = serde_json::to_string_pretty(&tokens)?;
        tokio::fs::write(&path, json).await?;
        self.inner.lock().await.tokens = Some(tokens);
        Ok(())
    }

    pub async fn clear(&self) {
        self.inner.lock().await.tokens = None;
        if let Some(path) = token_path() {
            tokio::fs::remove_file(&path).await.ok();
        }
    }

    pub async fn current(&self) -> Option<StoredTokens> {
        self.inner.lock().await.tokens.clone()
    }

    pub async fn ensure_fresh(&self, client_id: &str) -> Result<Option<StoredTokens>> {
        let mut state = self.inner.lock().await;
        let Some(tokens) = state.tokens.clone() else {
            return Ok(None);
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if tokens.expires_at_secs > now + 60 {
            return Ok(Some(tokens));
        }
        drop(state);
        let refreshed = refresh_tokens(client_id, &tokens.refresh_token).await?;
        let new_tokens = StoredTokens {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token.unwrap_or(tokens.refresh_token),
            expires_at_secs: now + refreshed.expires_in.unwrap_or(3600),
            scope: refreshed.scope.unwrap_or_else(|| tokens.scope.clone()),
        };
        self.store(new_tokens.clone()).await?;
        Ok(Some(new_tokens))
    }
}

fn token_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    Some(base.join("skwd").join("music").join("tokens.json"))
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
    scope: Option<String>,
}

async fn refresh_tokens(client_id: &str, refresh_token: &str) -> Result<TokenResponse> {
    let client = reqwest::Client::new();
    let resp = client
        .post(SPOTIFY_TOKEN_URL)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
        ])
        .send()
        .await
        .context("token refresh request")?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("token refresh failed: {} {}", s, body);
    }
    let body: TokenResponse = resp.json().await?;
    Ok(body)
}

pub fn random_verifier() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub fn challenge_for(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
}

pub fn build_authorize_url(client_id: &str, challenge: &str, state: &str) -> String {
    let scopes = SCOPES.replace(' ', "%20");
    format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&code_challenge_method=S256&code_challenge={}&state={}&scope={}",
        SPOTIFY_AUTHORIZE_URL,
        urlencoded(client_id),
        urlencoded(REDIRECT_URI),
        challenge,
        state,
        scopes,
    )
}

pub async fn exchange_code(
    client_id: &str,
    code: &str,
    verifier: &str,
) -> Result<StoredTokens> {
    let client = reqwest::Client::new();
    let resp = client
        .post(SPOTIFY_TOKEN_URL)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", REDIRECT_URI),
            ("client_id", client_id),
            ("code_verifier", verifier),
        ])
        .send()
        .await?;
    if !resp.status().is_success() {
        let s = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("code exchange failed: {} {}", s, body);
    }
    let body: TokenResponse = resp.json().await?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(StoredTokens {
        access_token: body.access_token,
        refresh_token: body
            .refresh_token
            .ok_or_else(|| anyhow::anyhow!("no refresh token in code exchange response"))?,
        expires_at_secs: now + body.expires_in.unwrap_or(3600),
        scope: body.scope.unwrap_or_default(),
    })
}

fn urlencoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

pub fn redirect_uri() -> &'static str {
    REDIRECT_URI
}

pub fn redirect_listen_addr() -> std::net::SocketAddr {
    "127.0.0.1:8765".parse().unwrap()
}

pub fn loopback_timeout() -> Duration {
    Duration::from_secs(180)
}
