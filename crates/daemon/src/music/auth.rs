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
        let state = self.inner.lock().await;
        let Some(tokens) = state.tokens.clone() else {
            return Ok(None);
        };
        let now = now_unix_secs();
        if token_is_fresh(&tokens, now) {
            return Ok(Some(tokens));
        }
        drop(state);
        let refreshed = refresh_tokens(client_id, &tokens.refresh_token).await?;
        let new_tokens = merge_refreshed_tokens(&tokens, refreshed, now);
        self.store(new_tokens.clone()).await?;
        Ok(Some(new_tokens))
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn token_is_fresh(tokens: &StoredTokens, now: u64) -> bool {
    tokens.expires_at_secs > now + 60
}

fn merge_refreshed_tokens(prev: &StoredTokens, refreshed: TokenResponse, now: u64) -> StoredTokens {
    StoredTokens {
        access_token: refreshed.access_token,
        refresh_token: refreshed.refresh_token.unwrap_or_else(|| prev.refresh_token.clone()),
        expires_at_secs: now + refreshed.expires_in.unwrap_or(3600),
        scope: refreshed.scope.unwrap_or_else(|| prev.scope.clone()),
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
    refresh_tokens_at(SPOTIFY_TOKEN_URL, client_id, refresh_token).await
}

async fn refresh_tokens_at(
    token_url: &str,
    client_id: &str,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let client = reqwest::Client::new();
    let resp = client
        .post(token_url)
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
        anyhow::bail!("token refresh failed: {s} {body}");
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
    exchange_code_at(SPOTIFY_TOKEN_URL, client_id, code, verifier).await
}

async fn exchange_code_at(
    token_url: &str,
    client_id: &str,
    code: &str,
    verifier: &str,
) -> Result<StoredTokens> {
    let client = reqwest::Client::new();
    let resp = client
        .post(token_url)
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
        anyhow::bail!("code exchange failed: {s} {body}");
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
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

pub fn redirect_listen_addr() -> std::net::SocketAddr {
    "127.0.0.1:8765".parse().unwrap()
}

pub fn loopback_timeout() -> Duration {
    Duration::from_secs(180)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_for_matches_rfc7636_vector() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let expected = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert_eq!(challenge_for(verifier), expected);
    }

    #[test]
    fn random_verifier_is_url_safe_and_long() {
        let v = random_verifier();
        assert_eq!(v.len(), 86);
        assert!(
            v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "unexpected char in {v}"
        );
        assert_ne!(random_verifier(), random_verifier());
    }

    #[test]
    fn build_authorize_url_includes_pkce_params() {
        let url = build_authorize_url("my client", "CHALLENGE123", "state-xyz");
        assert!(url.starts_with(SPOTIFY_AUTHORIZE_URL));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("code_challenge=CHALLENGE123"));
        assert!(url.contains("state=state-xyz"));
        assert!(url.contains("client_id=my%20client"));
        assert!(url.contains("redirect_uri=http%3A%2F%2F127.0.0.1%3A8765%2Fcallback"));
        assert!(url.contains("scope=user-read-playback-state%20"));
    }

    #[test]
    fn urlencoded_preserves_unreserved_and_escapes_rest() {
        assert_eq!(urlencoded("abcXYZ09-_.~"), "abcXYZ09-_.~");
        assert_eq!(urlencoded("a b/c"), "a%20b%2Fc");
    }

    fn tokens(expires_at: u64) -> StoredTokens {
        StoredTokens {
            access_token: "a".into(),
            refresh_token: "r".into(),
            expires_at_secs: expires_at,
            scope: "s".into(),
        }
    }

    #[test]
    fn token_is_fresh_requires_sixty_second_margin() {
        assert!(token_is_fresh(&tokens(1000), 900));
        assert!(!token_is_fresh(&tokens(1000), 940));
        assert!(!token_is_fresh(&tokens(1000), 1000));
        assert!(!token_is_fresh(&tokens(1000), 2000));
    }

    #[test]
    fn merge_refreshed_keeps_previous_when_fields_absent() {
        let prev = tokens(100);
        let refreshed: TokenResponse = serde_json::from_value(serde_json::json!({
            "access_token": "new-access"
        }))
        .unwrap();
        let merged = merge_refreshed_tokens(&prev, refreshed, 5000);
        assert_eq!(merged.access_token, "new-access");
        assert_eq!(merged.refresh_token, "r");
        assert_eq!(merged.scope, "s");
        assert_eq!(merged.expires_at_secs, 5000 + 3600);
    }

    #[test]
    fn merge_refreshed_overrides_when_fields_present() {
        let prev = tokens(100);
        let refreshed: TokenResponse = serde_json::from_value(serde_json::json!({
            "access_token": "na",
            "refresh_token": "nr",
            "expires_in": 1000,
            "scope": "ns"
        }))
        .unwrap();
        let merged = merge_refreshed_tokens(&prev, refreshed, 5000);
        assert_eq!(merged.refresh_token, "nr");
        assert_eq!(merged.scope, "ns");
        assert_eq!(merged.expires_at_secs, 6000);
    }

    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn exchange_code_at_maps_token_json_to_stored_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/token"))
            .and(body_string_contains("grant_type=authorization_code"))
            .and(body_string_contains("code_verifier=ver"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ACCESS",
                "refresh_token": "REFRESH",
                "expires_in": 3600,
                "scope": "user-read-playback-state"
            })))
            .mount(&server)
            .await;

        let toks = exchange_code_at(&format!("{}/api/token", server.uri()), "cid", "the-code", "ver")
            .await
            .unwrap();
        assert_eq!(toks.access_token, "ACCESS");
        assert_eq!(toks.refresh_token, "REFRESH");
        assert_eq!(toks.scope, "user-read-playback-state");
    }

    #[tokio::test]
    async fn exchange_code_at_errors_without_refresh_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ACCESS",
                "expires_in": 3600
            })))
            .mount(&server)
            .await;

        let err = exchange_code_at(&format!("{}/api/token", server.uri()), "cid", "c", "v")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no refresh token"), "got: {err}");
    }

    #[tokio::test]
    async fn exchange_code_at_propagates_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/token"))
            .respond_with(ResponseTemplate::new(400).set_body_string("invalid_grant"))
            .mount(&server)
            .await;

        let err = exchange_code_at(&format!("{}/api/token", server.uri()), "cid", "c", "v")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("code exchange failed"), "got: {err}");
    }

    #[tokio::test]
    async fn refresh_tokens_at_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "FRESH",
                "expires_in": 1800
            })))
            .mount(&server)
            .await;

        let resp = refresh_tokens_at(&format!("{}/api/token", server.uri()), "cid", "rt")
            .await
            .unwrap();
        assert_eq!(resp.access_token, "FRESH");
        assert_eq!(resp.expires_in, Some(1800));
        assert_eq!(resp.refresh_token, None);
    }

    #[tokio::test]
    async fn refresh_tokens_at_propagates_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/token"))
            .respond_with(ResponseTemplate::new(401).set_body_string("expired"))
            .mount(&server)
            .await;

        let err = refresh_tokens_at(&format!("{}/api/token", server.uri()), "cid", "rt")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("token refresh failed"), "got: {err}");
    }
}
