use anyhow::{Context, Result};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;

const BASE: &str = "https://api.spotify.com/v1";

pub struct ApiClient {
    http: reqwest::Client,
    token: String,
}

impl ApiClient {
    pub fn new(token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            token,
        }
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.token)
    }

    async fn get_json(&self, url: &str) -> Result<Value> {
        let resp = self
            .http
            .get(url)
            .header(AUTHORIZATION, self.auth_header())
            .send()
            .await
            .context("GET request")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {} -> {} {}", url, status, body);
        }
        Ok(resp.json::<Value>().await?)
    }

    async fn put_json(&self, url: &str, body: Value) -> Result<Value> {
        let resp = self
            .http
            .put(url)
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("PUT request")?;
        let status = resp.status();
        if status.as_u16() == 204 {
            return Ok(Value::Null);
        }
        if !status.is_success() {
            let s = resp.text().await.unwrap_or_default();
            anyhow::bail!("PUT {} -> {} {}", url, status, s);
        }
        let text = resp.text().await.unwrap_or_default();
        if text.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        let resp = self
            .http
            .post(url)
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("POST request")?;
        let status = resp.status();
        if status.as_u16() == 204 {
            return Ok(Value::Null);
        }
        if !status.is_success() {
            let s = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {} -> {} {}", url, status, s);
        }
        let text = resp.text().await.unwrap_or_default();
        if text.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn delete_json(&self, url: &str, body: Value) -> Result<Value> {
        let resp = self
            .http
            .delete(url)
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("DELETE request")?;
        let status = resp.status();
        if status.as_u16() == 204 {
            return Ok(Value::Null);
        }
        if !status.is_success() {
            let s = resp.text().await.unwrap_or_default();
            anyhow::bail!("DELETE {} -> {} {}", url, status, s);
        }
        let text = resp.text().await.unwrap_or_default();
        if text.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    pub async fn devices(&self) -> Result<Value> {
        self.get_json(&format!("{}/me/player/devices", BASE)).await
    }

    pub async fn currently_playing(&self) -> Result<Value> {
        let resp = self
            .http
            .get(format!("{}/me/player/currently-playing", BASE))
            .header(AUTHORIZATION, self.auth_header())
            .send()
            .await?;
        if resp.status().as_u16() == 204 {
            return Ok(Value::Null);
        }
        if !resp.status().is_success() {
            let s = resp.status();
            let b = resp.text().await.unwrap_or_default();
            anyhow::bail!("currently-playing {} {}", s, b);
        }
        Ok(resp.json::<Value>().await.unwrap_or(Value::Null))
    }

    pub async fn transfer_playback(&self, device_id: &str, play: bool) -> Result<()> {
        self.put_json(
            &format!("{}/me/player", BASE),
            serde_json::json!({"device_ids":[device_id], "play": play}),
        )
        .await?;
        Ok(())
    }

    pub async fn play_uris(
        &self,
        uris: &[String],
        device_id: Option<&str>,
    ) -> Result<()> {
        let mut url = format!("{}/me/player/play", BASE);
        if let Some(d) = device_id {
            url.push_str(&format!("?device_id={}", urlencode(d)));
        }
        self.put_json(&url, serde_json::json!({"uris": uris})).await?;
        Ok(())
    }

    pub async fn play_context(
        &self,
        context_uri: &str,
        offset_uri: Option<&str>,
        device_id: Option<&str>,
    ) -> Result<()> {
        let mut url = format!("{}/me/player/play", BASE);
        if let Some(d) = device_id {
            url.push_str(&format!("?device_id={}", urlencode(d)));
        }
        let body = if let Some(off) = offset_uri {
            serde_json::json!({"context_uri": context_uri, "offset": {"uri": off}})
        } else {
            serde_json::json!({"context_uri": context_uri})
        };
        self.put_json(&url, body).await?;
        Ok(())
    }

    pub async fn user_playlists(&self) -> Result<Value> {
        self.get_json(&format!("{}/me/playlists?limit=50", BASE)).await
    }

    pub async fn playlist_tracks(&self, playlist_id: &str) -> Result<Value> {
        self.get_json(&format!(
            "{}/playlists/{}/tracks?limit=100",
            BASE,
            urlencode(playlist_id)
        ))
        .await
    }

    pub async fn search(&self, query: &str, types: &[&str], limit: u32) -> Result<Value> {
        let url = format!(
            "{}/search?q={}&type={}&limit={}",
            BASE,
            urlencode(query),
            types.join(","),
            limit,
        );
        self.get_json(&url).await
    }

    pub async fn check_liked(&self, ids: &[String]) -> Result<Value> {
        let url = format!(
            "{}/me/tracks/contains?ids={}",
            BASE,
            urlencode(&ids.join(","))
        );
        self.get_json(&url).await
    }

    pub async fn like(&self, ids: &[String]) -> Result<()> {
        self.put_json(
            &format!("{}/me/tracks", BASE),
            serde_json::json!({"ids": ids}),
        )
        .await?;
        Ok(())
    }

    pub async fn unlike(&self, ids: &[String]) -> Result<()> {
        self.delete_json(
            &format!("{}/me/tracks", BASE),
            serde_json::json!({"ids": ids}),
        )
        .await?;
        Ok(())
    }

    pub async fn add_to_queue(&self, uri: &str, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/queue?uri={}", BASE, urlencode(uri));
        if let Some(d) = device_id {
            url.push_str(&format!("&device_id={}", urlencode(d)));
        }
        self.post_json(&url, Value::Null).await?;
        Ok(())
    }

    pub async fn liked_songs(&self) -> Result<Value> {
        self.get_json(&format!("{}/me/tracks?limit=50", BASE)).await
    }

    pub async fn queue(&self) -> Result<Value> {
        self.get_json(&format!("{}/me/player/queue", BASE)).await
    }

    pub async fn pause(&self, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/pause", BASE);
        if let Some(d) = device_id { url.push_str(&format!("?device_id={}", urlencode(d))); }
        self.put_json(&url, Value::Null).await?;
        Ok(())
    }

    pub async fn play_resume(&self, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/play", BASE);
        if let Some(d) = device_id { url.push_str(&format!("?device_id={}", urlencode(d))); }
        self.put_json(&url, serde_json::json!({})).await?;
        Ok(())
    }

    pub async fn skip_next(&self, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/next", BASE);
        if let Some(d) = device_id { url.push_str(&format!("?device_id={}", urlencode(d))); }
        self.post_json(&url, Value::Null).await?;
        Ok(())
    }

    pub async fn skip_previous(&self, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/previous", BASE);
        if let Some(d) = device_id { url.push_str(&format!("?device_id={}", urlencode(d))); }
        self.post_json(&url, Value::Null).await?;
        Ok(())
    }

    pub async fn set_volume(&self, percent: u8, device_id: Option<&str>) -> Result<()> {
        let mut url = format!("{}/me/player/volume?volume_percent={}", BASE, percent);
        if let Some(d) = device_id { url.push_str(&format!("&device_id={}", urlencode(d))); }
        self.put_json(&url, Value::Null).await?;
        Ok(())
    }

    pub async fn artist_top_tracks(&self, artist_id: &str, market: Option<&str>) -> Result<Value> {
        let mut url = format!("{}/artists/{}/top-tracks", BASE, urlencode(artist_id));
        if let Some(m) = market {
            if !m.is_empty() {
                url.push_str(&format!("?market={}", urlencode(m)));
            }
        }
        self.get_json(&url).await
    }
}

fn urlencode(s: &str) -> String {
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
