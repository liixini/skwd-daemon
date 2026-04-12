use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

static GEOCODE_CACHE: std::sync::LazyLock<StdMutex<HashMap<String, (f64, f64)>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

static WEATHER_CACHE: std::sync::LazyLock<StdMutex<HashMap<String, (Vec<String>, Instant)>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

const WEATHER_TTL_SECS: u64 = 1800;

fn wmo_to_conditions(code: i64) -> Vec<String> {
    match code {
        0 | 1 => vec!["clear".into(), "sunny".into()],
        45 | 48 => vec!["foggy".into()],
        51 | 53 | 55 | 61 | 63 | 65 | 80..=82 => vec!["rainy".into()],
        56 | 57 | 66 | 67 => vec!["rainy".into(), "snowy".into()],
        71 | 73 | 75 | 77 | 85 | 86 => vec!["snowy".into()],
        95 | 96 | 99 => vec!["stormy".into()],
        _ => vec!["cloudy".into()],
    }
}

async fn geocode(locale: &str) -> anyhow::Result<(f64, f64)> {
    let key = locale.to_lowercase();

    if let Some(coords) = GEOCODE_CACHE.lock().unwrap().get(&key).copied() {
        return Ok(coords);
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let resp: serde_json::Value = client
        .get("https://geocoding-api.open-meteo.com/v1/search")
        .query(&[("name", locale), ("count", "1"), ("language", "en"), ("format", "json")])
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("geocode request: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parse geocode response: {e}"))?;

    let results = resp
        .get("results")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("no geocoding results for '{locale}'"))?;

    let first = results
        .first()
        .ok_or_else(|| anyhow::anyhow!("no geocoding results for '{locale}'"))?;

    let lat = first
        .get("latitude")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| anyhow::anyhow!("missing latitude in geocode result"))?;
    let lon = first
        .get("longitude")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| anyhow::anyhow!("missing longitude in geocode result"))?;

    GEOCODE_CACHE.lock().unwrap().insert(key, (lat, lon));

    Ok((lat, lon))
}

pub async fn current_conditions(locale: &str) -> anyhow::Result<Vec<String>> {
    if locale.is_empty() {
        anyhow::bail!("locale is empty — set general.locale in config.json");
    }

    let key = locale.to_lowercase();

    {
        let cache = WEATHER_CACHE.lock().unwrap();
        if let Some((conditions, fetched_at)) = cache.get(&key)
            && fetched_at.elapsed().as_secs() < WEATHER_TTL_SECS
        {
            return Ok(conditions.clone());
        }
    }

    let (lat, lon) = geocode(locale).await?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let resp: serde_json::Value = client
        .get("https://api.open-meteo.com/v1/forecast")
        .query(&[
            ("latitude", lat.to_string()),
            ("longitude", lon.to_string()),
            ("current", "weather_code,wind_speed_10m".to_string()),
        ])
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("weather request: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parse weather response: {e}"))?;

    let current = resp
        .get("current")
        .ok_or_else(|| anyhow::anyhow!("missing 'current' in weather response"))?;

    let code = current
        .get("weather_code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let wind = current
        .get("wind_speed_10m")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);

    let mut conditions = wmo_to_conditions(code);

    if wind > 40.0 && !conditions.contains(&"windy".to_string()) {
        conditions.push("windy".into());
    }

    WEATHER_CACHE
        .lock()
        .unwrap()
        .insert(key, (conditions.clone(), Instant::now()));

    Ok(conditions)
}
