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

const GEOCODE_BASE: &str = "https://geocoding-api.open-meteo.com";
const FORECAST_BASE: &str = "https://api.open-meteo.com";

async fn geocode_with(locale: &str, base: &str) -> anyhow::Result<(f64, f64)> {
    let key = locale.to_lowercase();

    if let Some(coords) = GEOCODE_CACHE.lock().unwrap().get(&key).copied() {
        return Ok(coords);
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let resp: serde_json::Value = client
        .get(format!("{base}/v1/search"))
        .query(&[("name", locale), ("count", "1"), ("language", "en"), ("format", "json")])
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("geocode request: {e}"))?
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("parse geocode response: {e}"))?;

    let (lat, lon) = parse_geocode_response(&resp, locale)?;

    GEOCODE_CACHE.lock().unwrap().insert(key, (lat, lon));

    Ok((lat, lon))
}

fn parse_geocode_response(resp: &serde_json::Value, locale: &str) -> anyhow::Result<(f64, f64)> {
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
    Ok((lat, lon))
}

pub async fn current_conditions(locale: &str) -> anyhow::Result<Vec<String>> {
    current_conditions_with(locale, GEOCODE_BASE, FORECAST_BASE).await
}

async fn current_conditions_with(
    locale: &str,
    geo_base: &str,
    fc_base: &str,
) -> anyhow::Result<Vec<String>> {
    if locale.is_empty() {
        anyhow::bail!("locale is empty - set general.locale in config.json");
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

    let (lat, lon) = geocode_with(locale, geo_base).await?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let resp: serde_json::Value = client
        .get(format!("{fc_base}/v1/forecast"))
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

    let conditions = parse_weather_response(&resp)?;

    WEATHER_CACHE
        .lock()
        .unwrap()
        .insert(key, (conditions.clone(), Instant::now()));

    Ok(conditions)
}

fn parse_weather_response(resp: &serde_json::Value) -> anyhow::Result<Vec<String>> {
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
    Ok(conditions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wmo_codes_map_to_expected_conditions() {
        assert_eq!(wmo_to_conditions(0), vec!["clear".to_string(), "sunny".to_string()]);
        assert_eq!(wmo_to_conditions(1), vec!["clear".to_string(), "sunny".to_string()]);
    }

    #[test]
    fn parse_geocode_response_extracts_first_lat_lon() {
        let resp = serde_json::json!({
            "results": [
                { "latitude": 59.33, "longitude": 18.06 },
                { "latitude": 1.0, "longitude": 2.0 }
            ]
        });
        assert_eq!(parse_geocode_response(&resp, "stockholm").unwrap(), (59.33, 18.06));
    }

    #[test]
    fn parse_geocode_response_errors_on_empty_or_missing() {
        assert!(parse_geocode_response(&serde_json::json!({}), "x").is_err());
        assert!(parse_geocode_response(&serde_json::json!({ "results": [] }), "x").is_err());
        assert!(
            parse_geocode_response(&serde_json::json!({ "results": [{ "latitude": 1.0 }] }), "x")
                .is_err()
        );
    }

    #[test]
    fn parse_weather_response_maps_code_and_appends_windy() {
        let calm = serde_json::json!({ "current": { "weather_code": 61, "wind_speed_10m": 5.0 } });
        assert_eq!(parse_weather_response(&calm).unwrap(), vec!["rainy".to_string()]);

        let windy = serde_json::json!({ "current": { "weather_code": 0, "wind_speed_10m": 55.0 } });
        let got = parse_weather_response(&windy).unwrap();
        assert!(got.contains(&"windy".to_string()));
        assert!(got.contains(&"clear".to_string()));
    }

    #[test]
    fn parse_weather_response_errors_without_current() {
        assert!(parse_weather_response(&serde_json::json!({})).is_err());
        assert_eq!(wmo_to_conditions(45), vec!["foggy".to_string()]);
        assert_eq!(wmo_to_conditions(61), vec!["rainy".to_string()]);
        assert_eq!(wmo_to_conditions(81), vec!["rainy".to_string()]);
        assert_eq!(wmo_to_conditions(66), vec!["rainy".to_string(), "snowy".to_string()]);
        assert_eq!(wmo_to_conditions(75), vec!["snowy".to_string()]);
        assert_eq!(wmo_to_conditions(95), vec!["stormy".to_string()]);
        assert_eq!(wmo_to_conditions(3), vec!["cloudy".to_string()]);
        assert_eq!(wmo_to_conditions(12345), vec!["cloudy".to_string()]);
    }

    #[tokio::test]
    async fn current_conditions_with_chains_geocode_then_forecast() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [{ "latitude": 59.33, "longitude": 18.06 }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/forecast"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "current": { "weather_code": 71, "wind_speed_10m": 50.0 }
            })))
            .mount(&server)
            .await;

        let conditions =
            current_conditions_with("wiremock-weather-locale-xyz", &server.uri(), &server.uri())
                .await
                .unwrap();
        assert!(conditions.contains(&"snowy".to_string()));
        assert!(conditions.contains(&"windy".to_string()));
    }
}
