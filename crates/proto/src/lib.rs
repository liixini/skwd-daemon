use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

pub fn socket_path() -> PathBuf {
    let runtime_dir = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(runtime_dir).join("skwd").join("daemon.sock")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Response(Response),
    Event(Event),
}

impl Request {
    pub fn str_param<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.params.get(key).and_then(|v| v.as_str()).unwrap_or(default)
    }

    pub fn opt_str(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(|v| v.as_str())
    }

    pub fn bool_param(&self, key: &str, default: bool) -> bool {
        self.params
            .get(key)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(default)
    }

    pub fn opt_i64(&self, key: &str) -> Option<i64> {
        self.params.get(key).and_then(serde_json::Value::as_i64)
    }
}

impl Response {
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: u64, code: i32, message: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(ErrorInfo {
                code,
                message: message.into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(params: serde_json::Value) -> Request {
        Request {
            method: "x".into(),
            params,
            id: 1,
        }
    }

    #[test]
    fn str_param_returns_value_then_default() {
        let r = req(json!({ "name": "hello" }));
        assert_eq!(r.str_param("name", "fallback"), "hello");
        assert_eq!(r.str_param("missing", "fallback"), "fallback");
    }

    #[test]
    fn str_param_falls_back_on_wrong_type() {
        let r = req(json!({ "name": 42 }));
        assert_eq!(r.str_param("name", "fallback"), "fallback");
    }

    #[test]
    fn opt_str_distinguishes_present_absent_and_wrong_type() {
        let r = req(json!({ "name": "v", "n": 3 }));
        assert_eq!(r.opt_str("name"), Some("v"));
        assert_eq!(r.opt_str("missing"), None);
        assert_eq!(r.opt_str("n"), None);
    }

    #[test]
    fn bool_param_uses_default_when_absent_or_wrong_type() {
        let r = req(json!({ "flag": true, "notbool": "true" }));
        assert!(r.bool_param("flag", false));
        assert!(r.bool_param("missing", true));
        assert!(!r.bool_param("missing", false));
        assert!(r.bool_param("notbool", true));
    }

    #[test]
    fn opt_i64_parses_only_integers() {
        let r = req(json!({ "n": 7, "s": "9" }));
        assert_eq!(r.opt_i64("n"), Some(7));
        assert_eq!(r.opt_i64("s"), None);
        assert_eq!(r.opt_i64("missing"), None);
    }

    #[test]
    fn request_defaults_params_and_id() {
        let r: Request = serde_json::from_str(r#"{"method":"ping"}"#).unwrap();
        assert_eq!(r.method, "ping");
        assert_eq!(r.id, 0);
        assert!(r.params.is_null());
    }

    #[test]
    fn ok_response_omits_error_key() {
        let s = serde_json::to_string(&Response::ok(5, json!({ "a": 1 }))).unwrap();
        assert!(s.contains("\"result\""));
        assert!(!s.contains("\"error\""));
        assert!(s.contains("\"id\":5"));
    }

    #[test]
    fn err_response_omits_result_key() {
        let s = serde_json::to_string(&Response::err(5, -1, "boom")).unwrap();
        assert!(s.contains("\"error\""));
        assert!(!s.contains("\"result\""));
        assert!(s.contains("boom"));
    }

    #[test]
    fn server_message_untagged_disambiguates() {
        let resp = serde_json::to_string(&Response::ok(1, json!(null))).unwrap();
        match serde_json::from_str::<ServerMessage>(&resp).unwrap() {
            ServerMessage::Response(r) => assert_eq!(r.id, 1),
            ServerMessage::Event(_) => panic!("expected response"),
        }

        let ev = serde_json::to_string(&Event {
            event: "cache.item".into(),
            data: json!({ "k": "v" }),
        })
        .unwrap();
        match serde_json::from_str::<ServerMessage>(&ev).unwrap() {
            ServerMessage::Event(e) => assert_eq!(e.event, "cache.item"),
            ServerMessage::Response(_) => panic!("expected event"),
        }
    }

    #[test]
    fn socket_path_derives_from_runtime_dir() {
        let p = socket_path();
        assert!(p.ends_with("skwd/daemon.sock"));
        let base = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
        assert_eq!(p, PathBuf::from(base).join("skwd").join("daemon.sock"));
    }
}
