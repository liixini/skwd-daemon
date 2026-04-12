use std::process;

use skwd_proto::Request;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        eprintln!("usage: skwd <namespace> <method> [json-params]");
        eprintln!("examples:");
        eprintln!("  skwd wall toggle");
        eprintln!("  skwd wall apply '{{\"name\":\"sunset.jpg\"}}'");
        eprintln!("  skwd status");
        process::exit(1);
    }

    let (method, params) = if args.len() >= 2 {
        let method = format!("{}.{}", args[0], args[1]);
        let params = if args.len() >= 3 {
            match serde_json::from_str(&args[2]) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("invalid JSON params: {e}");
                    process::exit(1);
                }
            }
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };
        (method, params)
    } else {
        (args[0].clone(), serde_json::Value::Object(serde_json::Map::new()))
    };

    let sock_path = skwd_proto::socket_path();
    let stream = match UnixStream::connect(&sock_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot connect to daemon at {}: {e}", sock_path.display());
            eprintln!("is skwd-daemon running?");
            process::exit(1);
        }
    };

    let (reader, mut writer) = stream.into_split();

    let req = Request { method, params, id: 1 };
    let line = serde_json::to_string(&req).unwrap();
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.write_all(b"\n").await.unwrap();

    let mut reader = BufReader::new(reader);
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();

    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&response) {
        if let Some(err) = val.get("error") {
            eprintln!(
                "error: {}",
                err.get("message").and_then(|m| m.as_str()).unwrap_or("unknown")
            );
            process::exit(1);
        }
        if let Some(result) = val.get("result") {
            println!("{}", serde_json::to_string_pretty(result).unwrap());
        }
    } else {
        print!("{response}");
    }
}
