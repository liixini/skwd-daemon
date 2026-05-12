use std::process;

use skwd_proto::Request;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

mod gen_icons;
mod optimize_videos;

fn print_help() {
    println!("skwd - command-line interface for skwd-daemon\n");
    println!("usage: skwd <command> [json-params]");
    println!("       skwd help\n");

    println!("  skwd bar toggle");
    println!("  skwd bar show");
    println!("  skwd bar hide");
    println!("  skwd bar mouseover '{{\"state\":true}}'");
    println!("  skwd bar visualizer clean '{{\"state\":true}}'");
    println!("  skwd launcher toggle");
    println!("  skwd launcher show");
    println!("  skwd launcher hide");
    println!("  skwd settings toggle");
    println!("  skwd settings show");
    println!("  skwd settings hide");
    println!("  skwd power toggle");
    println!("  skwd power show");
    println!("  skwd power hide");
    println!("  skwd switch open");
    println!("  skwd switch next");
    println!("  skwd switch prev");
    println!("  skwd switch confirm");
    println!("  skwd switch cancel");
    println!("  skwd switch close");
    println!("  skwd status");
    println!("  skwd theme colors");
    println!("  skwd subscribe '{{\"events\":[\"skwd.*\"]}}'");
    println!("  skwd state get '{{\"key\":\"...\"}}'");
    println!("  skwd state set '{{\"key\":\"k\",\"value\":\"v\"}}'");
    println!("  skwd dev toggle");
    println!("  skwd dev enable");
    println!("  skwd dev disable");
    println!("  skwd dev status");
    println!("  skwd wall toggle");
    println!("  skwd wall show");
    println!("  skwd wall hide");
    println!("  skwd wall list");
    println!("  skwd wall import '{{\"path\":\"/abs/path/to/file.jpg\"}}'");
    println!("  skwd wall apply '{{\"name\":\"file.jpg\"}}'");
    println!("  skwd wall restore");
    println!("  skwd wall retheme");
    println!("  skwd wall theme_preview '{{\"name\":\"file.jpg\"}}'");
    println!("  skwd wall preheat");
    println!("  skwd wall cache_rebuild");
    println!("  skwd wall cache_status");
    println!("  skwd wall clear_data");
    println!("  skwd wall recompute_colors");
    println!("  skwd wall set_favourite '{{\"name\":\"file.jpg\",\"favourite\":true}}'");
    println!("  skwd wall update_analysis '{{\"name\":\"file.jpg\",\"tags\":[\"...\"]}}'");
    println!("  skwd wall update_metadata '{{\"name\":\"file.jpg\",\"...\":\"...\"}}'");
    println!("  skwd wall delete '{{\"name\":\"file.jpg\"}}'");
    println!("  skwd wall outputs");
    println!("  skwd wall set_audio '{{\"output\":\"DP-1\",\"enabled\":true}}'");
    println!("  skwd wall suppress");
    println!("  skwd wall unsuppress");
    println!("  skwd wall weather");
    println!("  skwd wall random_start");
    println!("  skwd wall random_stop");
    println!("  skwd wall random_status");
    println!("  skwd music status");
    println!("  skwd music devices");
    println!("  skwd music device set '{{\"id\":\"...\"}}'");
    println!("  skwd music player start");
    println!("  skwd music player stop");
    println!("  skwd music player play");
    println!("  skwd music player pause");
    println!("  skwd music player next");
    println!("  skwd music player previous");
    println!("  skwd music player volume '{{\"volume_percent\":50}}'");
    println!("  skwd music transfer '{{\"device_id\":\"...\"}}'");
    println!("  skwd music play uris '{{\"uris\":[\"spotify:track:...\"]}}'");
    println!("  skwd music play context '{{\"context_uri\":\"spotify:playlist:...\"}}'");
    println!("  skwd music queue");
    println!("  skwd music queue add '{{\"uri\":\"spotify:track:...\"}}'");
    println!("  skwd music search '{{\"q\":\"...\",\"type\":\"track\"}}'");
    println!("  skwd music playlists");
    println!("  skwd music playlist tracks '{{\"id\":\"...\"}}'");
    println!("  skwd music liked");
    println!("  skwd music like check '{{\"ids\":[\"...\"]}}'");
    println!("  skwd music like set '{{\"id\":\"...\",\"liked\":true}}'");
    println!("  skwd music artist top_tracks '{{\"id\":\"...\"}}'");
    println!("  skwd music auth start");
    println!("  skwd music auth status");
    println!("  skwd music auth logout");
    println!("  skwd lyrics peek");
    println!("  skwd lyrics get '{{\"track\":\"...\",\"artist\":\"...\"}}'");
    println!("  skwd lyrics fetch '{{\"track\":\"...\",\"artist\":\"...\"}}'");
    println!("  skwd steam download '{{\"id\":\"123456789\"}}'");
    println!("  skwd steam retry '{{\"id\":\"123456789\"}}'");
    println!("  skwd steam status");
    println!("  skwd optimize start '{{\"preset\":\"...\",\"paths\":[\"...\"]}}'");
    println!("  skwd optimize cancel");
    println!("  skwd optimize status");
    println!("  skwd optimize presets");
    println!("  skwd video_convert start '{{\"preset\":\"...\",\"paths\":[\"...\"]}}'");
    println!("  skwd video_convert cancel");
    println!("  skwd video_convert status");
    println!("  skwd video_convert presets");
    println!("  skwd analysis start");
    println!("  skwd analysis stop");
    println!("  skwd analysis status");
    println!("  skwd analysis regenerate");
    println!("  skwd analysis retag_one '{{\"name\":\"file.jpg\"}}'");
    println!("  skwd optimize-videos [DIR_OR_FILE...]");
    println!("  skwd gen-icons [--font PATH] [--output PATH]");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || matches!(args[0].as_str(), "help" | "--help" | "-h") {
        print_help();
        process::exit(if args.is_empty() { 1 } else { 0 });
    }

    if args[0] == "optimize-videos" {
        let code = optimize_videos::run(&args[1..]).await;
        process::exit(code);
    }

    if args[0] == "gen-icons" {
        let code = gen_icons::run(&args[1..]);
        process::exit(code);
    }

    let (method, params) = if args.len() >= 2 {
        let mut method_parts: Vec<String> = vec![args[0].clone(), args[1].clone()];
        let mut params = serde_json::Value::Object(serde_json::Map::new());
        for a in &args[2..] {
            let trimmed = a.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                match serde_json::from_str::<serde_json::Value>(a) {
                    Ok(v) => {
                        params = v;
                        break;
                    }
                    Err(e) => {
                        eprintln!("invalid JSON params: {e}");
                        process::exit(1);
                    }
                }
            }
            method_parts.push(a.clone());
        }
        (method_parts.join("."), params)
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
