use skwd_proto::{Request, Response};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread;

fn echo_server(listener: &UnixListener) {
    let (stream, _) = listener.accept().unwrap();
    let mut writer = stream.try_clone().unwrap();
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let line = line.unwrap();
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Request>(&line) {
            Ok(req) => Response::ok(req.id, serde_json::json!({ "method": req.method })),
            Err(e) => Response::err(0, -1, format!("parse error: {e}")),
        };
        let mut out = serde_json::to_string(&resp).unwrap();
        out.push('\n');
        writer.write_all(out.as_bytes()).unwrap();
    }
}

fn read_one(reader: &mut impl BufRead) -> Response {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.ends_with('\n'), "messages must be newline-terminated");
    serde_json::from_str(line.trim()).unwrap()
}

#[test]
fn newline_framed_request_response_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let server = thread::spawn(move || echo_server(&listener));

    let client = UnixStream::connect(&sock).unwrap();
    let mut writer = client.try_clone().unwrap();
    let mut reader = BufReader::new(client);

    for (id, method) in [(1u64, "wall.show"), (2, "lyrics.get")] {
        let req = Request {
            method: method.into(),
            params: serde_json::json!({}),
            id,
        };
        let mut wire = serde_json::to_string(&req).unwrap();
        wire.push('\n');
        writer.write_all(wire.as_bytes()).unwrap();

        let resp = read_one(&mut reader);
        assert_eq!(resp.id, id);
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap()["method"], method);
    }

    drop(writer);
    drop(reader);
    server.join().unwrap();
}

#[test]
fn blank_lines_are_skipped_between_messages() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let server = thread::spawn(move || echo_server(&listener));

    let client = UnixStream::connect(&sock).unwrap();
    let mut writer = client.try_clone().unwrap();
    let mut reader = BufReader::new(client);

    writer.write_all(b"\n\n").unwrap();
    let req = Request {
        method: "ping".into(),
        params: serde_json::json!({}),
        id: 9,
    };
    let mut wire = serde_json::to_string(&req).unwrap();
    wire.push('\n');
    writer.write_all(wire.as_bytes()).unwrap();

    let resp = read_one(&mut reader);
    assert_eq!(resp.id, 9);

    drop(writer);
    drop(reader);
    server.join().unwrap();
}

#[test]
fn malformed_json_yields_parse_error_response() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("daemon.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let server = thread::spawn(move || echo_server(&listener));

    let client = UnixStream::connect(&sock).unwrap();
    let mut writer = client.try_clone().unwrap();
    let mut reader = BufReader::new(client);

    writer.write_all(b"{ not json }\n").unwrap();
    let resp = read_one(&mut reader);
    assert_eq!(resp.id, 0);
    let err = resp.error.unwrap();
    assert_eq!(err.code, -1);
    assert!(err.message.starts_with("parse error"));

    drop(writer);
    drop(reader);
    server.join().unwrap();
}
