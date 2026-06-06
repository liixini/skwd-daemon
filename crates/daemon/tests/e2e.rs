use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Daemon {
    child: Child,
    _dir: tempfile::TempDir,
    sock: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_daemon() -> Daemon {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let runtime = root.join("run");
    std::fs::create_dir_all(&runtime).unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_skwd-daemon"))
        .env("SKWD_HEADLESS", "1")
        .env("HOME", &root)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_DATA_HOME", root.join("data"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn skwd-daemon");

    let sock = runtime.join("skwd").join("daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !sock.exists() {
        assert!(Instant::now() < deadline, "daemon socket never appeared at {}", sock.display());
        std::thread::sleep(Duration::from_millis(50));
    }
    Daemon { child, _dir: dir, sock }
}

fn connect(sock: &PathBuf) -> UnixStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match UnixStream::connect(sock) {
            Ok(s) => return s,
            Err(e) => {
                assert!(Instant::now() < deadline, "could not connect to daemon: {e}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn rpc(writer: &mut UnixStream, reader: &mut BufReader<UnixStream>, line: &str) -> serde_json::Value {
    writer.write_all(line.as_bytes()).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
    loop {
        let mut resp = String::new();
        let n = reader.read_line(&mut resp).unwrap();
        assert!(n > 0, "daemon closed connection before responding to {line:?}");
        let v: serde_json::Value =
            serde_json::from_str(&resp).unwrap_or_else(|e| panic!("bad response {resp:?}: {e}"));
        if v.get("event").is_some() && v.get("id").is_none() {
            continue;
        }
        return v;
    }
}

#[test]
fn e2e_daemon_answers_json_rpc_over_socket() {
    let d = start_daemon();
    let stream = connect(&d.sock);
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);

    let status = rpc(&mut writer, &mut reader, r#"{"method":"status","id":1}"#);
    assert_eq!(status["id"], 1);
    assert_eq!(status["result"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(status["result"]["current_wallpaper"].is_null());

    let set = rpc(
        &mut writer,
        &mut reader,
        r#"{"method":"state.set","id":2,"params":{"key":"e2e","value":"hello"}}"#,
    );
    assert!(set["error"].is_null());

    let get = rpc(
        &mut writer,
        &mut reader,
        r#"{"method":"state.get","id":3,"params":{"key":"e2e"}}"#,
    );
    assert_eq!(get["result"]["value"], "hello");

    let list = rpc(&mut writer, &mut reader, r#"{"method":"wall.list","id":4}"#);
    assert_eq!(list["result"]["count"], 0);
    assert!(list["result"]["wallpapers"].is_array());

    let sub = rpc(
        &mut writer,
        &mut reader,
        r#"{"method":"subscribe","id":5,"params":{"events":["skwd.wall.*"]}}"#,
    );
    assert!(sub["error"].is_null());

    let unknown = rpc(&mut writer, &mut reader, r#"{"method":"does.not.exist","id":6}"#);
    assert_eq!(unknown["error"]["code"], -32601);

    let malformed = rpc(&mut writer, &mut reader, "{not valid json");
    assert_eq!(malformed["id"], 0);
    assert_eq!(malformed["error"]["code"], -1);
}

#[test]
fn e2e_daemon_persists_state_across_reconnect() {
    let d = start_daemon();

    {
        let stream = connect(&d.sock);
        let mut writer = stream.try_clone().unwrap();
        let mut reader = BufReader::new(stream);
        let set = rpc(
            &mut writer,
            &mut reader,
            r#"{"method":"state.set","id":1,"params":{"key":"persist","value":"yes"}}"#,
        );
        assert!(set["error"].is_null());
    }

    let stream = connect(&d.sock);
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let get = rpc(
        &mut writer,
        &mut reader,
        r#"{"method":"state.get","id":2,"params":{"key":"persist"}}"#,
    );
    assert_eq!(get["result"]["value"], "yes");
}
