use std::os::unix::process::CommandExt;

pub fn start(limit_mb: u64, check_interval_secs: u64) {
    let argv: Vec<String> = std::env::args().collect();
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(_) => return,
    };
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(check_interval_secs));
            let rss_kb = match read_self_rss_kb() {
                Some(v) => v,
                None => continue,
            };
            if rss_kb / 1024 < limit_mb {
                continue;
            }
            tracing::warn!(
                rss_mb = rss_kb / 1024,
                limit_mb,
                "RSS exceeded threshold - re-execing"
            );
            let mut cmd = std::process::Command::new(&exe);
            if argv.len() > 1 {
                cmd.args(&argv[1..]);
            }
            let err = cmd.exec();
            tracing::error!(?err, "exec failed");
            std::process::exit(1);
        }
    });
}

fn read_self_rss_kb() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let v = rest.split_whitespace().next()?;
            return v.parse().ok();
        }
    }
    None
}
