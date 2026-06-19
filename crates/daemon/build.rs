use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    emit_version();

    let target = std::env::var("TARGET").unwrap_or_default();
    if !target.contains("linux") {
        return;
    }

    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let target_dir = match out_dir.ancestors().nth(3) {
        Some(p) => p.to_path_buf(),
        None => return,
    };

    let build_dir = target_dir.join("build");
    let Ok(entries) = std::fs::read_dir(&build_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("steamworks-sys-") {
            continue;
        }
        let so = entry.path().join("out").join("libsteam_api.so");
        if !so.exists() {
            continue;
        }
        let dest = target_dir.join("libsteam_api.so");
        if let Err(e) = std::fs::copy(&so, &dest) {
            println!("cargo:warning=failed to copy libsteam_api.so: {e}");
        } else {
            println!("cargo:rerun-if-changed={}", so.display());
        }
        return;
    }
}

fn emit_version() {
    let version = git_version()
        .unwrap_or_else(|| std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".into()));
    println!("cargo:rustc-env=SKWD_VERSION={version}");
    for p in ["../../.git/HEAD", "../../.git/index"] {
        if std::path::Path::new(p).exists() {
            println!("cargo:rerun-if-changed={p}");
        }
    }
}

fn git_version() -> Option<String> {
    let run = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git").args(args).output().ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
        if s.is_empty() { None } else { Some(s) }
    };
    let count = run(&["rev-list", "--count", "HEAD"])?;
    let hash = run(&["rev-parse", "--short", "HEAD"])?;
    Some(format!("r{count}.{hash}"))
}
