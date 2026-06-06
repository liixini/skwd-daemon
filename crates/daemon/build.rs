use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

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
