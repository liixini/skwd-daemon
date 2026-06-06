use std::ffi::OsString;
use std::path::{Path, PathBuf};

use tokio::process::Command;

pub fn output_path(input: &Path, suffix: &str, override_ext: Option<&str>) -> anyhow::Result<PathBuf> {
    let parent = input
        .parent()
        .ok_or_else(|| anyhow::anyhow!("input has no parent: {}", input.display()))?;
    let effects_dir = parent.join("effects");
    let stem = input
        .file_stem()
        .ok_or_else(|| anyhow::anyhow!("input has no stem: {}", input.display()))?
        .to_string_lossy()
        .into_owned();
    let ext = override_ext.map_or_else(|| input.extension().and_then(|e| e.to_str()).unwrap_or("jpg").to_owned(), str::to_owned);
    Ok(effects_dir.join(format!("{stem}-{suffix}.{ext}")))
}

#[allow(dead_code)]
pub async fn run_magick(input: &Path, output: &Path, args: Vec<OsString>) -> anyhow::Result<()> {
    if let Some(parent) = output.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut cmd = Command::new("magick");
    cmd.arg(input);
    cmd.args(&args);
    cmd.arg(output);

    let result = cmd.output().await?;
    if !result.status.success() {
        anyhow::bail!(
            "magick exited with status {:?}: {}",
            result.status.code(),
            String::from_utf8_lossy(&result.stderr).trim()
        );
    }
    Ok(())
}

#[allow(dead_code)]
pub async fn simple_effect(
    input: &Path,
    suffix: &str,
    override_ext: Option<&str>,
    args: Vec<OsString>,
) -> anyhow::Result<PathBuf> {
    let output = output_path(input, suffix, override_ext)?;
    run_magick(input, &output, args).await?;
    Ok(output)
}

pub fn os(s: impl AsRef<str>) -> OsString {
    OsString::from(s.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_path_places_file_in_effects_dir() {
        let out = output_path(Path::new("/photos/wall.png"), "blur", None).unwrap();
        assert_eq!(out, PathBuf::from("/photos/effects/wall-blur.png"));
    }

    #[test]
    fn output_path_honors_ext_override_and_default() {
        let out = output_path(Path::new("/photos/wall.png"), "tint", Some("webp")).unwrap();
        assert_eq!(out, PathBuf::from("/photos/effects/wall-tint.webp"));

        let out = output_path(Path::new("/photos/noext"), "x", None).unwrap();
        assert_eq!(out, PathBuf::from("/photos/effects/noext-x.jpg"));
    }

    #[test]
    fn output_path_errors_without_parent() {
        assert!(output_path(Path::new("/"), "x", None).is_err());
    }

    #[test]
    fn os_wraps_str_into_osstring() {
        assert_eq!(os("hello"), OsString::from("hello"));
    }
}
