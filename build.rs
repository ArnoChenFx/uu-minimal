//! Bake release version into the binary (matches GitHub tag / CI release job).

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=UU_MINIMAL_VERSION");

    let version = std::env::var("UU_MINIMAL_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| git_exact_tag_version())
        .unwrap_or_else(|| {
            std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string())
        });

    println!("cargo:rustc-env=UU_MINIMAL_VERSION={version}");
}

fn git_exact_tag_version() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["describe", "--tags", "--exact-match", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let tag = String::from_utf8(output.stdout).ok()?.trim().to_string();
    tag.strip_prefix('v').map(str::to_owned).or(Some(tag))
}
