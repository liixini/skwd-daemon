#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_script="$root/packaging/build-ffmpeg-runtime.sh"
link_script="$root/packaging/link-private-ffmpeg.sh"
test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

failures=0

expect_failure() {
    local expected_status="$1"
    local expected_message="$2"
    shift 2

    local output status
    set +e
    output=$("$@" 2>&1)
    status=$?
    set -e
    if [[ "$status" -ne "$expected_status" || "$output" != *"$expected_message"* ]]; then
        printf 'expected failure %s containing %q, got status %s:\n%s\n' \
            "$expected_status" "$expected_message" "$status" "$output" >&2
        failures=$((failures + 1))
    fi
}

expect_failure 2 "usage:" "$build_script"
expect_failure 2 "usage:" "$build_script" /tmp/one /tmp/two
expect_failure 2 "must be absolute" "$build_script" relative
expect_failure 2 "unsafe install prefix" "$build_script" /
expect_failure 2 "must not contain whitespace" "$build_script" "$test_root/with space"
expect_failure 2 "must be normalized" "$build_script" "$test_root/parent/../prefix"

mkdir "$test_root/nonempty"
printf 'preserve me\n' > "$test_root/nonempty/sentinel"
expect_failure 2 "must be empty" "$build_script" "$test_root/nonempty"
grep -qx 'preserve me' "$test_root/nonempty/sentinel"

printf 'not a directory\n' > "$test_root/file-prefix"
expect_failure 2 "not a directory" "$build_script" "$test_root/file-prefix"
ln -s "$test_root/nonempty" "$test_root/symlink-prefix"
expect_failure 2 "unsafe install prefix" "$build_script" "$test_root/symlink-prefix"

mkdir "$test_root/jobs-prefix"
expect_failure 2 "between 1 and 64" env SKWD_FFMPEG_JOBS=0 "$build_script" "$test_root/jobs-prefix"
expect_failure 2 "between 1 and 64" env SKWD_FFMPEG_JOBS=65 "$build_script" "$test_root/jobs-prefix"
expect_failure 2 "between 1 and 64" env SKWD_FFMPEG_JOBS='1 --eval=bad' "$build_script" "$test_root/jobs-prefix"

mkdir "$test_root/no-tools-prefix"
expect_failure 127 "required build tool is unavailable" env PATH=/nonexistent /usr/bin/bash "$build_script" "$test_root/no-tools-prefix"

fake_bin="$test_root/fake-bin"
mkdir "$fake_bin" "$test_root/download-prefix" "$test_root/tmp"
curl_args="$test_root/curl-args"
cat > "$fake_bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$SKWD_TEST_CURL_ARGS"
output=
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--output" ]]; then
        output="$2"
        shift 2
    else
        shift
    fi
done
printf 'corrupt archive\n' > "$output"
EOF
chmod 755 "$fake_bin/curl"
set +e
PATH="$fake_bin:$PATH" SKWD_TEST_CURL_ARGS="$curl_args" TMPDIR="$test_root/tmp" \
    "$build_script" "$test_root/download-prefix" >/dev/null 2>&1
download_status=$?
set -e
if [[ "$download_status" -eq 0 ]]; then
    echo "corrupt FFmpeg download was accepted" >&2
    failures=$((failures + 1))
fi
grep -Fx -- '--fail' "$curl_args" >/dev/null
grep -Fx -- '--location' "$curl_args" >/dev/null
grep -Fx -- '--proto' "$curl_args" >/dev/null
grep -Fx -- '=https' "$curl_args" >/dev/null
grep -Fx -- '--tlsv1.2' "$curl_args" >/dev/null
if find "$test_root/tmp" -mindepth 1 -print -quit | grep -q .; then
    echo "temporary build directory survived failed checksum validation" >&2
    failures=$((failures + 1))
fi

expect_failure 1 "SKWD_FFMPEG_PREFIX is required" env -u SKWD_FFMPEG_PREFIX "$link_script"
expect_failure 2 "must be absolute" env SKWD_FFMPEG_PREFIX=relative "$link_script"
expect_failure 2 "unsafe SKWD_FFMPEG_PREFIX" env SKWD_FFMPEG_PREFIX=/ "$link_script"

runtime="$test_root/runtime"
mkdir -p "$runtime/lib"
for library in libavcodec.so.63 libavformat.so.63 libavutil.so.61 libswresample.so.7 libswscale.so.10; do
    printf 'fixture\n' > "$runtime/lib/$library"
done
missing_runtime="$test_root/missing-runtime"
cp -a "$runtime" "$missing_runtime"
rm "$missing_runtime/lib/libavcodec.so.63"
expect_failure 1 "missing libavcodec.so.63" env SKWD_FFMPEG_PREFIX="$missing_runtime" "$link_script"
ln -s "$runtime" "$test_root/runtime-link"
expect_failure 2 "unsafe SKWD_FFMPEG_PREFIX" env SKWD_FFMPEG_PREFIX="$test_root/runtime-link" "$link_script"

cc_args="$test_root/cc-args"
cat > "$fake_bin/cc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$#" "$@" > "$SKWD_TEST_CC_ARGS"
EOF
chmod 755 "$fake_bin/cc"
PATH="$fake_bin:$PATH" SKWD_TEST_CC_ARGS="$cc_args" SKWD_FFMPEG_PREFIX="$runtime" \
    "$link_script" '-Wl,--as-needed' 'argument with spaces' '*literal*'
test "$(sed -n '1p' "$cc_args")" = 4
test "$(sed -n '2p' "$cc_args")" = "-L$runtime/lib"
test "$(sed -n '3p' "$cc_args")" = '-Wl,--as-needed'
test "$(sed -n '4p' "$cc_args")" = 'argument with spaces'
test "$(sed -n '5p' "$cc_args")" = '*literal*'

if [[ "$failures" -ne 0 ]]; then
    echo "$failures adversarial FFmpeg runtime script test(s) failed" >&2
    exit 1
fi
echo "adversarial FFmpeg runtime script tests passed"
