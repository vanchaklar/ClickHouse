#!/usr/bin/env bash
set -euo pipefail

mkdir -p build-native-smoke/package
exec >build-native-smoke/validation.log 2>&1
: "${SOURCE_BASE:?}" "${BUILDER_IMAGE:?}" "${TARGET_ARCH:?}" "${GITHUB_SHA:?}"

actual_arch="$(uname -m)"
case "${TARGET_ARCH}:${actual_arch}" in
    x86_64:x86_64|arm64:aarch64) ;;
    *) echo "Native architecture mismatch: ${TARGET_ARCH} / ${actual_arch}" >&2; exit 1 ;;
esac
uname -a >build-native-smoke/host.txt
getconf GNU_LIBC_VERSION >>build-native-smoke/host.txt
asset="clickhouse-expodecay-linux-${TARGET_ARCH}-${GITHUB_SHA:0:12}.tar.gz"
(
    cd dist
    sha256sum --check "${asset}.sha256"
)
tar -xzf "dist/${asset}" -C build-native-smoke/package
(
    cd build-native-smoke/package
    sha256sum --check SHA256SUMS
)
python3 - <<'PY'
import json
import os
from pathlib import Path

meta = json.loads(Path("build-native-smoke/package/SOURCE.json").read_text())
for field, variable in (
    ("repository", "GITHUB_REPOSITORY"),
    ("source_base", "SOURCE_BASE"),
    ("build_commit", "GITHUB_SHA"),
    ("architecture", "TARGET_ARCH"),
    ("builder_image", "BUILDER_IMAGE"),
    ("run_id", "GITHUB_RUN_ID"),
):
    if meta[field] != os.environ[variable]:
        raise SystemExit(f"Source identity mismatch: {field}")
PY
chmod +x build-native-smoke/package/clickhouse
build-native-smoke/package/clickhouse local --version >build-native-smoke/version.txt 2>&1
python3 .github/scripts/brain-expodecay/smoke.py \
    --binary build-native-smoke/package/clickhouse \
    --output build-native-smoke
python3 - <<'PY'
import hashlib
import json
import os
import platform
from pathlib import Path

binary = Path("build-native-smoke/package/clickhouse")
source = json.loads(Path("build-native-smoke/package/SOURCE.json").read_text())
with binary.open("rb") as stream:
    digest = hashlib.file_digest(stream, "sha256").hexdigest()
report = {
    "status": "passed",
    "architecture": os.environ["TARGET_ARCH"],
    "native_machine": platform.machine(),
    "source_base": os.environ["SOURCE_BASE"],
    "build_commit": os.environ["GITHUB_SHA"],
    "build_attempt": source["run_attempt"],
    "validation_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
    "binary_sha256": digest,
    "smoke_summary": "smoke-summary.json",
    "scope": "Native ClickHouse local SQL smoke, not full regression or device validation",
    "unresolved": ["04613 overflow"],
}
Path("build-native-smoke/validation.json").write_text(json.dumps(report, indent=2) + "\n")
PY
