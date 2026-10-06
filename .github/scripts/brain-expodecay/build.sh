#!/usr/bin/env bash
set -euo pipefail

mkdir -p build-release dist/package build-cache/sccache
exec >build-release/driver.log 2>&1

: "${SOURCE_BASE:?}" "${BUILDER_IMAGE:?}" "${TARGET_ARCH:?}" "${GITHUB_SHA:?}"
test "${GITHUB_REPOSITORY}" = vanchaklar/ClickHouse
test "${GITHUB_REF}" = refs/heads/build/brain-expodecay-2026-10-06
test "$(git rev-parse HEAD)" = "${GITHUB_SHA}"
if ! git cat-file -e "${SOURCE_BASE}^{commit}"; then
    git fetch --no-tags --depth=1 origin "${SOURCE_BASE}"
fi

# Keep every tracked source file identical to the reviewed base.
git diff --exit-code "${SOURCE_BASE}" HEAD -- . \
    ':(exclude).github/workflows/brain_expodecay_build.yml' \
    ':(exclude).github/scripts/brain-expodecay'
git diff --quiet
git submodule status --recursive >build-release/submodules.txt
if grep -Eq '^[+-U]' build-release/submodules.txt; then
    echo "Uninitialized, divergent, or conflicted submodules" >&2
    exit 1
fi

case "${TARGET_ARCH}" in
    x86_64) toolchain=x86_64; cpu_option=-DX86_ARCH_LEVEL=2; cpu_profile=x86-64-v2+pclmul ;;
    arm64) toolchain=aarch64; cpu_option=-DNO_ARMV81_OR_HIGHER=1; cpu_profile=armv8+crc ;;
    *) echo "Unsupported architecture" >&2; exit 1 ;;
esac
export cpu_profile
df -h >build-release/disk-before.txt
docker pull "${BUILDER_IMAGE}" >build-release/image-pull.log 2>&1
docker image inspect "${BUILDER_IMAGE}" >build-release/builder-image.json
docker run --rm \
    --env SCCACHE_DIR=/work/build-cache/sccache \
    --env SCCACHE_CACHE_SIZE=3G \
    --env GIT_CONFIG_COUNT=1 \
    --env GIT_CONFIG_KEY_0=safe.directory \
    --env GIT_CONFIG_VALUE_0=/work \
    --env TOOLCHAIN="${toolchain}" \
    --env CPU_OPTION="${cpu_option}" \
    --volume "${PWD}:/work" \
    --workdir /work \
    "${BUILDER_IMAGE}" \
    bash -euo pipefail -c '
        record_sccache_status()
        {
            local build_status=$?
            trap - EXIT
            if ! sccache --show-stats >build-release/sccache-stats.log 2>&1; then
                echo "Could not record sccache statistics; see sccache-stats.log" >&2
            fi
            exit "${build_status}"
        }
        trap record_sccache_status EXIT
        cmake --version >build-release/toolchain.log
        "${CC}" --version >>build-release/toolchain.log
        ninja --version >>build-release/toolchain.log
        cmake -S . -B build-release -G Ninja \
            -DCMAKE_TOOLCHAIN_FILE="cmake/linux/toolchain-${TOOLCHAIN}.cmake" \
            -DCMAKE_BUILD_TYPE=Release \
            -DENABLE_THINLTO=0 \
            -DENABLE_TESTS=0 \
            -DENABLE_UTILS=0 \
            -DENABLE_EXAMPLES=0 \
            -DENABLE_BENCHMARKS=0 \
            -DENABLE_CLICKHOUSE_SELF_EXTRACTING=0 \
            "${CPU_OPTION}" >build-release/configure.log 2>&1
        ninja -C build-release -t targets all >build-release/ninja-targets.txt
        preflight_targets=(
            src/CMakeFiles/dbms.dir/Columns/ColumnExponentialTimeDecaying.cpp.o
            src/CMakeFiles/dbms.dir/Interpreters/Aggregator.cpp.o
        )
        for target in "${preflight_targets[@]}"; do
            if ! grep -Fq "${target}:" build-release/ninja-targets.txt; then
                echo "Expected preflight target is missing: ${target}" >&2
                exit 1
            fi
        done
        cmake --build build-release --target "${preflight_targets[@]}" >build-release/preflight-compile.log 2>&1
        cmake --build build-release --target clickhouse >build-release/build.log 2>&1
    ' >build-release/container.log 2>&1

install -m 0755 build-release/programs/clickhouse dist/package/clickhouse
file dist/package/clickhouse >build-release/binary-file.txt
readelf -h dist/package/clickhouse >build-release/elf-header.txt
readelf --version-info dist/package/clickhouse >build-release/elf-versions.txt
case "${TARGET_ARCH}" in
    x86_64) grep -q 'Advanced Micro Devices X86-64' build-release/elf-header.txt ;;
    arm64) grep -q 'AArch64' build-release/elf-header.txt ;;
esac
cp build-release/submodules.txt dist/package/SUBMODULES.txt
cp LICENSE dist/package/LICENSE
python3 - <<'PY'
import json
import os
from pathlib import Path

metadata = {
    "repository": os.environ["GITHUB_REPOSITORY"],
    "source_base": os.environ["SOURCE_BASE"],
    "build_commit": os.environ["GITHUB_SHA"],
    "architecture": os.environ["TARGET_ARCH"],
    "cpu_profile": os.environ["cpu_profile"],
    "builder_image": os.environ["BUILDER_IMAGE"],
    "run_url": "https://github.com/" + os.environ["GITHUB_REPOSITORY"]
        + "/actions/runs/" + os.environ["GITHUB_RUN_ID"],
    "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
    "run_id": os.environ["GITHUB_RUN_ID"],
    "build_type": "Release",
    "native_smoke": "pending; see separate validation.json",
    "known_limitations": [
        "The existing 04613 overflow failure is unresolved.",
        "This focused smoke suite is not the full ClickHouse test suite.",
        "ARM64 is Linux/glibc with armv8+crc, not an Android/NDK executable.",
    ],
}
Path("dist/package/SOURCE.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY
cat >dist/package/README.txt <<'TXT'
Experimental ExpoDecay ClickHouse build; not an official ClickHouse release.
The source base, build commit, builder digest, and CPU profile are in SOURCE.json.
Verify SHA256SUMS before use. The executable supports local, client, and server.
Enable allow_experimental_time_decay_aggregate_functions for ExpoDecay queries.
Both compact 64-bit and 128-bit types are covered by the native smoke suite.
The existing 04613 overflow failure remains unresolved.
Final artifacts include validation.json and smoke-summary.json beside this archive.
ARM64 targets Linux/glibc, including a compatible Debian environment on a phone;
it does not target Android Bionic or the Android NDK. Device validation is separate.
TXT
(
    cd dist/package
    sha256sum clickhouse SOURCE.json SUBMODULES.txt LICENSE README.txt >SHA256SUMS
)
asset="clickhouse-expodecay-linux-${TARGET_ARCH}-${GITHUB_SHA:0:12}.tar.gz"
tar -C dist/package -czf "dist/${asset}" clickhouse SOURCE.json SUBMODULES.txt LICENSE README.txt SHA256SUMS
(
    cd dist
    sha256sum "${asset}" >"${asset}.sha256"
)
df -h >build-release/disk-after.txt
