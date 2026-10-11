"""Package pinned upstream artifacts without executing any downloaded binary."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tarfile

HEAD = "cf2fd6eb11d658ff0427d756ca9ab7b8572fa14a"
MERGE = "b50c0b5f3520aa4e873a1ef53f38ba2d36def745"
VERSION = "26.10.1.1"
TAG = "experimental-expodecay-cf2fd6eb11d6-20261011"
BASE = f"https://clickhouse-builds.s3.amazonaws.com/PRs/110934/{HEAD}/pr"
RUN = "https://github.com/ClickHouse/ClickHouse/actions/runs/37762511780"
BUILDS = (
    ("x86_64", "amd64", "amd", 62, 113277058004, 113299818461),
    ("aarch64", "arm64", "arm", 183, 113277058447, 113299818366),
)
OUT = Path("tmp/expodecay-release")
WORK = Path("tmp/expodecay-downloads")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def fetch(url, destination):
    # A failed/partial response must never become a release asset.
    partial = destination.with_suffix(destination.suffix + ".partial")
    try:
        subprocess.run([
            "curl", "--fail", "--location", "--silent", "--show-error",
            "--proto", "=https", "--proto-redir", "=https", "--retry", "2",
            "--connect-timeout", "30", "--max-time", "900",
            "--max-filesize", "2000000000", "--output", str(partial), url,
        ], check=True)
        if partial.stat().st_size == 0:
            raise ValueError(f"Empty artifact: {url}")
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)


def validate_elf(path, machine):
    with path.open("rb") as source:
        header = source.read(20)
    if (len(header) != 20 or header[:6] != b"\x7fELF\x02\x01"
            or struct.unpack_from("<H", header, 18)[0] != machine
            or path.stat().st_size < 1_000_000):
        raise ValueError(f"Not the expected Linux 64-bit ELF artifact: {path}")


def compress(binary, archive, license_file):
    # Fixed metadata makes the archive reproducible for fixed input bytes.
    with archive.open("wb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0, compresslevel=6) as gz:
            with tarfile.open(fileobj=gz, mode="w|") as tar:
                for path, name, mode in ((binary, "clickhouse", 0o755), (license_file, "LICENSE", 0o644)):
                    info = tarfile.TarInfo(name)
                    info.size = path.stat().st_size
                    info.mode = mode
                    info.mtime = info.uid = info.gid = 0
                    with path.open("rb") as source:
                        tar.addfile(info, source)
    subprocess.run(["gzip", "--test", str(archive)], check=True)
    with tarfile.open(archive, "r|gz") as tar:
        members = []
        for member in tar:
            members.append(member.name)
            if member.name not in ("clickhouse", "LICENSE") or not member.isfile():
                raise ValueError("Unexpected archive member")
            expected = binary if member.name == "clickhouse" else license_file
            source = tar.extractfile(member)
            if member.size != expected.stat().st_size or hashlib.file_digest(source, "sha256").hexdigest() != digest(expected):
                raise ValueError("Compression round-trip changed bytes")
        if members != ["clickhouse", "LICENSE"]:
            raise ValueError("Incomplete archive")


def main():
    # Fresh output only; reruns run on a clean runner and cannot mix stale files.
    OUT.mkdir(parents=True, exist_ok=False)
    WORK.mkdir(parents=True, exist_ok=False)
    artifacts = []
    for arch, deb_arch, family, machine, binary_job, package_job in BUILDS:
        name = f"clickhouse-expodecay-{VERSION}-linux-{arch}-{HEAD[:12]}"
        url = f"{BASE}/build_{family}_binary/clickhouse"
        binary = WORK / name
        fetch(url, binary)
        validate_elf(binary, machine)
        archive = OUT / (name + ".tar.gz")
        compress(binary, archive, Path("LICENSE"))
        raw_sha = digest(binary)
        (OUT / (name + ".raw.sha256")).write_text(f"{raw_sha}  clickhouse\n")
        (OUT / (name + ".tar.gz.sha256")).write_text(f"{digest(archive)}  {archive.name}\n")
        artifacts.append(dict(name=archive.name, kind="standalone", architecture=arch,
                              source_url=url, upstream_job=f"{RUN}/job/{binary_job}",
                              raw_size=binary.stat().st_size, raw_sha256=raw_sha,
                              size=archive.stat().st_size, sha256=digest(archive),
                              compression_round_trip="passed"))
        binary.unlink()
        for package in ("clickhouse-common-static", "clickhouse-client", "clickhouse-server"):
            package_name = f"{package}_{VERSION}_{deb_arch}.deb"
            url = f"{BASE}/build_{family}_release/{package_name}"
            destination = OUT / package_name
            fetch(url, destination)
            with destination.open("rb") as source:
                if source.read(8) != b"!<arch>\n":
                    raise ValueError(f"Invalid Debian archive: {package_name}")
            # Read package metadata only; no install scripts or binaries run.
            info = subprocess.check_output([
                "dpkg-deb", "--show", "--showformat=${Package}\t${Version}\t${Architecture}",
                str(destination),
            ], text=True).strip()
            if info != f"{package}\t{VERSION}\t{deb_arch}":
                raise ValueError(f"Unexpected Debian metadata: {info}")
            artifacts.append(dict(name=package_name, kind="deb", architecture=deb_arch,
                                  source_url=url, upstream_job=f"{RUN}/job/{package_job}",
                                  size=destination.stat().st_size, sha256=digest(destination)))
    provenance = dict(
        upstream_repository="ClickHouse/ClickHouse", upstream_pr=110934,
        upstream_head=HEAD, compiled_merge=MERGE, upstream_run=RUN,
        reported_version=VERSION, reported_revision=54515,
        version_evidence="upstream successful CI logs; binaries not executed here",
        packaging_commit=os.environ["GITHUB_SHA"],
        packaging_run=f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}",
        checksum_scope="Generated integrity hashes; no independent upstream raw-binary SHA-256 was available. Not an independent source/binary attestation.",
        artifacts=artifacts,
    )
    (OUT / "PROVENANCE.json").write_text(json.dumps(provenance, indent=2) + "\n")
    shutil.copyfile("LICENSE", OUT / "LICENSE")
    lines = [f"{digest(path)}  {path.name}" for path in sorted(OUT.iterdir())]
    (OUT / "SHA256SUMS").write_text("\n".join(lines) + "\n")
    notes = f"""Experimental ExpoDecay CI artifacts — {VERSION}

Repackaged from successful upstream PR https://github.com/ClickHouse/ClickHouse/pull/110934 builds.
PR head: `{HEAD}`. Actual compiled merge: `{MERGE}`.
Upstream run: {RUN}. Exact job URLs and original artifact URLs are in `PROVENANCE.json`.
Release tag points to PR source head; packaging commit is `{os.environ['GITHUB_SHA']}`.

Primary assets: gzip-compressed standalone Linux x86_64 and ARM64 binaries.
Each archive contains `clickhouse` and `LICENSE`; compression round-trip checked byte-for-byte by SHA-256 and size.
The ARM64 build is Linux/glibc software, suitable for evaluating in Debian/PRoot; it is NOT a native Android/Bionic build. PRoot compatibility is not tested here.
Matching client, server and common-static Debian packages are included unchanged from the same compiled revision's release builds. They are different build variants from the standalone binary jobs.
RPM, debug-symbol, Keeper, macOS and other distributions are omitted from this focused release.

`SHA256SUMS` checks release assets; `.raw.sha256` checks `clickhouse` after extraction, and `.tar.gz.sha256` checks the corresponding compressed archive.
These are generated integrity hashes, NOT independently verified upstream checksums or proof of reproducible compilation. No independent upstream raw-binary SHA-256 was found.
Version {VERSION}, revision 54515 comes from upstream CI logs. Packaging does not execute binaries, test Brain compatibility, or establish production readiness.
This is an unofficial experimental prerelease, not an official stable ClickHouse release.

| Asset | Bytes |
| --- | ---: |
"""
    notes += "\n".join(f"| `{p.name}` | {p.stat().st_size} |" for p in sorted(OUT.iterdir())) + "\n"
    Path("tmp/expodecay-release-notes.txt").write_text(notes)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(notes)
    print(json.dumps(provenance, indent=2), flush=True)


if __name__ == "__main__":
    main()
