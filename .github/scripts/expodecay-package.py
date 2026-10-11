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
    print(f"Downloading pinned artifact: {url}", flush=True)
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


def extract_executable(package, destination):
    found = False
    process = subprocess.Popen(["dpkg-deb", "--fsys-tarfile", str(package)], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as tar:
            for member in tar:
                if member.name.removeprefix("./") != "usr/bin/clickhouse":
                    continue
                if found or not member.isfile():
                    raise ValueError("Expected one regular clickhouse executable")
                with destination.open("wb") as output:
                    shutil.copyfileobj(tar.extractfile(member), output)
                found = True
        if process.wait() != 0 or not found:
            raise ValueError("Debian executable extraction failed")
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.terminate()
            process.wait()


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    WORK.mkdir(parents=True, exist_ok=False)
    assets, extracted = [], []
    for arch, deb_arch, family, machine, _, release_job in BUILDS:
        common = None
        for package in ("clickhouse-common-static",):
            name = f"{package}_{VERSION}_{deb_arch}.deb"
            target = WORK / name
            url = f"{BASE}/build_{family}_release/{name}"
            fetch(url, target)
            with target.open("rb") as source:
                if source.read(8) != b"!<arch>\n":
                    raise ValueError("Not a Debian archive")
            metadata = subprocess.check_output([
                "dpkg-deb", "--show", "--showformat=${Package} ${Version} ${Architecture}", str(target)
            ], text=True)
            if metadata != f"{package} {VERSION} {deb_arch}":
                raise ValueError(f"Unexpected Debian metadata: {metadata}")
            assets.append(dict(name=name, size=target.stat().st_size, sha256=digest(target),
                               source_url=url, build_variant=f"{family}_release", job_id=release_job))
            if package == "clickhouse-common-static":
                common = target
        binary = WORK / f"clickhouse-{arch}"
        extract_executable(common, binary)
        validate_elf(binary, machine)
        archive = OUT / f"clickhouse-expodecay-{VERSION}-linux-{arch}-release-{HEAD[:12]}.tar.gz"
        compress(binary, archive, Path("LICENSE"))
        record = dict(architecture=arch, source_package=common.name, source_package_sha256=digest(common),
                      member="usr/bin/clickhouse", raw_size=binary.stat().st_size, raw_sha256=digest(binary),
                      compressed_size=archive.stat().st_size, build_variant=f"{family}_release")
        record["archive"] = archive.name
        (OUT / (archive.name + ".sha256")).write_text(f"{digest(archive)}  {archive.name}\n")
        (OUT / (archive.name + ".raw.sha256")).write_text(f"{digest(binary)}  clickhouse\n")
        extracted.append(record)
    provenance = dict(upstream_head=HEAD, compiled_merge=MERGE, version=VERSION, upstream_run=RUN,
                      version_evidence="Upstream build logs and Debian metadata; executable not run",
                      packaging_commit=os.environ.get("GITHUB_SHA"),
                      packaging_run=os.environ.get("GITHUB_RUN_ID"), assets=assets, extracted=extracted)
    (OUT / "PROVENANCE.json").write_text(json.dumps(provenance, indent=2) + "\n")
    (OUT / "README.txt").write_text(
        f"Experimental ExpoDecay ClickHouse {VERSION}\nSource: {HEAD}\nCompiled merge: {MERGE}\n"
        "Standalone archives from matching amd_release/arm_release builds. Archives "
        "contain only the executable extracted from common-static Debian packages and its license. Input packages are not included.\n"
        "No downloaded executable was run. Brain compatibility is untested. ARM64 is Linux, not native Android/Bionic.\n"
        "SHA256SUMS are generated integrity evidence, not an independent upstream attestation.\n")
    shutil.copyfile("LICENSE", OUT / "LICENSE")
    (OUT / "SHA256SUMS").write_text("".join(f"{digest(path)}  {path.name}\n" for path in sorted(OUT.iterdir())))
    summary = "\n".join(f"- `{p.name}`: {p.stat().st_size:,} bytes" for p in sorted(OUT.iterdir()))
    print(json.dumps(provenance, indent=2), flush=True)
    print(summary, flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write("## Verified packages\n" + summary + "\n")


if __name__ == "__main__":
    main()
