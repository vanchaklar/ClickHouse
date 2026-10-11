"""Publish only the complete verified asset set; preserve existing releases."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.error
import urllib.request

REPO = "vanchaklar/ClickHouse"
HEAD = "cf2fd6eb11d658ff0427d756ca9ab7b8572fa14a"
TAG = "experimental-expodecay-cf2fd6eb11d6-20261011"
OUT = Path("tmp/expodecay-release")
TITLE = "Experimental ExpoDecay 26.10.1.1 — cf2fd6eb11d6"


def get_release():
    request = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/releases/tags/{TAG}",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"],
                 "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def gh(*args):
    subprocess.run(["gh", "release", *args, "--repo", REPO], check=True)


def main():
    if os.environ["GITHUB_REPOSITORY"] != REPO:
        raise ValueError("Wrong release repository")
    subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd=OUT, check=True)
    provenance = json.loads((OUT / "PROVENANCE.json").read_text())
    if provenance["upstream_head"] != HEAD:
        raise ValueError("Wrong artifact revision")
    release = get_release()
    if release is None:
        gh("create", TAG, "--target", HEAD, "--title", TITLE, "--prerelease", "--draft",
           "--notes-file", "tmp/expodecay-release-notes.txt")
        release = get_release()
    if not release or not release["draft"] or release["name"] != TITLE:
        raise ValueError("Refusing to overwrite an existing published or unrelated release")
    # Recover a partially uploaded draft only when existing assets match exactly.
    existing = {asset["name"]: asset for asset in release["assets"]}
    paths = sorted(OUT.iterdir())
    if set(existing) - {p.name for p in paths}:
        raise ValueError("Draft has unexpected assets; refusing to replace them")
    for path in paths:
        with path.open("rb") as source:
            checksum = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
        if path.name in existing:
            asset = existing[path.name]
            if asset.get("digest") != checksum or asset["size"] != path.stat().st_size:
                raise ValueError(f"Draft asset differs: {path.name}")
        else:
            gh("upload", TAG, str(path))
    actual = {asset["name"]: asset for asset in get_release()["assets"]}
    if set(actual) != {path.name for path in paths}:
        raise ValueError("Release asset inventory differs")
    for path in paths:
        with path.open("rb") as source:
            checksum = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
        if actual[path.name].get("digest") != checksum or actual[path.name]["size"] != path.stat().st_size:
            raise ValueError(f"Uploaded asset integrity differs: {path.name}")
    gh("edit", TAG, "--draft=false", "--prerelease", "--latest=false",
       "--notes-file", "tmp/expodecay-release-notes.txt")
    release = get_release()
    if release["draft"] or not release["prerelease"]:
        raise ValueError("Release was not published as a prerelease")
    print(release["html_url"], flush=True)


if __name__ == "__main__":
    main()
