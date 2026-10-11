"""Upload verified assets only to an existing owner-published prerelease."""
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


def get_resource(resource):
    request = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/{resource}",
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
    release = get_resource(f"releases/tags/{TAG}")
    if release is None:
        print("Owner-created release is not present; verified Actions artifact retained.", flush=True)
        return
    if release["draft"] or not release["prerelease"]:
        raise ValueError("Expected an owner-published prerelease")
    ref = get_resource(f"git/ref/tags/{TAG}")
    if not ref:
        raise ValueError("Release tag missing")
    obj = ref["object"]
    for _ in range(5):
        if obj["type"] != "tag":
            break
        obj = get_resource(f"git/tags/{obj['sha']}")["object"]
    if obj["type"] != "commit" or obj["sha"] != HEAD:
        raise ValueError("Release tag does not resolve to the pinned source")
    existing = {asset["name"]: asset for asset in release["assets"]}
    paths = sorted(OUT.iterdir())
    for path in paths:
        with path.open("rb") as source:
            checksum = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
        if path.name in existing:
            asset = existing[path.name]
            if asset.get("digest") != checksum or asset["size"] != path.stat().st_size:
                raise ValueError(f"Draft asset differs: {path.name}")
        else:
            gh("upload", TAG, str(path))
    actual = {asset["name"]: asset for asset in get_resource(f"releases/tags/{TAG}")["assets"]}
    if not {path.name for path in paths}.issubset(actual):
        raise ValueError("Release asset inventory differs")
    for path in paths:
        with path.open("rb") as source:
            checksum = "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()
        if actual[path.name].get("digest") != checksum or actual[path.name]["size"] != path.stat().st_size:
            raise ValueError(f"Uploaded asset integrity differs: {path.name}")
    print(release["html_url"], flush=True)


if __name__ == "__main__":
    main()
