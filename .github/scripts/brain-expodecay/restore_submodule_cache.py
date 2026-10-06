#!/usr/bin/env python3
"""Restore a pinned ClickHouse submodule object cache without cached Git config.

Requires Python 3.10+, Git, zstd, a normal fresh checkout, and a downloaded
archive. Does not use credentials or the network. Failures are fatal. Existing
modules are refused; there is no clone fallback.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile


def git_environment():
    env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null',
               GIT_TERMINAL_PROMPT='0', GIT_ALLOW_PROTOCOL='', GIT_NO_REPLACE_OBJECTS='1')
    return env


def git(repo, *arguments, env=None):
    command = ['git', '-C', str(repo), '-c', 'core.hooksPath=/dev/null',
               '-c', 'core.fsmonitor=false', '-c', 'protocol.allow=never', *arguments]
    return subprocess.run(command, env=env or git_environment(), check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def archive_stream(path):
    proc = subprocess.Popen(['zstd', '-dc', str(path)], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    return proc, tarfile.open(fileobj=proc.stdout, mode='r|')


def finish_stream(proc, archive):
    archive.close()
    while proc.stdout.read(1048576):
        pass
    proc.stdout.close()
    error = proc.stderr.read()
    if proc.wait():
        raise RuntimeError('Archive decompression/checksum failed: '+error.decode(errors='replace')[:200])


def member_path(member):
    path = PurePosixPath(member.name)
    if path.is_absolute() or '..' in path.parts or path.parts[:2] != ('.git', 'modules'):
        raise ValueError('Cache member escapes .git/modules')
    if not (member.isdir() or member.isfile()) or member.size < 0:
        raise ValueError('Cache includes a link, special file, or invalid size')
    return path


def selected_member(path, links):
    parts = path.parts
    if len(parts) < 5 or '/'.join(parts[2:4]) not in links:
        return False
    suffix = '/'.join(parts[4:])
    # Never import hooks, config, refs, index, alternates, or replacement refs.
    return (suffix == 'shallow'
            or bool(re.fullmatch(r'objects/[0-9a-f]{2}/[0-9a-f]{38}', suffix))
            or bool(re.fullmatch(r'objects/pack/pack-[0-9a-f]{40}\.(pack|idx|rev|bitmap)', suffix)))


def restore(args):
    repo = args.repo.resolve(strict=True)
    if not (repo / '.git').is_dir() or (repo / '.git').is_symlink():
        raise ValueError('A normal checkout with a real .git directory is required')
    modules = repo / '.git/modules'
    if modules.exists() or modules.is_symlink():
        raise ValueError('Refusing to overwrite existing .git/modules')
    links = {}
    for record in git(repo, 'ls-files', '--stage', '-z').split(b'\0'):
        if not record:
            continue
        metadata, raw_path = record.split(b'\t', 1)
        mode, sha, stage = metadata.decode('ascii').split()
        if mode != '160000':
            continue
        path = raw_path.decode('utf-8')
        if stage != '0' or not re.fullmatch(r'contrib/[A-Za-z0-9_.+-]+', path):
            raise ValueError('Unexpected/unmerged submodule path')
        if not re.fullmatch(r'[0-9a-f]{40}', sha):
            raise ValueError('Invalid submodule commit')
        links[path] = sha
    if not links:
        raise ValueError('No submodules found')
    digest_text = '\n'.join(links[path] for path in sorted(links, key=lambda item: item.encode()))
    actual_key = hashlib.sha256(digest_text.encode()).hexdigest()[:16]
    if actual_key != args.cache_key:
        raise ValueError(f'Checkout submodules require key {actual_key}, not {args.cache_key}')
    # Git submodule names must match cache directories for this repository.
    paths = git(repo, 'config', '--file', '.gitmodules', '--get-regexp', r'^submodule\..*\.path$')
    declared = {}
    for line in paths.decode().splitlines():
        key, value = line.split(None, 1)
        name = key.removeprefix('submodule.').removesuffix('.path')
        if name != value:
            raise ValueError('Submodule name and path differ')
        declared[value] = name
    if set(declared) != set(links):
        raise ValueError('.gitmodules does not match gitlinks')
    digest = hashlib.sha256()
    with args.archive.open('rb') as handle:
        for chunk in iter(lambda: handle.read(8388608), b''):
            digest.update(chunk)
    if digest.hexdigest() != args.sha256:
        raise ValueError('Archive SHA256 mismatch')
    names = set()
    expanded = 0
    selected = 0
    proc, archive = archive_stream(args.archive)
    try:
        for member in archive:
            path = member_path(member)
            if str(path) in names:
                raise ValueError('Duplicate archive member')
            names.add(str(path))
            expanded += member.size
            if expanded > args.max_expanded_gib * 1024**3:
                raise ValueError('Archive expansion limit exceeded')
            if member.isfile() and selected_member(path, links):
                selected += member.size
        finish_stream(proc, archive)
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    if selected + 1024**3 > shutil.disk_usage(repo).free:
        raise ValueError('Insufficient disk for cache plus 1 GiB reserve')
    staging = Path(tempfile.mkdtemp(prefix='submodule-cache-', dir=repo / '.git'))
    try:
        proc, archive = archive_stream(args.archive)
        try:
            for member in archive:
                path = member_path(member)
                if not member.isfile() or not selected_member(path, links):
                    continue
                target = staging.joinpath(*path.parts[2:])
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open('xb') as output, archive.extractfile(member) as source:
                    shutil.copyfileobj(source, output, length=1048576)
                target.chmod(0o600)
            finish_stream(proc, archive)
        except BaseException:
            proc.kill()
            proc.wait()
            raise
        for path, sha in links.items():
            directory = staging / path
            (directory / 'refs').mkdir(parents=True, exist_ok=True)
            (directory / 'objects/info').mkdir(parents=True, exist_ok=True)
            (directory / 'objects/pack').mkdir(parents=True, exist_ok=True)
            shallow = directory / 'shallow'
            if shallow.exists() and any(not re.fullmatch(r'[0-9a-f]{40}', item)
                                        for item in shallow.read_text().splitlines()):
                raise ValueError('Invalid shallow boundary')
            worktree = os.path.relpath(repo / path, modules / path)
            (directory / 'HEAD').write_text(sha+'\n')
            (directory / 'config').write_text(
                '[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n'
                '\tbare = false\n\tlogallrefupdates = true\n'
                '\thooksPath = /dev/null\n\tfsmonitor = false\n'
                f'\tworktree = {worktree}\n')
            git(repo, '--git-dir='+str(directory), '--work-tree='+str(repo / path),
                'cat-file', '-e', sha+'^{commit}')
        staging.rename(modules)
    except BaseException:
        shutil.rmtree(staging)
        raise
    report = dict(cache_key=actual_key, archive_sha256=digest.hexdigest(),
                  archive_bytes=args.archive.stat().st_size, validated_members=len(names),
                  expanded_file_bytes=expanded, restored_object_and_shallow_bytes=selected,
                  verified_submodule_commits=len(links), imported_cached_configuration=False)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--cache-key', required=True)
    parser.add_argument('--repo', type=Path, default=Path('.'))
    parser.add_argument('--report', type=Path)
    parser.add_argument('--max-expanded-gib', type=int, default=12)
    restore(parser.parse_args())
