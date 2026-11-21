"""Extract evidence archives before publishing them to the parser tree."""

import os
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import zipfile


ARCHIVE_EXTENSIONS = ('.zip', '.tar', '.tar.gz', '.tar.bz2', '.tar.xz', '.tgz', '.tbz', '.tbz2', '.txz')


def is_archive_upload(path, filename):
    return (filename.lower().endswith(ARCHIVE_EXTENSIONS)
            or zipfile.is_zipfile(path) or tarfile.is_tarfile(path))


def validate_member(destination, name):
    root = os.path.normcase(os.path.realpath(destination))
    target = os.path.normcase(os.path.realpath(os.path.join(root, name)))
    if target != root and not target.startswith(root + os.sep):
        raise ValueError("Archive member is outside the extraction directory")


def run_7z(path, destination, timeout=300):
    with open(os.devnull, 'r+b') as devnull:
        process = subprocess.Popen(
            ['7z', 'x', '-y', '-o' + destination, '--', os.path.abspath(path)],
            stdin=devnull, stdout=devnull, stderr=devnull,
        )
        timed_out = threading.Event()

        def stop():
            if process.poll() is None:
                timed_out.set()
                try:
                    process.kill()
                except OSError:
                    pass

        timer = threading.Timer(timeout, stop)
        timer.daemon = True
        timer.start()
        try:
            status = process.wait()
        finally:
            timer.cancel()
            timer.join()
        if timed_out.is_set():
            raise RuntimeError("7z extraction timed out after %s seconds" % timeout)
        if status:
            raise RuntimeError("7z extraction failed with exit status %s" % status)


def extract_zip(path, destination):
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            validate_member(destination, member.filename)
            if stat.S_ISLNK(member.external_attr >> 16):
                raise ValueError("ZIP contains a symbolic link")
        try:
            archive.extractall(destination)
            return
        except (NotImplementedError, RuntimeError) as exc:
            if not isinstance(exc, NotImplementedError) and 'compression' not in str(exc).lower():
                raise

    # An unsupported compression method can fail after other entries were written.
    shutil.rmtree(destination)
    os.mkdir(destination)
    run_7z(path, destination)


def extract_archive(path, destination):
    destination = os.path.abspath(destination)
    machine_folder = os.path.dirname(destination)
    staging = tempfile.mkdtemp(prefix='.kuiper-archive-', dir=os.path.dirname(machine_folder))
    try:
        contents = os.path.join(staging, 'contents')
        os.mkdir(contents)
        if path.lower().endswith('.zip') or zipfile.is_zipfile(path):
            extract_zip(path, contents)
        else:
            with tarfile.open(path, mode='r:*') as archive:
                for member in archive.getmembers():
                    validate_member(contents, member.name)
                    if not (member.isfile() or member.isdir()):
                        raise ValueError("TAR contains an unsupported entry type")
                archive.extractall(contents)

        if os.path.lexists(destination):
            raise ValueError("Archive destination already exists")
        os.rename(contents, destination)
    finally:
        # Publication has already committed if rename succeeded.
        shutil.rmtree(staging, ignore_errors=True)
