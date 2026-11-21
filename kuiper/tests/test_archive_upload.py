import io
import os
import runpy
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch


class ArchiveUploadTest(unittest.TestCase):
    def setUp(self):
        module = Path(__file__).parent.parent / 'app' / 'archive_upload.py'
        functions = runpy.run_path(str(module))
        self.extract = functions['extract_archive']
        self.is_archive = functions['is_archive_upload']
        self.run_7z = functions['run_7z']
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.case = Path(temp.name)
        self.machine = self.case / 'machine'
        self.machine.mkdir()
        self.destination = self.machine / 'upload'

    def test_zip_publishes_only_after_extraction(self):
        source = self.case / 'report.zip'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('reports/report.txt', 'report')
        extractall = zipfile.ZipFile.extractall

        def check_visibility(archive, target):
            self.assertFalse(self.destination.exists())
            self.assertEqual(os.listdir(self.machine), [])
            extractall(archive, target)

        with patch.object(zipfile.ZipFile, 'extractall', check_visibility):
            self.extract(str(source), str(self.destination))
        self.assertEqual((self.destination / 'reports' / 'report.txt').read_text(), 'report')
        self.assertFalse(list(self.case.glob('.kuiper-archive-*')))

    def test_corrupt_named_archive_is_not_a_loose_artifact(self):
        source = self.case / 'broken.ZIP'
        source.write_bytes(b'truncated')
        self.assertTrue(self.is_archive(str(source), source.name))
        with self.assertRaises(zipfile.BadZipFile):
            self.extract(str(source), str(self.destination))
        self.assertFalse(self.destination.exists())
        self.assertFalse(list(self.case.glob('.kuiper-archive-*')))
        self.assertTrue(source.exists())

    def test_tar_gzip_preserves_payload(self):
        source = self.case / 'reports.tar.gz'
        with tarfile.open(source, 'w:gz') as archive:
            member = tarfile.TarInfo('report.txt')
            member.size = 6
            archive.addfile(member, io.BytesIO(b'report'))
        self.extract(str(source), str(self.destination))
        self.assertEqual((self.destination / 'report.txt').read_bytes(), b'report')

    def test_failed_fallback_leaves_no_partial_output(self):
        source = self.case / 'report.zip'
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('report.txt', 'report')

        def primary_failure(archive, target):
            Path(target, 'primary-partial.txt').touch()
            raise NotImplementedError('unsupported compression')

        def fallback_failure(path, target):
            self.assertEqual(os.listdir(target), [])
            Path(target, 'fallback-partial.txt').touch()
            raise RuntimeError('7z extraction failed with exit status 2')

        zipper = self.extract.__globals__['extract_zip']
        with patch.object(zipfile.ZipFile, 'extractall', primary_failure):
            with patch.dict(zipper.__globals__, run_7z=fallback_failure):
                with self.assertRaisesRegex(RuntimeError, 'exit status 2'):
                    self.extract(str(source), str(self.destination))
        self.assertEqual(os.listdir(self.machine), [])
        self.assertFalse(list(self.case.glob('.kuiper-archive-*')))

    def test_7z_nonzero_exit_is_a_failure(self):
        process = Mock()
        process.wait.return_value = 2
        with patch('subprocess.Popen', return_value=process):
            with self.assertRaisesRegex(RuntimeError, 'exit status 2'):
                self.run_7z('broken.zip', str(self.destination))
