import os
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class WERInterfaceTest(unittest.TestCase):
    def parse(self, payload):
        interface = os.path.join(
            os.path.dirname(__file__), "..", "app", "parsers", "WER", "interface.py"
        )
        parse = runpy.run_path(interface)["auto_interface"]
        popen = subprocess.Popen

        def launch(command, **kwargs):
            return popen([sys.executable] + command[1:], **kwargs)

        with tempfile.TemporaryDirectory() as folder:
            report = os.path.join(folder, "Report with spaces.wer")
            with open(report, "wb") as stream:
                stream.write(payload)
            with patch.object(subprocess, "Popen", side_effect=launch):
                return parse(report, "WER")

    def test_decode_error_is_reported_as_parser_failure(self):
        result = self.parse(b"\xff\xfe\x00")
        self.assertIsNone(result[0])
        self.assertIn("UnicodeDecodeError", result[1])

    def test_utf16_report_returns_a_record(self):
        result = self.parse("AppName=Example\n".encode("utf-16"))
        self.assertEqual(result, [{"AppName": "Example"}])
