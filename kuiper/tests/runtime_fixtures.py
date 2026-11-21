"""Small parser checks that run under Python 2.7 and Python 3."""

from __future__ import print_function

import io
import os
import runpy
import shutil
import tempfile
import unittest


APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app')


class RuntimeFixtures(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.folder)

    def test_exchange_log(self):
        path = os.path.join(self.folder, 'ECPServer.log')
        with io.open(path, 'w', encoding='utf-8-sig', newline='') as report:
            report.write(u'#Fields: TimeStamp,EventData\r\n')
            report.write(u'2020-01-01T00:00:00Z,"caf\u00e9, test"\r\n')
        module = os.path.join(APP, 'parsers', 'exchange_logs', 'exchange_log_interface.py')
        parse = runpy.run_path(module)['exchange_log_interface']
        self.assertEqual(list(parse(path, 'Exchange Logs')), [{
            'TimeStamp': '2020-01-01T00:00:00Z',
            '@timestamp': '2020-01-01T00:00:00Z',
            'EventData': u'caf\u00e9, test',
        }])

    def test_wer_helper(self):
        path = os.path.join(self.folder, 'Report with spaces.wer')
        with io.open(path, 'w', encoding='utf-16') as report:
            report.write(u'AppName=Example\n')
        module = os.path.join(APP, 'parsers', 'WER', 'interface.py')
        parse = runpy.run_path(module)['auto_interface']
        self.assertEqual(parse(path, 'WER'), [{'AppName': 'Example'}])
        with io.open(path, 'wb') as report:
            report.write(b'\xff\xfe\x00')
        result = parse(path, 'WER')
        self.assertIsNone(result[0])
        self.assertIn('UnicodeDecodeError', result[1])


if __name__ == '__main__':
    unittest.main()
