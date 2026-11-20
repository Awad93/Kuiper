import os
import runpy
import sys
import tempfile
import types
import unittest
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


class ParserRecoveryTest(unittest.TestCase):
    def setUp(self):
        app = types.ModuleType("app")
        app.os = os
        path = os.path.join(os.path.dirname(__file__), "..", "app", "database", "dbstuff.py")
        with tempfile.TemporaryDirectory() as parser_path:
            app.app = MagicMock(config={
                "DB_NAME": "test", "DB_IP": "localhost", "DB_PORT": 27017,
                "PARSER_PATH": parser_path,
            })
            with patch.dict(sys.modules, {"app": app}):
                with patch("pymongo.MongoClient"):
                    self.db = runpy.run_path(path)["db_files"]

        self.machine = {
            "_id": "machine",
            "files": [{"file_path": "artifact", "parsers": [
                {"parser_name": name, "status": status, "task_id": "old"}
                for name, status in (("a", "done"), ("b", "queued"), ("c", "parsing"))
            ]}],
        }
        self.db.add_machine_in_files = MagicMock(return_value=[True, self.machine])
        self.db.collection.update.return_value = {"n": 1}

    def test_progress_clears_expired_work_without_overwriting_completed_work(self):
        lease = {"owner": "previous", "expires": datetime.utcnow() - timedelta(minutes=1)}
        self.machine["parser_lease"] = lease
        recovered = deepcopy(self.machine)
        recovered["files"][0]["parsers"][1]["status"] = "pending"
        recovered["files"][0]["parsers"][2]["status"] = "error"
        self.db.collection.find_one.return_value = recovered

        ok, progress = self.db.get_parsing_progress("machine")
        self.assertTrue(ok)
        self.assertEqual([progress[name][0]["status"] for name in ("a", "b", "c")],
                         ["done", "pending", "error"])
        query, update = self.db.collection.update.call_args[0]
        self.assertEqual(query["parser_lease"], lease)
        self.assertEqual(query["files.0.parsers.2.task_id"], "old")
        self.assertEqual(query["files.0.parsers.2.status"], "parsing")
        self.assertNotIn("files.0.parsers.0.status", update["$set"])
        self.assertEqual(update["$set"]["files.0.parsers.1.status"], "pending")
        self.assertEqual(update["$set"]["files.0.parsers.2.status"], "error")

    def test_progress_leaves_unexpired_lease_alone(self):
        self.machine["parser_lease"] = {
            "owner": "current", "expires": datetime.utcnow() + timedelta(minutes=1),
        }
        ok, progress = self.db.get_parsing_progress("machine")
        self.assertTrue(ok)
        self.assertEqual(progress["c"][0]["status"], "parsing")
        self.db.collection.update.assert_not_called()
