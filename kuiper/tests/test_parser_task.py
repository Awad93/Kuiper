import os
import runpy
import sys
import types
import unittest
from unittest.mock import Mock, patch

from celery import Celery, states


class ParserTaskTest(unittest.TestCase):
    def setUp(self):
        celery = Celery("parser-test", broker="memory://", backend="cache+memory://")
        self.addCleanup(celery.close)
        self.artifacts = {"queued": "queued", "running": "parsing", "complete": "done"}
        self.update_files = Mock(side_effect=self.update_artifacts)

        app = types.ModuleType("app")
        app.app = Mock(config={"celery_task_name": "test.run_parsers"})
        app.celery_app = celery
        db = types.ModuleType("app.database.dbstuff")
        db.db_files = Mock(update_files_by_task_id=self.update_files)
        db.logger = Mock(DEBUG=10, INFO=20, ERROR=40)
        modules = {
            "app": app,
            "app.database": types.ModuleType("app.database"),
            "app.database.dbstuff": db,
            "app.database.elkdb": types.ModuleType("app.database.elkdb"),
        }
        path = os.path.join(
            os.path.dirname(__file__), "..", "app", "controllers", "parser_management.py"
        )
        with patch.dict(sys.modules, modules):
            parser_module = runpy.run_path(path)

        self.run_parsers = parser_module["run_parsers"]
        self.error_handler = parser_module["task_error_handler"]
        self.manager = parser_module["Parser_Manager"]

    def update_artifacts(self, machine_id, task_id, status, message):
        for artifact, current_status in self.artifacts.items():
            if current_status in ("queued", "parsing"):
                self.artifacts[artifact] = status
        return [True, "Updated"]

    def run_task(self):
        return self.run_parsers.apply(
            args=("case", "machine", ["parser"]),
            link_error=[self.error_handler.s()],
            throw=False,
        )

    def assert_failed(self, result, message):
        self.assertEqual(result.state, states.FAILURE)
        self.assertEqual(str(result.result), message)
        self.update_files.assert_called_once_with("machine", result.id, "error", message)
        self.assertEqual(
            self.artifacts, {"queued": "error", "running": "error", "complete": "done"}
        )

    def test_file_discovery_failure(self):
        with patch.object(self.manager, "specify_files_to_be_parser", return_value=False):
            result = self.run_task()
        self.assert_failed(result, "Failed collecting the list of files to be parsed")

    def test_parsing_failure(self):
        with patch.object(self.manager, "specify_files_to_be_parser", return_value=True):
            with patch.object(self.manager, "start_parsing", return_value=False):
                result = self.run_task()
        self.assert_failed(result, "Failed processing the files")

    def test_unexpected_exception(self):
        with patch.object(self.manager, "specify_files_to_be_parser", return_value=True):
            with patch.object(self.manager, "start_parsing", side_effect=ValueError("parser crashed")):
                result = self.run_task()
        self.assert_failed(result, "parser crashed")
        self.assertIsInstance(result.result, ValueError)

    def test_success_returns_progress(self):
        def parse(manager):
            manager.tmp_total_files = 1
            manager.files_parsed = 1
            manager.set_task_state("PROCESSING", 1)
            return True

        with patch.object(self.manager, "specify_files_to_be_parser", return_value=True):
            with patch.object(self.manager, "start_parsing", parse):
                result = self.run_task()
        self.assertEqual(result.state, states.SUCCESS)
        self.assertEqual(result.result["files_done"], 1)
        self.assertEqual(result.result["total_files"], 1)
        self.update_files.assert_not_called()


if __name__ == "__main__":
    unittest.main()
