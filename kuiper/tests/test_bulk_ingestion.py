import json
import os
import runpy
import sys
import types
import unittest
from unittest.mock import Mock, mock_open, patch

from elasticsearch.exceptions import ConnectionTimeout


class BulkIngestionTest(unittest.TestCase):
    def setUp(self):
        app = types.ModuleType("app")
        app.logger = Mock(ERROR=40, WARNING=30, INFO=20)
        file = os.path.join(os.path.dirname(__file__), "..", "app", "database", "elkdb.py")
        with patch.dict(sys.modules, {"app": app}):
            with patch("builtins.open", mock_open(read_data="ElasticSearch: {IP: localhost, PORT: 9200}")):
                self.db = runpy.run_path(file)["db_es"]

        sleep = patch("time.sleep")
        self.sleep = sleep.start()
        self.addCleanup(sleep.stop)
        self.records = [{"Data": {"value": "same"}}, {"Data": {"value": "same"}}]
        self.ids = ["first-occurrence", "second-occurrence"]
        self.index = {}
        self.received = []
        self.db.bulk_db.bulk = Mock(side_effect=self.store)

    def store(self, body, **kwargs):
        lines = iter(body.splitlines())
        items = []
        received = []
        for header in lines:
            operation, metadata = next(iter(json.loads(header).items()))
            source = json.loads(next(lines))
            record_id = metadata["_id"]
            received.append(record_id)
            status = 409 if operation == "create" and record_id in self.index else 201
            if status == 201:
                self.index[record_id] = source
            items.append({operation: {"_id": record_id, "_index": "case", "status": status}})
        self.received.append(received)
        return {"items": items}

    def push(self):
        return self.db.bulk_queue_push(self.records, "case", chunk_size=1, record_ids=self.ids)

    def test_timeout_retries_only_unconfirmed_actions(self):
        def timeout_after_write(body, **kwargs):
            response = self.store(body, **kwargs)
            if len(self.received) == 2:
                raise ConnectionTimeout("TIMEOUT", "lost response")
            return response

        self.db.bulk_db.bulk.side_effect = timeout_after_write
        ok, _, failed, successful = self.push()
        self.assertTrue(ok)
        self.assertEqual(failed, [])
        self.assertCountEqual(successful, self.ids)
        self.assertEqual(self.received[1], self.received[2])
        self.assertNotEqual(self.received[0], self.received[2])
        self.assertEqual(len(self.index), 2)
        self.sleep.assert_called_once_with(1)

    def test_replay_fills_missing_record_without_overwriting_existing_record(self):
        self.index[self.ids[0]] = {"Data": {"value": "same"}, "tag_id": "keep"}
        ok, _, failed, successful = self.push()
        self.assertTrue(ok)
        self.assertEqual(failed, [])
        self.assertCountEqual(successful, self.ids)
        self.assertEqual(self.index[self.ids[0]]["tag_id"], "keep")
        self.assertEqual(self.index[self.ids[1]], self.records[1])

    def test_outage_exhausts_retries_and_reports_all_unconfirmed_records(self):
        self.db.bulk_db.bulk.side_effect = ConnectionTimeout("TIMEOUT", "offline")
        ok, message, failed, successful = self.push()
        self.assertFalse(ok)
        self.assertIn("retry limit", message)
        self.assertCountEqual([record["_id"] for record in failed], self.ids)
        self.assertEqual(successful, [])
        self.assertEqual(self.db.bulk_db.bulk.call_count, 4)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [1, 2, 4])

    def test_permanent_rejection_is_reported_without_retry(self):
        def reject(body, **kwargs):
            result = self.store(body, **kwargs)
            item = result["items"][0]["create"]
            if item["_id"] == self.ids[0]:
                self.index.pop(item["_id"])
                item["status"] = 403
            return result

        self.db.bulk_db.bulk.side_effect = reject
        ok, _, failed, successful = self.push()
        self.assertTrue(ok)
        self.assertEqual([record["_id"] for record in failed], [self.ids[0]])
        self.assertEqual(successful, [self.ids[1]])
        self.sleep.assert_not_called()

    def test_manual_tags_use_new_ids(self):
        for _ in range(2):
            self.db.bulk_queue_push(self.records[:1], "case")
        self.assertEqual(len(self.index), 2)

    def test_throttled_record_is_retried_independently(self):
        def throttle_once(body, **kwargs):
            response = self.store(body, **kwargs)
            item = response["items"][0]["create"]
            if len(self.received) == 1:
                self.index.pop(item["_id"])
                item["status"] = 429
            return response

        self.db.bulk_db.bulk.side_effect = throttle_once
        ok, _, failed, successful = self.push()
        self.assertTrue(ok)
        self.assertEqual(failed, [])
        self.assertCountEqual(successful, self.ids)
        self.assertEqual(self.received[0], self.received[2])
        self.assertNotEqual(self.received[1], self.received[2])

    def test_mapping_repair_uses_same_create_id(self):
        def reject_date_once(body, **kwargs):
            response = self.store(body, **kwargs)
            item = response["items"][0]["create"]
            if len(self.received) == 1:
                self.index.pop(item["_id"])
                item.update(status=400, error={
                    "reason": "failed to parse field [Data.value] of type [date] in document with id 1",
                })
            return response

        self.db.bulk_db.bulk.side_effect = reject_date_once
        ok, _, failed, successful = self.push()
        self.assertTrue(ok)
        self.assertEqual(failed, [])
        self.assertCountEqual(successful, self.ids)
        self.assertEqual(self.received[0], self.received[2])
        repaired = self.index[self.received[0][0]]
        self.assertEqual(repaired["Data"]["value"], "1700-01-01T00:00:00")
