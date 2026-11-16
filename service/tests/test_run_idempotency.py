"""
Run-tracking and reply-guard tests.

Pure tests always run. Tests that need Postgres run only when ZAI_TEST_DATABASE_URL points at a scratch
database (they create and drop their own ai_runs / artifacts tables there, never touch anything else):

  ZAI_TEST_DATABASE_URL=postgresql://localhost:5544/zai_test python -m unittest service.tests.test_run_idempotency

Run from the repo root with service/ on the path, e.g.  cd service && python -m unittest tests.test_run_idempotency
"""
from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest import mock

import psycopg2

from app.pipeline import graph
from app.pipeline.ingest.run_log import RunLog
from app.tools.db_tool import DBTool

DB_URL = os.getenv("ZAI_TEST_DATABASE_URL", "")


class ScopedKeyTests(unittest.TestCase):
    def key(self, payload, event="CHECKIN_CREATED", pid="CHK-1"):
        return graph._scoped_primary_id_for_run(payload, event_type=event, primary_id=pid)

    def test_sheet_event_keeps_plain_key(self):
        self.assertEqual(self.key({"source": "sheets"}), "CHK-1")
        self.assertEqual(self.key({}), "CHK-1")

    def test_wootzcheckin_event_gets_its_own_key(self):
        self.assertEqual(self.key({"source": "wootzcheckin"}), "CHK-1::WOOTZ")

    def test_direct_and_sheet_events_never_share_a_key(self):
        for event in ("CHECKIN_CREATED", "CHECKIN_UPDATED", "CONVERSATION_ADDED"):
            self.assertNotEqual(
                self.key({"source": "wootzcheckin"}, event), self.key({"source": "sheets"}, event)
            )

    def test_ingest_and_media_modes_still_scoped(self):
        self.assertEqual(self.key({"meta": {"ingest_only": True}}), "CHK-1::INGEST_V1")
        self.assertEqual(self.key({"meta": {"ingest_only": True, "media_only": True}}), "CHK-1::MEDIA_V1")
        self.assertEqual(
            self.key({"source": "wootzcheckin", "meta": {"ingest_only": True}}), "CHK-1::INGEST_V1::WOOTZ"
        )

    def test_bypass_idempotency_unchanged(self):
        k = self.key({"source": "wootzcheckin", "meta": {"bypass_idempotency": True}})
        self.assertTrue(k.startswith("CHK-1::REPLAY::"))


class ReplyGuardTests(unittest.TestCase):
    settings = SimpleNamespace(database_url="postgresql://unused")

    def test_true_when_reply_exists(self):
        with mock.patch.object(DBTool, "has_ai_reply_for_checkin", return_value=True):
            self.assertTrue(graph._reply_already_written(self.settings, {"checkin_id": "CHK-1"}))

    def test_false_when_no_reply(self):
        with mock.patch.object(DBTool, "has_ai_reply_for_checkin", return_value=False):
            self.assertFalse(graph._reply_already_written(self.settings, {"checkin_id": "CHK-1"}))

    def test_fails_open_on_lookup_error(self):
        with mock.patch.object(DBTool, "has_ai_reply_for_checkin", side_effect=RuntimeError("db down")):
            self.assertFalse(graph._reply_already_written(self.settings, {"checkin_id": "CHK-1"}))

    def test_false_without_checkin_id(self):
        self.assertFalse(graph._reply_already_written(self.settings, {}))


@unittest.skipUnless(DB_URL, "set ZAI_TEST_DATABASE_URL to a scratch Postgres to run these")
class PostgresRunLogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = SimpleNamespace(database_url=DB_URL)
        with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS artifacts; DROP TABLE IF EXISTS ai_runs;")
            cur.execute(
                """
                CREATE TABLE ai_runs (
                  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  tenant_id TEXT NOT NULL, event_type TEXT NOT NULL, primary_id TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'QUEUED', error_message TEXT,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ);
                CREATE UNIQUE INDEX uq_ai_runs_idempotency ON ai_runs (tenant_id, event_type, primary_id);
                CREATE TABLE artifacts (
                  artifact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  run_id UUID REFERENCES ai_runs(run_id) ON DELETE CASCADE,
                  artifact_type TEXT NOT NULL, url TEXT NOT NULL, meta JSONB,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now());
                """
            )

    @classmethod
    def tearDownClass(cls):
        with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS artifacts; DROP TABLE IF EXISTS ai_runs;")

    def setUp(self):
        with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM artifacts; DELETE FROM ai_runs;")
        self.runlog = RunLog(self.settings)

    def tenant_of(self, run_id):
        with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
            cur.execute("SELECT tenant_id FROM ai_runs WHERE run_id=%s", (run_id,))
            return cur.fetchone()[0]

    def test_bug_reproduced_with_old_shared_key(self):
        """What production did: direct and sheet event shared one key, the second tenant rename collided."""
        direct = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", "CHK-1")
        self.runlog.update_tenant(direct, "TENANT-A")
        sheet = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", "CHK-1")  # old key: same primary id
        self.assertNotEqual(direct, sheet)
        # the real, unguarded SQL the old code ran:
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            with psycopg2.connect(DB_URL) as conn, conn.cursor() as cur:
                cur.execute("UPDATE ai_runs SET tenant_id=%s WHERE run_id=%s", ("TENANT-A", sheet))

    def test_direct_and_sheet_events_both_complete_with_scoped_keys(self):
        for source, pid in (("wootzcheckin", "CHK-2"), ("sheets", "CHK-2")):
            key = graph._scoped_primary_id_for_run({"source": source}, event_type="CHECKIN_CREATED", primary_id=pid)
            run = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", key)
            self.runlog.update_tenant(run, "TENANT-A")  # must not raise for either
            self.assertEqual(self.tenant_of(run), "TENANT-A")

    def test_update_tenant_collision_is_swallowed(self):
        """Same-source retry: the second rename hits the unique index and must not abort the run."""
        first = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", "CHK-3")
        self.runlog.update_tenant(first, "TENANT-A")
        second = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", "CHK-3")
        self.runlog.update_tenant(second, "TENANT-A")  # used to raise UniqueViolation
        self.assertEqual(self.tenant_of(first), "TENANT-A")
        self.assertEqual(self.tenant_of(second), "UNKNOWN")  # stays under the hint tenant
        self.runlog.success(second)  # the run can still finish normally

    def test_reply_guard_lookup(self):
        run = self.runlog.start("UNKNOWN", "CHECKIN_CREATED", "CHK-4")
        db = DBTool(DB_URL)
        self.assertFalse(db.has_ai_reply_for_checkin("CHK-4"))
        db.insert_artifact(run_id=run, artifact_type="AI_REPLY_GROUNDED", url="appsheet:conversation",
                           meta={"checkin_id": "CHK-4", "tenant_id": "TENANT-A"})
        self.assertTrue(db.has_ai_reply_for_checkin("CHK-4"))
        self.assertFalse(db.has_ai_reply_for_checkin("CHK-5"))
        # other artifact types do not count as a reply
        db.insert_artifact(run_id=run, artifact_type="IMAGE_CAPTION", url="x", meta={"checkin_id": "CHK-5"})
        self.assertFalse(db.has_ai_reply_for_checkin("CHK-5"))


if __name__ == "__main__":
    unittest.main()
