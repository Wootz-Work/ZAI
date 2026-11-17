"""The Conversation row ZAI writes back carries the reply as-is, with no "[ZAI] " prefix."""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from app.pipeline.nodes import writeback as wb_module


class FakeAppSheet:
    rows: list = []

    def __init__(self, settings):
        pass

    def enabled(self):
        return True

    def action_rows(self, *, table_name, action, rows, timeout):
        FakeAppSheet.rows = list(rows)


class WritebackRemarksTests(unittest.TestCase):
    def run_writeback(self, reply):
        settings = SimpleNamespace(
            appsheet_conversation_table="Conversation",
            appsheet_conversation_key_col="Conversation ID",
            appsheet_conversation_critical_col="Critical",
            database_url="",
        )
        state = {"ai_reply": reply, "checkin_id": "CHK-1", "event_type": "CONVERSATION_ADDED", "logs": []}
        with mock.patch.object(wb_module, "AppSheetClient", FakeAppSheet):
            wb_module.writeback(settings, state)
        return FakeAppSheet.rows[0]

    def test_remarks_has_no_zai_prefix(self):
        row = self.run_writeback("- Check the CMM report.")
        self.assertEqual(row["Remarks"], "- Check the CMM report.")
        self.assertFalse(row["Remarks"].startswith("[ZAI]"))

    def test_other_columns_unchanged(self):
        row = self.run_writeback("Reply text")
        self.assertEqual(row["Added by"], "zai@wootz.work")
        self.assertEqual(row["CheckIn ID"], "CHK-1")


if __name__ == "__main__":
    unittest.main()
