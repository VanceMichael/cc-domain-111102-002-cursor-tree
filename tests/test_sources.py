import unittest
import json
from app.wire import Replay

from app.directory import decode_page, read_page
class DomainTests(unittest.TestCase):
    def test_root(self):
        p=read_page(Replay("fixtures/responses.json"), "https://directory.invalid/root")
        self.assertEqual(p.snapshot,"S1")
        self.assertEqual(len(p.items),2)
    def test_missing_fields(self):
        with self.assertRaises(ValueError): decode_page(b"{}")
    def test_bad_kind(self):
        p={"node_id":"x","parent_id":None,"snapshot":"s","items":[{"kind":"oops","id":"i"}],"next_cursor":None}
        with self.assertRaises(ValueError): decode_page(json.dumps(p))


class WireTests(unittest.TestCase):
    def test_unknown_route(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        with TemporaryDirectory() as d:
            p=Path(d)/"routes.json";p.write_text("{}")
            self.assertEqual(Replay(p).get("https://missing.invalid").status,404)
