import contextlib
import io
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.collect import (
    CHAIN_END,
    CURSOR_EXPIRED,
    NOT_ATTEMPTED,
    SNAPSHOT_CHANGED,
    Collector,
    main,
)
from app.wire import Replay


def page(node, parent, snap, items, nxt=None):
    return json.dumps({
        "node_id": node, "parent_id": parent, "snapshot": snap,
        "items": items, "next_cursor": nxt,
    })


def branch(i, url):
    return {"kind": "branch", "id": i, "url": url}


def site(i, name=None):
    d = {"kind": "site", "id": i}
    if name:
        d["name"] = name
    return d


def ok(body):
    return {"status": 200, "body": body}


def gone():
    return {"status": 410, "body": "cursor_expired"}


def make_transport(case, routes):
    tmp = TemporaryDirectory()
    case.addCleanup(tmp.cleanup)
    p = Path(tmp.name) / "routes.json"
    p.write_text(json.dumps(routes), encoding="utf-8")
    return Replay(p)


class FixtureRunTests(unittest.TestCase):
    """给定 fixtures：游标过期 + 目录迁移 + 回边的组合。"""

    def test_fixture_run_is_mixed_not_complete(self):
        c = Collector(Replay("fixtures/responses.json"), max_requests=20, max_branch_restarts=2)
        rep = c.run("https://directory.invalid/root")
        v = rep["verdict"]
        # west 落到 S2，root/east 停在 S1：跨代次，不能宣称完整
        self.assertFalse(v["complete"])
        self.assertTrue(v["mixed"])
        self.assertEqual(set(v["scopes"]), {"S1", "S2"})
        west = rep["branches"]["west"]
        self.assertEqual(west["status"], "complete")
        self.assertEqual(west["snapshot"], "S2")
        self.assertEqual(west["restarts"], 1)
        self.assertEqual([d["outcome"] for d in west["discarded"]], [CURSOR_EXPIRED])
        self.assertEqual(west["discarded"][0]["site_ids"], ["v1"])
        self.assertEqual(rep["branches"]["east"]["snapshot"], "S1")
        # 网点及到根路径：v1 只从采纳的 east@S1 来，废弃的 west@S1 半程不并入
        sites = {s["id"]: s for s in rep["sites"]}
        self.assertEqual(set(sites), {"v1", "v2"})
        self.assertEqual(sites["v1"]["path"], "root/east/v1")
        self.assertEqual(sites["v2"]["path"], "root/west/v2")
        # 回边 east -> root
        self.assertEqual(len(rep["back_edges"]), 1)
        be = rep["back_edges"][0]
        self.assertEqual((be["from"], be["to"], be["kind"]), ("east", "root", "ancestor"))
        # v1 的双重父关系是迁移观测（一部分在已废弃半程），采纳结果内无冲突
        self.assertEqual(rep["dual_parents"]["sites"], [])
        moves = rep["dual_parents"]["move_observations"]
        self.assertEqual([m["site"] for m in moves], ["v1"])
        self.assertEqual([l["branch"] for l in moves[0]["accepted"]], ["east"])
        self.assertEqual([l["branch"] for l in moves[0]["discarded"]], ["west"])
        # 同名不同身份不合并
        self.assertEqual(rep["dual_parents"]["same_name_distinct_ids"],
                         [{"name": "河村", "ids": ["v1", "v2"]}])
        self.assertEqual(rep["requests_used"], 5)

    def test_rerun_reuses_matching_generation(self):
        first = Collector(Replay("fixtures/responses.json"), max_requests=20, max_branch_restarts=2)
        first.run("https://directory.invalid/root")
        state = first.state()
        second = Collector(Replay("fixtures/responses.json"),
                           max_requests=20, max_branch_restarts=2, prior=state)
        rep = second.run("https://directory.invalid/root")
        # east@S1 与本轮根代次一致，复用；west@S2 与根代次 S1 不符，重新采集
        self.assertTrue(rep["branches"]["east"]["reused"])
        self.assertFalse(rep["branches"]["west"]["reused"])
        self.assertEqual(rep["requests_used"], 4)
        nodes = [e["node"] for e in rep["excluded_prior_records"]]
        self.assertIn("west", nodes)
        self.assertNotIn("east", nodes)


class GenerationTests(unittest.TestCase):
    def test_mid_pagination_snapshot_change_discards_half(self):
        routes = {
            "R": [ok(page("root", None, "S1", [branch("a", "A")]))],
            "A": [ok(page("a", "root", "S1", [site("s1")], "C1")),
                  ok(page("a", "root", "S1", [site("s1")], "C2"))],
            "C1": [ok(page("a", "root", "S2", [site("sX")]))],
            "C2": [ok(page("a", "root", "S1", [site("s2")]))],
        }
        c = Collector(make_transport(self, routes), max_requests=20, max_branch_restarts=2)
        rep = c.run("R")
        a = rep["branches"]["a"]
        self.assertEqual(a["status"], "complete")
        self.assertEqual(a["snapshot"], "S1")
        self.assertEqual([d["outcome"] for d in a["discarded"]], [SNAPSHOT_CHANGED])
        # sX 来自代次混杂的废弃半程，不纳入采纳结果
        self.assertEqual({s["id"] for s in rep["sites"]}, {"s1", "s2"})
        self.assertTrue(rep["verdict"]["complete"])
        self.assertEqual(rep["verdict"]["generation"], "S1")

    def test_restart_budget_bounds_failing_branch(self):
        routes = {
            "R": [ok(page("root", None, "S1", [branch("a", "A"), branch("b", "B")]))],
            "A": [ok(page("a", "root", "S1", [site("s1")], "C")),
                  ok(page("a", "root", "S1", [site("s1")], "C"))],
            "C": [gone(), gone()],
            "B": [ok(page("b", "root", "S1", [site("s2")]))],
        }
        c = Collector(make_transport(self, routes), max_requests=20, max_branch_restarts=1)
        rep = c.run("R")
        a = rep["branches"]["a"]
        self.assertEqual(a["status"], "incomplete")
        self.assertEqual(a["reason"], CURSOR_EXPIRED)
        self.assertEqual(a["restarts"], 1)
        # 失效分支不拖垮其他分支
        self.assertEqual(rep["branches"]["b"]["status"], "complete")
        self.assertFalse(rep["verdict"]["complete"])
        self.assertTrue(any("root/a" in g for g in rep["verdict"]["gaps"]))
        # 未完成分支里的网点不纳入采纳结果
        self.assertEqual({s["id"] for s in rep["sites"]}, {"s2"})

    def test_request_budget_marks_untouched_scope(self):
        routes = {
            "R": [ok(page("root", None, "S1", [branch("a", "A"), branch("b", "B")]))],
            "A": [ok(page("a", "root", "S1", [site("s1")]))],
            "B": [ok(page("b", "root", "S1", [site("s2")]))],
        }
        c = Collector(make_transport(self, routes), max_requests=2, max_branch_restarts=1)
        rep = c.run("R")
        self.assertEqual(rep["branches"]["a"]["status"], "complete")
        self.assertEqual(rep["branches"]["b"]["reason"], NOT_ATTEMPTED)
        self.assertFalse(rep["verdict"]["complete"])
        self.assertTrue(any("root/b" in g for g in rep["verdict"]["gaps"]))


class GraphTests(unittest.TestCase):
    def test_dual_parent_and_back_edge(self):
        routes = {
            "R": [ok(page("root", None, "S1", [branch("a", "A"), branch("b", "B")]))],
            "A": [ok(page("a", "root", "S1", [site("s1", "河村"), branch("x", "X")]))],
            "B": [ok(page("b", "root", "S1",
                         [site("s1", "河村"), branch("x", "X"), branch("root", "R")]))],
            "X": [ok(page("x", "a", "S1", [site("s2")]))],
        }
        c = Collector(make_transport(self, routes), max_requests=20, max_branch_restarts=1)
        rep = c.run("R")
        self.assertTrue(rep["verdict"]["complete"])
        # 网点 s1 同代次双重父关系：两条路径都保留
        dual = rep["dual_parents"]["sites"]
        self.assertEqual([d["site"] for d in dual], ["s1"])
        self.assertEqual({l["branch"] for l in dual[0]["locations"]}, {"a", "b"})
        s1_paths = sorted(s["path"] for s in rep["sites"] if s["id"] == "s1")
        self.assertEqual(s1_paths, ["root/a/s1", "root/b/s1"])
        # 分支 x 双重父关系：只采集一次，但两个父级都记录
        self.assertEqual(rep["dual_parents"]["branches"][0]["node"], "x")
        self.assertEqual(rep["branches"]["x"]["parents"], ["a", "b"])
        self.assertEqual(rep["requests_used"], 4)
        s2 = [s for s in rep["sites"] if s["id"] == "s2"][0]
        self.assertEqual(s2["path"], "root/a/x/s2")
        # 回边 b -> root 不成环
        be = rep["back_edges"][0]
        self.assertEqual((be["from"], be["to"], be["kind"]), ("b", "root", "ancestor"))


class RerunTests(unittest.TestCase):
    def _prior(self, branch_rec):
        return {"version": 1, "root_snapshot": "S1", "root_complete": True,
                "branches": {"a": branch_rec}}

    def _rec(self, snapshot, sites=None):
        return {"node_id": "a", "url": "A", "status": "complete", "reason": CHAIN_END,
                "snapshot": snapshot, "pages": ["A"], "digest": "x",
                "sites": sites or [], "children": [],
                "declared_parent": "root", "parents": ["root"],
                "restarts": 0, "requests": 1}

    def test_root_generation_change_explains_exclusions(self):
        prior = self._prior(self._rec("S1"))
        routes = {
            "R": [ok(page("root", None, "S2", [branch("a", "A")]))],
            "A": [ok(page("a", "root", "S2", [site("s1")]))],
        }
        c = Collector(make_transport(self, routes),
                      max_requests=10, max_branch_restarts=1, prior=prior)
        rep = c.run("R")
        self.assertFalse(rep["branches"]["a"]["reused"])
        self.assertTrue(rep["verdict"]["complete"])
        self.assertEqual(rep["verdict"]["generation"], "S2")
        excl = rep["excluded_prior_records"]
        self.assertTrue(any("根代次由 S1 变为 S2" in e["reason"] for e in excl))
        self.assertTrue(any(e["node"] == "a" and "已重新采集" in e["reason"] for e in excl))

    def test_prior_newer_branch_reusable_when_root_catches_up(self):
        prior = self._prior(self._rec("S2", sites=[site("s0", "老网点")]))
        routes = {"R": [ok(page("root", None, "S2", [branch("a", "A")]))]}
        c = Collector(make_transport(self, routes),
                      max_requests=10, max_branch_restarts=1, prior=prior)
        rep = c.run("R")
        # 上一轮混代次留下的 a@S2，在根追平到 S2 后可直接复用
        self.assertTrue(rep["branches"]["a"]["reused"])
        self.assertEqual(rep["requests_used"], 1)
        self.assertTrue(rep["verdict"]["complete"])
        self.assertEqual(rep["verdict"]["generation"], "S2")
        s0 = [s for s in rep["sites"] if s["id"] == "s0"][0]
        self.assertEqual(s0["path"], "root/a/s0")
        self.assertEqual(s0["snapshot"], "S2")


class MainTests(unittest.TestCase):
    def test_cli_two_runs_persist_state(self):
        with TemporaryDirectory() as d:
            state = str(Path(d) / "state.json")
            report = str(Path(d) / "report.json")
            argv = ["--state", state, "--report", report]
            with contextlib.redirect_stdout(io.StringIO()):
                main(argv)
            first = json.loads(Path(report).read_text(encoding="utf-8"))
            self.assertFalse(first["verdict"]["complete"])
            self.assertTrue(first["verdict"]["mixed"])
            with contextlib.redirect_stdout(io.StringIO()):
                main(argv)
            second = json.loads(Path(report).read_text(encoding="utf-8"))
            self.assertTrue(second["branches"]["east"]["reused"])
            self.assertEqual(second["requests_used"], 4)
            saved = json.loads(Path(state).read_text(encoding="utf-8"))
            self.assertEqual(saved["branches"]["west"]["snapshot"], "S2")


if __name__ == "__main__":
    unittest.main()
