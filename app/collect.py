"""县乡村服务网点目录采集器：有界重试、分支恢复与覆盖凭据。

设计约束：
- 沿目录接口的父子关系发现分支，不预置行政区划清单。
- 一个分支的一遍翻页必须全部来自同一 snapshot（代次）；游标过期（410）
  允许从该分支首页重新申请，但拿到不同代次时，前后两半不得拼成一个分支。
- 有界重试：分支重启次数与整轮请求预算都有上限，单个失效分支不拖垮其他分支。
- 覆盖凭据：完整分支记录游标链与内容摘要，写入状态文件；中断后续跑时，
  仍属本轮根代次的完整分支直接复用，根代次变化时报告解释哪些既往结果未纳入。
- 节点身份以 id 为准，显示名称可重复，不按名称合并；回边与双重父关系单独报告。
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from app.directory import decode_page
from app.wire import Replay

COMPLETE = "complete"
INCOMPLETE = "incomplete"

CHAIN_END = "cursor_chain_exhausted"          # 完整：沿 next_cursor 走到末页
CURSOR_EXPIRED = "cursor_expired"             # 游标过期（410），重启次数用尽
SNAPSHOT_CHANGED = "snapshot_changed"         # 翻页中途代次变化，重启次数用尽
BUDGET_EXHAUSTED = "request_budget_exhausted"  # 整轮请求预算耗尽
NOT_ATTEMPTED = "not_attempted"               # 预算耗尽前未轮到
UPSTREAM_STATUS = "upstream_status"           # 非 200/410 的接口状态
INVALID_PAGE = "invalid_page"                 # 响应结构无法解析
IDENTITY_MISMATCH = "identity_mismatch"       # 响应节点身份与发现身份不一致
TRANSPORT_ERROR = "transport_error"           # 传输层异常

REASON_TEXT = {
    CHAIN_END: "游标链完整走到末页",
    CURSOR_EXPIRED: "游标过期（410），分支重启次数已用尽",
    SNAPSHOT_CHANGED: "翻页中途代次变化，分支重启次数已用尽",
    BUDGET_EXHAUSTED: "本轮请求预算耗尽",
    NOT_ATTEMPTED: "请求预算耗尽，本轮未轮到采集",
    INVALID_PAGE: "响应结构无效",
    IDENTITY_MISMATCH: "响应节点身份与发现身份不一致",
    TRANSPORT_ERROR: "传输层异常",
}


def reason_text(reason):
    if reason.startswith(UPSTREAM_STATUS):
        return "接口返回非预期状态 " + reason.rsplit("_", 1)[-1]
    return REASON_TEXT.get(reason, reason)


class BudgetExhausted(Exception):
    """整轮请求预算耗尽。"""


@dataclass
class Attempt:
    """一遍翻页的记录；被废弃的半程也保留，用于迁移观测。"""

    snapshot: str | None = None
    pages: list = field(default_factory=list)
    site_ids: list = field(default_factory=list)
    outcome: str = CHAIN_END


@dataclass
class Branch:
    node_id: str
    url: str
    status: str = INCOMPLETE
    reason: str = NOT_ATTEMPTED
    snapshot: str | None = None
    pages: list = field(default_factory=list)
    digest: str | None = None
    sites: list = field(default_factory=list)      # 采纳的网点项（原始字典）
    children: list = field(default_factory=list)   # 采纳的子分支项（原始字典）
    declared_parent: str | None = None
    restarts: int = 0
    requests: int = 0
    reused: bool = False
    discarded: list = field(default_factory=list)  # 被废弃的半程 Attempt


def _digest(pages, item_ids):
    payload = json.dumps({"pages": pages, "items": item_ids}, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class Collector:
    def __init__(self, transport, *, max_requests, max_branch_restarts, prior=None):
        self.transport = transport
        self.max_requests = max_requests
        self.max_branch_restarts = max_branch_restarts
        self.prior = prior if isinstance(prior, dict) else {}
        self.requests_used = 0
        self.branches = {}       # node_id -> Branch（本轮采纳视图）
        self.paths = {}          # node_id -> 自根的发现路径
        self.parents = {}        # node_id -> [发现父级...]
        self.root_id = None
        self.root_snapshot = None
        self.root_complete = False
        self.sites = []          # 采纳的网点观测
        self.back_edges = []
        self.branch_dual_parents = []
        self.declared_mismatch = []
        self._order = []

    # ---- 取数 -------------------------------------------------------------

    def _get(self, url):
        if self.requests_used >= self.max_requests:
            raise BudgetExhausted
        self.requests_used += 1
        return self.transport.get(url)

    def _crawl(self, expected_id, url):
        """采集一个分支；失败原因落在返回值里，绝不抛出。重试有界。"""
        br = Branch(node_id=expected_id or "", url=url)
        restarts = 0
        while True:
            attempt = Attempt()
            cursor = url
            snapshot = None
            sites, children, order = [], [], []
            declared = None
            outcome = None
            while cursor is not None:
                try:
                    resp = self._get(cursor)
                except BudgetExhausted:
                    outcome = BUDGET_EXHAUSTED
                    break
                except Exception:
                    outcome = TRANSPORT_ERROR
                    break
                br.requests += 1
                if resp.status == 410:
                    outcome = CURSOR_EXPIRED
                    break
                if resp.status != 200:
                    outcome = f"{UPSTREAM_STATUS}_{resp.status}"
                    break
                try:
                    page = decode_page(resp.body)
                except ValueError:
                    outcome = INVALID_PAGE
                    break
                if snapshot is None:
                    snapshot = page.snapshot
                    attempt.snapshot = snapshot
                    if expected_id is None:
                        expected_id = page.node_id
                        br.node_id = page.node_id
                    elif page.node_id != expected_id:
                        outcome = IDENTITY_MISMATCH
                        break
                    declared = page.parent_id
                elif page.snapshot != snapshot:
                    # 同一遍翻页里现两个代次：这一半作废，不得拼接
                    outcome = SNAPSHOT_CHANGED
                    break
                attempt.pages.append(cursor)
                for item in page.items:
                    order.append(item["id"])
                    if item["kind"] == "site":
                        sites.append(item)
                        attempt.site_ids.append(item["id"])
                    else:
                        children.append(item)
                cursor = page.next_cursor
            if outcome is None:
                br.status = COMPLETE
                br.reason = CHAIN_END
                br.snapshot = snapshot
                br.pages = list(attempt.pages)
                br.sites = sites
                br.children = children
                br.declared_parent = declared
                br.restarts = restarts
                br.digest = _digest(attempt.pages, order)
                return br
            attempt.outcome = outcome
            br.discarded.append(attempt)
            if outcome == BUDGET_EXHAUSTED:
                br.reason = BUDGET_EXHAUSTED
                br.restarts = restarts
                return br
            if restarts >= self.max_branch_restarts:
                br.reason = outcome
                br.restarts = restarts
                return br
            restarts += 1

    # ---- 图语义 -----------------------------------------------------------

    def _place(self, br, parent_id):
        if parent_id is None:
            self.paths[br.node_id] = (br.node_id,)
        else:
            self.paths[br.node_id] = self.paths.get(parent_id, ()) + (br.node_id,)
        self.parents.setdefault(br.node_id, [])

    def _register(self, br, parent_id):
        """登记一个完整分支的网点与子分支，返回新入队的子分支。"""
        path = self.paths[br.node_id]
        for item in br.sites:
            self.sites.append({
                "id": item["id"],
                "name": item.get("name"),
                "branch": br.node_id,
                "snapshot": br.snapshot,
                "path": "/".join(path + (item["id"],)),
            })
        discovered = self.parents.get(br.node_id, [])
        if br.declared_parent is not None and discovered and br.declared_parent not in discovered:
            self.declared_mismatch.append({
                "node": br.node_id,
                "declared_parent": br.declared_parent,
                "discovered_parents": list(discovered),
                "snapshot": br.snapshot,
            })
        new_entries = []
        ancestors = set(path)
        for item in br.children:
            cid, curl = item["id"], item.get("url")
            if cid in ancestors:
                self.back_edges.append({
                    "from": br.node_id,
                    "to": cid,
                    "url": curl,
                    "snapshot": br.snapshot,
                    "kind": "self" if cid == br.node_id else "ancestor",
                })
                continue
            if cid in self.parents:
                if br.node_id not in self.parents[cid]:
                    self.parents[cid].append(br.node_id)
                    self.branch_dual_parents.append({
                        "node": cid,
                        "parents": list(self.parents[cid]),
                        "snapshot": br.snapshot,
                    })
                continue
            self.parents[cid] = [br.node_id]
            new_entries.append((cid, curl, br.node_id))
        return new_entries

    # ---- 复用 -------------------------------------------------------------

    def _prior_record(self, node_id):
        return (self.prior.get("branches") or {}).get(node_id)

    def _reuse(self, rec):
        return Branch(
            node_id=rec["node_id"], url=rec["url"], status=COMPLETE,
            reason=rec.get("reason", CHAIN_END), snapshot=rec.get("snapshot"),
            pages=list(rec.get("pages", [])), digest=rec.get("digest"),
            sites=list(rec.get("sites", [])), children=list(rec.get("children", [])),
            declared_parent=rec.get("declared_parent"), reused=True,
        )

    # ---- 主流程 -----------------------------------------------------------

    def run(self, entry_url):
        queue = deque([(None, entry_url, None)])
        while queue:
            expected_id, url, parent_id = queue.popleft()
            if expected_id is not None and expected_id in self.branches:
                continue
            br = None
            if expected_id is not None and self.root_complete:
                rec = self._prior_record(expected_id)
                if rec and rec.get("status") == COMPLETE and rec.get("snapshot") == self.root_snapshot:
                    br = self._reuse(rec)
            if br is None and expected_id is not None and self.requests_used >= self.max_requests:
                br = Branch(node_id=expected_id, url=url)  # 预算已尽，未轮到
            if br is None:
                br = self._crawl(expected_id, url)
            self.branches[br.node_id] = br
            self._order.append(br.node_id)
            if parent_id is None:
                self.root_id = br.node_id
                if br.status == COMPLETE:
                    self.root_complete = True
                    self.root_snapshot = br.snapshot
            self._place(br, parent_id)
            if br.status == COMPLETE:
                queue.extend(self._register(br, parent_id))
        return self._report()

    # ---- 报告与状态 -------------------------------------------------------

    def _site_conflicts(self):
        accepted = {}
        for s in self.sites:
            accepted.setdefault(s["id"], []).append(s)
        discarded = {}
        for nid in self._order:
            for a in self.branches[nid].discarded:
                for sid in a.site_ids:
                    discarded.setdefault(sid, []).append(
                        {"branch": nid, "snapshot": a.snapshot, "outcome": a.outcome})
        dual = [{"site": sid, "locations": locs} for sid, locs in accepted.items()
                if len({l["branch"] for l in locs}) > 1]
        moves = []
        for sid, dlocs in discarded.items():
            alocs = accepted.get(sid, [])
            involved = {d["branch"] for d in dlocs} | {l["branch"] for l in alocs}
            if len(involved) > 1:
                moves.append({"site": sid, "accepted": alocs, "discarded": dlocs})
        return dual, moves

    def _same_name_notes(self):
        by_name = {}
        for s in self.sites:
            if s.get("name"):
                by_name.setdefault(s["name"], set()).add(s["id"])
        return [{"name": n, "ids": sorted(ids)} for n, ids in by_name.items() if len(ids) > 1]

    def _excluded_prior(self):
        out = []
        prior_root = self.prior.get("root_snapshot")
        if prior_root and self.root_snapshot and prior_root != self.root_snapshot:
            out.append({"node": "*", "reason": f"根代次由 {prior_root} 变为 {self.root_snapshot}"})
        for nid, rec in (self.prior.get("branches") or {}).items():
            if nid == self.root_id:
                continue  # 根每轮必重新读取，作为代次基准
            cur = self.branches.get(nid)
            if cur is not None and cur.reused:
                continue
            if cur is not None:
                out.append({"node": nid, "reason": (
                    f"既往结果未纳入：已重新采集（既往代次 {rec.get('snapshot')}，"
                    f"本轮代次 {cur.snapshot or '未完成'}）")})
            else:
                out.append({"node": nid, "reason": "既往结果未纳入：本轮目录未发现该节点"})
        return out

    def _verdict(self):
        gaps = []
        for nid in self._order:
            br = self.branches[nid]
            if br.status != COMPLETE:
                path = "/".join(self.paths.get(nid, (nid,)))
                label = "根分支" if nid == self.root_id else f"分支 {path}"
                gaps.append(f"{label} 未采完：{reason_text(br.reason)}；该分支及其下级范围未覆盖")
        scopes = {}
        for nid in self._order:
            br = self.branches[nid]
            if br.status == COMPLETE:
                scopes.setdefault(br.snapshot, []).append(nid)
        mixed = len(scopes) > 1
        complete = self.root_complete and not gaps and not mixed
        explanation = []
        if complete:
            explanation.append(f"所有分支均在代次 {self.root_snapshot} 内采完，本轮可宣称完整。")
        else:
            explanation.append("本轮不能宣称完整。")
            if mixed:
                parts = "；".join(f"{g}：{'、'.join(nodes)}" for g, nodes in scopes.items())
                explanation.append(f"采纳结果跨代次（{parts}），业务方不接受混代次的完整结果。")
            explanation.extend(gaps)
        return {
            "complete": complete,
            "generation": self.root_snapshot if complete else None,
            "mixed": mixed,
            "scopes": scopes,
            "gaps": gaps,
            "explanation": explanation,
        }

    def _report(self):
        dual_sites, moves = self._site_conflicts()
        branches = {}
        for nid in self._order:
            br = self.branches[nid]
            branches[nid] = {
                "node_id": nid,
                "path": "/".join(self.paths.get(nid, (nid,))),
                "url": br.url,
                "status": br.status,
                "reason": br.reason,
                "reason_text": reason_text(br.reason),
                "snapshot": br.snapshot,
                "pages": br.pages,
                "restarts": br.restarts,
                "requests": br.requests,
                "reused": br.reused,
                "digest": br.digest,
                "declared_parent": br.declared_parent,
                "parents": self.parents.get(nid, []),
                "sites": [i["id"] for i in br.sites],
                "children": [i["id"] for i in br.children],
                "discarded": [
                    {"snapshot": a.snapshot, "pages": a.pages,
                     "outcome": a.outcome, "site_ids": a.site_ids}
                    for a in br.discarded
                ],
            }
        return {
            "requests_used": self.requests_used,
            "max_requests": self.max_requests,
            "root": {"node_id": self.root_id, "snapshot": self.root_snapshot,
                     "complete": self.root_complete},
            "branches": branches,
            "sites": self.sites,
            "dual_parents": {
                "sites": dual_sites,
                "branches": self.branch_dual_parents,
                "declared_parent_mismatch": self.declared_mismatch,
                "move_observations": moves,
                "same_name_distinct_ids": self._same_name_notes(),
            },
            "back_edges": self.back_edges,
            "excluded_prior_records": self._excluded_prior(),
            "verdict": self._verdict(),
        }

    def state(self):
        """跨轮状态：完整分支的记录即覆盖凭据，供续跑复用。"""
        return {
            "version": 1,
            "root_snapshot": self.root_snapshot,
            "root_complete": self.root_complete,
            "branches": {
                nid: {
                    "node_id": br.node_id, "url": br.url, "status": br.status,
                    "reason": br.reason, "snapshot": br.snapshot, "pages": br.pages,
                    "digest": br.digest, "sites": br.sites, "children": br.children,
                    "declared_parent": br.declared_parent,
                    "parents": self.parents.get(nid, []),
                    "restarts": br.restarts, "requests": br.requests,
                }
                for nid, br in self.branches.items()
            },
        }


def format_text(rep):
    """把报告渲染为中文文本，含游标过期 / 目录迁移 / 回边的组合说明。"""
    lines = []
    root = rep["root"]
    lines.append(
        f"根节点 {root['node_id']}：代次 {root['snapshot']}"
        f"（{'完整' if root['complete'] else '未采完'}）；"
        f"本轮请求 {rep['requests_used']}/{rep['max_requests']}")
    lines.append("分支：")
    for nid, b in rep["branches"].items():
        if nid == root["node_id"]:
            continue
        tag = "完整" if b["status"] == COMPLETE else "未采完"
        src = "复用既往凭据" if b["reused"] else f"{b['requests']} 次请求"
        extra = f"，重启 {b['restarts']} 次" if b["restarts"] else ""
        lines.append(f"- {b['path']}：{tag} @ {b['snapshot']}（{src}{extra}；{b['reason_text']}）")
        for d in b["discarded"]:
            lines.append(
                f"    废弃半程：代次 {d['snapshot']}，{len(d['pages'])} 页，"
                f"结局 {d['outcome']}，涉及网点 {d['site_ids'] or '无'}")
    lines.append("网点：")
    for s in rep["sites"]:
        lines.append(f"- {s['id']}（{s.get('name') or '无名'}） 路径 {s['path']} @ {s['snapshot']}")
    dp = rep["dual_parents"]
    notes = []
    for nid, b in rep["branches"].items():
        for d in b["discarded"]:
            if d["outcome"] == CURSOR_EXPIRED:
                notes.append(
                    f"分支 {b['path']} 在代次 {d['snapshot']} 翻页时游标过期（410），"
                    f"重启后落到代次 {b['snapshot'] or '未知'}；"
                    f"{d['snapshot']} 的 {len(d['pages'])} 页半程未与后半拼接。")
            elif d["outcome"] == SNAPSHOT_CHANGED:
                notes.append(
                    f"分支 {b['path']} 翻页中途代次由 {d['snapshot']} 变化，该半程已废弃。")
    for m in dp["move_observations"]:
        acc = "、".join(f"{l['branch']}@{l['snapshot']}" for l in m["accepted"]) or "无"
        dis = "、".join(f"{l['branch']}@{l['snapshot']}（已废弃半程）" for l in m["discarded"]) or "无"
        notes.append(
            f"网点 {m['site']} 在目录迁移中现于多处：采纳 {acc}；另见 {dis}。"
            "双重父关系如实保留，未按名称合并，也未静默挑一边。")
    for s in dp["sites"]:
        locs = "、".join(f"{l['branch']}@{l['snapshot']}" for l in s["locations"])
        notes.append(f"网点 {s['site']} 在采纳结果内有双重父关系：{locs}，全部保留。")
    for b in dp["branches"]:
        notes.append(
            f"分支 {b['node']} 有多个父级 {b['parents']}，仅沿首个发现父级展开，全部父级如实记录。")
    for m in dp["declared_parent_mismatch"]:
        notes.append(
            f"分支 {m['node']} 自述父级 {m['declared_parent']} 与发现父级 "
            f"{m['discovered_parents']} 不一致，按移动信号记录。")
    for e in rep["back_edges"]:
        kind = "自身" if e["kind"] == "self" else "祖先"
        notes.append(f"{e['from']} 列出指向{kind} {e['to']} 的分支，识别为回边，未重复入队。")
    for n in dp["same_name_distinct_ids"]:
        notes.append(f"名称「{n['name']}」对应不同网点身份 {n['ids']}，按身份区分，未合并。")
    if notes:
        lines.append("说明（游标过期 / 目录迁移 / 回边）：")
        lines.extend("- " + n for n in notes)
    if rep["excluded_prior_records"]:
        lines.append("既往结果未纳入：")
        lines.extend(f"- {e['node']}：{e['reason']}" for e in rep["excluded_prior_records"])
    lines.append("结论：")
    lines.extend("- " + x for x in rep["verdict"]["explanation"])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python3 -m app.collect", description="县乡村服务网点目录采集器")
    parser.add_argument("--config", default="fixtures/root.json", help="入口与预算配置")
    parser.add_argument("--routes", default="fixtures/responses.json", help="重放的接口响应")
    parser.add_argument("--state", default="runtime/collection_state.json", help="跨轮状态与覆盖凭据")
    parser.add_argument("--report", default="runtime/report.json", help="本轮报告输出")
    args = parser.parse_args(argv)
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    state_path = Path(args.state)
    prior = None
    if state_path.exists():
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
        if loaded.get("version") == 1:
            prior = loaded
    collector = Collector(
        Replay(args.routes),
        max_requests=cfg["max_requests"],
        max_branch_restarts=cfg["max_branch_restarts"],
        prior=prior,
    )
    report = collector.run(cfg["entry"])
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(collector.state(), ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.report).write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(format_text(report))


if __name__ == "__main__":
    main()
