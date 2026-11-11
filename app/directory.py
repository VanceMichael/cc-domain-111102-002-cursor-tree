from dataclasses import dataclass
import json

@dataclass(frozen=True)
class Page:
    node_id: str
    parent_id: str | None
    snapshot: str
    items: tuple
    next_cursor: str | None

def decode_page(body):
    value = json.loads(body)
    required = {"node_id", "parent_id", "snapshot", "items", "next_cursor"}
    if not required <= value.keys() or not isinstance(value["items"], list):
        raise ValueError("目录页结构不完整")
    for item in value["items"]:
        if item.get("kind") not in {"branch", "site"} or not item.get("id"):
            raise ValueError("目录项类型或身份无效")
    return Page(value["node_id"], value["parent_id"], value["snapshot"], tuple(value["items"]), value["next_cursor"])

def read_page(transport, url):
    response = transport.get(url)
    if response.status != 200:
        raise ValueError("目录接口状态：" + str(response.status))
    return decode_page(response.body)
