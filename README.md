# 县乡村目录的游标失效与覆盖证明

这个 Python 项目保存采集侧的来源读取工具和合成资料。代码位于 app，示例输入位于 fixtures，现有读取工具的回归位于 tests。示例不代表任何真实单位的业务记录。

## 数据与接口

root.json 提供入口和预算。响应的 next_cursor 是完整续页 URL，HTTP 410 表示游标失效，可重新访问当前分支首页。snapshot 属于整轮目录，branch 的 url 可访问子目录，site 的 id 是稳定网点身份。响应序列包含 S1 到 S2 的切换及回边，用于研究目录连续性。

## 本地使用

使用 Python 3.12 或更新版本，当前工具仅依赖标准库。从项目根目录执行：

```sh
python3 -m app.inspect
python3 -m app.collect
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

读取命令输出当前素材的结构或单次读取结果。运行过程中产生的文件放在 runtime 中，与原始素材分开。公开示例仅使用 .invalid 域名，网络适配时应替换为已授权的来源。

## 采集器

`python3 -m app.collect` 按 root.json 的入口与预算（max_requests、max_branch_restarts）沿父子关系发现分支，不预置行政区列表：

- 一个分支的一遍翻页必须同属一个 snapshot；游标过期（410）允许从该分支首页重新申请，但不同代次的半程不拼接，废弃半程保留在报告中。
- 分支重启次数与整轮请求预算都有界；失效分支记为未采完并给出原因，不拖垮其他分支。
- 完整分支的游标链与内容摘要作为覆盖凭据写入 runtime/collection_state.json。续跑时仍属本轮根代次的完整分支直接复用；根代次变化时，报告列出哪些既往结果未纳入及原因。
- 节点身份以 id 为准，显示名称可重复；回边识别后不入队，移动产生的双重父关系单独报告，不按名称合并、不静默挑一边。
- 报告（runtime/report.json 与控制台）列出网点到根的来源路径、每个分支的代次与完成原因，并说明本轮能否宣称完整。

给定 fixtures 的组合例子：west 在 S1 翻页时游标过期，重启后落到 S2；v1 在迁移中同时现于 west@S1（已废弃半程）与 east@S1；east 含指向 root 的回边。因此本轮采纳结果跨 S1（root、east）与 S2（west）两个代次，结论为不能宣称完整，各代次覆盖范围分别列出。
