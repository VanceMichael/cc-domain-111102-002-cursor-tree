# 县乡村目录的游标失效与覆盖证明

这个 Python 项目保存采集侧的来源读取工具和合成资料。代码位于 app，示例输入位于 fixtures，现有读取工具的回归位于 tests。示例不代表任何真实单位的业务记录。

## 数据与接口

root.json 提供入口和预算。响应的 next_cursor 是完整续页 URL，HTTP 410 表示游标失效，可重新访问当前分支首页。snapshot 属于整轮目录，branch 的 url 可访问子目录，site 的 id 是稳定网点身份。响应序列包含 S1 到 S2 的切换及回边，用于研究目录连续性。

## 本地使用

使用 Python 3.12 或更新版本，当前工具仅依赖标准库。从项目根目录执行：

```sh
python3 -m app.inspect
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

读取命令输出当前素材的结构或单次读取结果。运行过程中产生的文件放在 runtime 中，与原始素材分开。公开示例仅使用 .invalid 域名，网络适配时应替换为已授权的来源。
