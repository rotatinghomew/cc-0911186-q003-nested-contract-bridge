# 嵌套事件 Schema 演进下的分层兼容投影

报名事件的版本化 Schema、兼容投影和导出清单基线，包含旧版与新版合成事件。

仓库内的实现使用 Python 3.11 标准库和 SQLite，数据只写入命令行指定的本地数据库及导出目录。`lakeapp.storage` 保存原始接收、业务状态和版本清单；`lakeapp.contract` 负责输入约定；`lakeapp.ingest` 接收来源批次；`lakeapp.derive` 生成当前基线支持的事实；`lakeapp.publish` 读取并写出可追溯版本；`lakeapp.cli` 提供本地操作入口。

## 运行

在 Python 3.11 环境中执行 `python3 -m unittest discover -s tests -v` 运行测试，执行 `python3 -m compileall -q lakeapp tests` 检查构建和语法。示例数据是本地合成记录，不依赖外部服务或联网。

## 现有契约

输入批次包含来源名、批次标识、游标和记录数组。原始记录以 JSON 保存，重复批次不会再次产生事实；业务键相同但内容不同的记录会保留冲突信息。事实查询按业务日期和版本清单读取，清单中的文件摘要与行数用于发现导出内容被替换。数据库初始化由运行入口完成，已有数据不会因重新打开而丢失。

## 事件版本

- `v1`：`contact`（单一联系方式）与 `source`。未声明 `schema_version` 且不含新字段的记录按 `v1` 处理。
- `v2`：`contact_channels`（`[{type,value}]` 数组）与 `enrollment_source`（`{channel,campaign,medium,detail}`）。记录可显式声明 `schema_version`；只出现新字段时自动识别为 `v2`。
- 未知 `schema_version`、未知字段（含嵌套字段）、跨版本字段、非法渠道类型都会按单条记录拒收，不会摊平进事实，也不会让整批数据丢失。

`presence` 区分三种输入状态：字段不存在（`missing`）、显式 `null`（`null`）、有值（`present`）；不适用于该版本的字段记为 `not_applicable`。导出同时给出 `origin`（`input` / `input_null` / `derived` / `default` / `not_collected`）说明值的来源。`available` 单独表示导出里是否真的带有可用值，因此显式空值不会被读成"从未采集"。

只有 `METRIC_FIELDS`（当前为 `value`）可以参与指标；新字段必须连同版本一起加入契约后才能进入指标。

## 分层与追溯

- **ODS**：`raw_batches` 原样保存每个批次（含游标、记录数组、接收时间），`raw` 命令回读并给出 `payload_sha256`，可据此重放。
- **拒收区**：无法安全转换的记录写入 `quarantine`，保留原始记录、输入版本、`error_code`、`details`、尝试次数与最近一次错误。`quarantine` 命令按来源/批次查询；`replay` 用当前解析器重跑，仍不可接受的记录继续可见，已接受的记录留下 `replayed_at`。重复投递同一批次不会重复产生拒收行，也不会覆盖已重放状态。
- **DWD**：`facts` 保留旧列 `contact`、`source`（`business_key` + `contact` + `source` 三列形状不变）。旧记录缺失 `source` 时仍默认为 `unknown`；显式 `null` 保持 `null`。多渠道 upsert 无法无损映射到单值 `contact`，会被拒收而不是任选其一。
- **ADS**：`export` 与版本清单里的 `ads` 节点提供 `contact_channels`、`enrollment_source`，以及每个字段的 `presence`、`available`、`origin`、`derived_from`、`metric`，另附 `provenance`（来源、批次、游标、事实版本、更新时间）。
- **版本清单**：`publish` 冻结一版，同时给出 DWD 行摘要、ADS 摘要、被拒收记录摘要和行数；`verify` 比对摘要，发现导出被替换。

## 命令

```bash
python3 -m lakeapp.cli data.db receive  '{"source":"enrollment","batch_id":"b1","cursor":1,"records":[...]}'
python3 -m lakeapp.cli data.db raw      '{"source":"enrollment","batch_id":"b1"}'
python3 -m lakeapp.cli data.db facts
python3 -m lakeapp.cli data.db fact     '{"source":"enrollment","business_key":"member-1"}'
python3 -m lakeapp.cli data.db quarantine '{"source":"enrollment"}'
python3 -m lakeapp.cli data.db replay   '{"source":"enrollment"}'
python3 -m lakeapp.cli data.db export
python3 -m lakeapp.cli data.db publish  '{"name":"daily","version":1}'
python3 -m lakeapp.cli data.db read     '{"name":"daily","version":1}'
python3 -m lakeapp.cli data.db verify   '{"name":"daily","version":1}'
```

## 目录职责

`lakeapp/storage.py` 负责事务、表结构与旧库迁移，`lakeapp/contract.py` 负责版本识别和记录约束，`lakeapp/ingest.py` 负责接收、来源进度、原始批次回读、拒收与重放，`lakeapp/derive.py` 负责从事件生成 DWD 事实，`lakeapp/projection.py` 负责 DWD 旧列投影和 ADS 导出，`lakeapp/publish.py` 负责版本清单、摘要与回读，`lakeapp/cli.py` 负责把这些业务操作组合给本地使用者。
