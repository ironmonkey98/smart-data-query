# smart-data-query

最小可运行版本，支持：
- 自然语言问题规范化
- CSV 数据读取
- MySQL 配置式读取
- 聚合统计与趋势对比
- SVG 折线图输出
- 停车经营收入归因
- 停车经营异常诊断与建议
- 停车经营车流与利用率分析
- 停车经营管理层综合周报
- 停车经营管理层日报
- Web 独立管理层报表页
- 规则增强解释
- OpenAI 兼容接口增强解释
- 基于 session 文件的多轮追问

## 目录

```text
smart-data-query/
├── main.py
├── SKILL.md
├── README.md
├── data/
│   ├── 5个停车场数据/
│   ├── sample_parking_ops.csv
│   └── sample_sales.csv
├── references/
│   ├── db-schema.md
│   ├── term-glossary.md
│   └── nl-optimization-notes.md
├── scripts/
│   ├── connect-db.py
│   ├── connect_db.py
│   ├── sql-generator.py
│   ├── sql_generator.py
│   ├── chart-render.py
│   ├── chart_render.py
│   ├── build_parking_ops_from_excels.py
│   └── smart_query.py
└── tests/
    ├── test_codex_upgrade.py
    ├── test_real_parking_ingestion.py
    └── test_report_page.py
```

## 运行测试

```bash
python3 -m unittest "tests/test_codex_upgrade.py" "tests/test_real_parking_ingestion.py" "tests/test_report_page.py" -v
```

## 真实停车场数据接入

`data/5个停车场数据/` 下是 5 个车场的原始 Excel 明细：
- `流水数据/`：临停收费流水
- `通行数据/`：车辆通行明细
- `车场基础数据.xlsx`：总车位

当前应用默认使用 [sample_parking_ops.db](/Users/yehong/smart-data-query%203/data/sample_parking_ops.db) 作为停车经营主数据源；[sample_parking_ops.csv](/Users/yehong/smart-data-query%203/data/sample_parking_ops.csv) 保留为派生日级结果夹具。

重新生成命令：

```bash
python3 "scripts/build_parking_ops_from_excels.py"
```

生成结果：
- `data/sample_parking_ops.db`
  - `parking_lots`
  - `parking_payment_records`
  - `parking_passage_records`
- `data/sample_parking_ops.csv`
  - 由 SQLite 多表联查派生的日级指标结果

联查口径：
- `total_revenue` / `temp_revenue`：按 `parking_payment_records.actual_amount` 日汇总
- `entry_count`：按 `parking_passage_records` 入场记录计数
- `occupancy_rate`：按停留分钟数 ÷ `总车位 * 1440` 估算
- `payment_failure_rate`：按收费结果中的失败记录占比估算
- `free_release_count`：临时车且应收、实收均为 0 的通行记录

说明：
- 真实数据当前不包含可靠的 `monthly_revenue`、`abnormal_open_count` 原始来源，汇总时按 `0` 处理。
- 对“今天/最近7天/本周”这类相对时间问法，若系统日期晚于样本数据，执行层会自动对齐到数据最新日期，保证日报/周报仍可生成。

## 直接运行程序

推荐直接用包根目录入口：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/run-output"
```

这样不需要关心 `scripts/` 目录。

## 运行示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_sales.csv" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "对比最近30天华东和华南的成交额趋势，排除退款，做个折线图" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/demo-output"
```

运行后会生成：
- `summary.json`
- `chart.svg`

## MySQL 配置示例

参考文件：[sample_mysql_config.json](/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_mysql_config.json)

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "parking_user",
  "password": "replace_me",
  "database": "parking_ops",
  "query": "SELECT parking_lot, total_revenue FROM parking_daily_stats LIMIT 10"
}
```

MySQL 运行示例：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type mysql \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_mysql_config.json" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/mysql-demo-output"
```

说明：
- 运行真实 MySQL 需要本机安装 `pymysql` 或 `mysql-connector-python`
- 配置里必须提供 `query` 或 `table`

## 停车经营分析示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/parking-demo-output"
```

这个模式下输出会包含：
- `analysis.primary_lot`
- `analysis.diagnosis`
- `analysis.recommendations`
- `executive_summary`

## 车流与利用率分析示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场车流和利用率下滑最明显" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/flow-demo-output"
```

## 管理层周报示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "生成最近7天停车经营周报，给管理层看" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/report-demo-output"
```

Web 端输入 `生成最近7天停车经营周报，给管理层看` 时，会直接生成独立报表页入口。

## 管理层日报示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "给老板看下今天经营情况" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/daily-report-output"
```

Web 端支持这些口语化问法：
- `给老板看下今天经营情况`
- `做个停车经营日报给管理层`
- `哪个场子今天有问题`

## 缺槽位追问示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/clarify-demo-output"
```

输出会返回：
- `needs_clarification`
- `clarifying_question`

## 多轮追问示例

第一轮：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output" \
  --session-file "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output/session.json"
```

第二轮追问：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type sqlite \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.db" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --follow-up-question "为什么是 B 停车场？" \
  --session-file "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output/session.json" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output"
```

## LLM 增强解释

启用方式：

```bash
export OPENAI_API_KEY="your-key"
export OPENAI_BASE_URL="https://api.openai.com/v1"
export OPENAI_MODEL="gpt-4o-mini"
```

然后加上：

```bash
--enable-llm
```

## 当前边界

CSV 场景已经直接可跑。

MySQL 已支持真实接入，但运行前需要本机安装 `pymysql` 或 `mysql-connector-python`，并提供可访问的真实库配置。

停车经营分析第一版已覆盖：
- 收入分析
- 异常诊断
- 车流与利用率分析
- 管理层综合经营周报
- 管理层经营日报
- 管理层独立网页报表
- LLM 优先的停车经营语义拆解（失败时回退本地规则）

尚未覆盖：
- 基于业务知识库的深层归因链
- `compare_periods` 当前仅稳定支持 `sales` 数据源

## 报表链路

停车经营日报/周报当前链路如下：
- 用户自然语言问题 → `scripts/sql_generator.py` 先生成 `semantic_plan`
- `semantic_plan` 经过 schema 校验与 mapping，转换成兼容现有执行层的 task；失败时回退到最小规则解析
- `scripts/parking_analyst.py` 生成日报或周报结构化结果
- `server.py` 持久化报表 payload，返回 `report_url`
- 前端通过 `/api/report/{report_id}` 渲染独立管理层报表页

说明：
- 当前只覆盖停车经营域；销售域仍保持原有规则链路。
- 停车经营 NLP 现为“第一性原则语义模型 + intent 兼容映射 + 最小规则兜底”。
- task 里会保留 `semantic_plan`，执行层现已优先消费 `semantic_plan`，`intent` 只作为兼容 fallback。
- `日报` 默认按“今天/今日”处理；在样例数据中若当天无数据，会回落到最近一个可用日期做演示。
