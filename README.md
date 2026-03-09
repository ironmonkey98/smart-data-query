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
│   └── smart_query.py
└── tests/
    └── test_smart_data_query.py
```

## 运行测试

```bash
python3 -m unittest "tests/test_codex_upgrade.py" "tests/test_report_page.py" -v
```

## 直接运行程序

推荐直接用包根目录入口：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
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
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
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
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场车流和利用率下滑最明显" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/flow-demo-output"
```

## 管理层周报示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "生成最近7天停车经营周报，给管理层看" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/report-demo-output"
```

Web 端输入 `生成最近7天停车经营周报，给管理层看` 时，会直接生成独立报表页入口。

## 管理层日报示例

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
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
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
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
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
  --schema "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/db-schema.md" \
  --glossary "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/references/term-glossary.md" \
  --question "最近7天哪个车场收入下滑最明显，原因是什么" \
  --output-dir "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output" \
  --session-file "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/follow-up-demo-output/session.json"
```

第二轮追问：

```bash
python3 "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/main.py" \
  --source-type csv \
  --source "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/data/sample_parking_ops.csv" \
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
- 用户自然语言问题 → `scripts/sql_generator.py` 先调用 LLM Planner 做结构化拆解
- LLM 结果经过 schema 校验与字段归一；失败时回退到最小规则解析
- `scripts/parking_analyst.py` 生成日报或周报结构化结果
- `server.py` 持久化报表 payload，返回 `report_url`
- 前端通过 `/api/report/{report_id}` 渲染独立管理层报表页

说明：
- 当前只覆盖停车经营域；销售域仍保持原有规则链路。
- 停车经营 NLP 现为“LLM 主导 + 最小规则兜底”，术语表主要作为提示上下文，不再承担主要分支逻辑。
- `日报` 默认按“今天/今日”处理；在样例数据中若当天无数据，会回落到最近一个可用日期做演示。
