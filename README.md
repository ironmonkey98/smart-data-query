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
python3 -m unittest "/Users/yehong/机器狗资料/cloud-robot-platform/OUTPUT_DIR/smart-data-query/tests/test_smart_data_query.py"
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

尚未覆盖：
- 管理层日报模板化输出
- 基于业务知识库的深层归因链
