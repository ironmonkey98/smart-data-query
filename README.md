# smart-data-query

停车经营智能分析 Agent —— 用自然语言对话，Claude 自主编写 SQL、分析数据、生成报告。

## 核心能力

- **Claude 直接写 SQL**：不依赖关键词匹配，Claude 根据 schema 动态生成任意查询
- **ReAct 分析模式**：复杂问题先输出分析计划，再分步执行
- **主动澄清**：问题不明确时追问，绝不随意猜测
- **跨 session 记忆**：重要发现持久化到 `memory/insights.jsonl`，下次对话自动加载
- **SSE 流式响应**：实时显示 Claude 思考过程和 SQL 执行状态

## 快速启动

```bash
# macOS / Linux
./start.sh

# Windows
run_windows.bat
```

访问 `http://localhost:8000`

**环境变量**（`.env` 文件或 shell export）：

```
ANTHROPIC_AUTH_TOKEN=sk-ant-...   # 或 ANTHROPIC_API_KEY
ANTHROPIC_BASE_URL=...            # 可选，兼容代理
ANTHROPIC_MODEL=claude-opus-4-6   # 可选
```

## 项目结构

```
smart-data-query/
├── server.py                          # FastAPI + Claude Tool Use 主服务
├── static/index.html                  # 单页对话界面
├── start.sh / run_windows.bat         # 启动脚本
├── requirements-server.txt
├── data/
│   ├── sample_parking_ops.db          # SQLite 主数据库（3 张表）
│   └── 5个停车场数据/                 # 原始 Excel 源文件
├── scripts/
│   ├── connect_db.py                  # SQLite 执行层
│   └── build_parking_ops_from_excels.py  # 从 Excel 重建数据库
├── references/
│   ├── db-schema.md                   # 表结构定义（注入 System Prompt）
│   └── term-glossary.md              # 业务术语对照
└── memory/
    └── insights.jsonl                 # 持久化的分析记忆
```

## 数据库结构

`sample_parking_ops.db` 包含 3 张表：

| 表名 | 说明 |
|------|------|
| `parking_lots` | 车场基础信息（名称、总车位数） |
| `parking_payment_records` | 支付流水（金额、支付结果、时间） |
| `parking_passage_records` | 通行记录（入场/离场、停留时长） |

从原始 Excel 重建：

```bash
python3 scripts/build_parking_ops_from_excels.py
```

## Agent 架构

```
用户问题
  └─→ Claude（Tool Use 循环，最多 8 轮）
         ├─→ list_data_sources   查看可用数据源和 schema
         ├─→ execute_sql         写 SQL → 执行 → 返回结果
         ├─→ reflect             输出分析计划（复杂问题）
         └─→ save_insight        保存重要发现到 memory/
```

`server.py` 将完整 schema（`db-schema.md` + `term-glossary.md`）注入 System Prompt，Claude 可自主生成任意 SELECT / CTE 查询。

## 对话示例

**澄清追问**
> "帮我看看停车场数据"
> → Claude 追问时间范围、车场、分析重点，不猜测

**执行 SQL 分析**
> "最近 7 天各车场支付失败率排名"
> → Claude 写 SQL → 执行 → 返回排名结果

**综合报告（ReAct 模式）**
> "给我一份最近 30 天停车场综合分析报告"
> → 先输出「分析计划」→ 多次执行 SQL → 生成完整报告

**跨 session 记忆**
> "帮我记住：3 个车场连续 30 天零收入"
> → 写入 `memory/insights.jsonl`，重启服务后仍可引用

## 数据说明

- 数据时间范围：2024-10 ～ 2025-12
- 覆盖车场：厦门七星大厦、高林居住区、中航城 C07、国金广场、软件园三期 A 区
- 相对时间问法（"最近 7 天"/"本月"）自动对齐到数据最新日期
