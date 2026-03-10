# Gap Analysis：Agent 能力现状 vs 目标

## 评估结论速览

| 目标 | 状态 | 评分 |
|------|------|------|
| 1. Claude SDK 集成，Agent 框架运行 | ✅ 已实现 | 100% |
| 2. Agent 能自主分析推理和决策 | ⚠️ 部分实现 | 50% |
| 3. Agent 自己编写 SQL | ❌ 未实现 | 0% |
| 4. 取消硬编码 | ❌ 大量硬编码 | 15% |
| 5. 无法明确时澄清，绝不猜测 | ⚠️ 部分实现 | 40% |

---

## 目标 1：Claude SDK 集成 ✅

**已实现：**
- `server.py` 使用 `anthropic.Anthropic` 客户端 + `client.messages.stream` 流式调用
- Tool Use 循环（最多 5 轮）：Claude 自主决定调哪个工具、何时停止
- SSE 实时推流到前端
- Session 管理（内存，2小时 TTL）

**结论：Claude SDK 的 Agent Tool Use 框架已正确实现。**

---

## 目标 2：Agent 自主分析推理和决策 ⚠️

**已实现的部分：**
- Claude 能自主决策：选哪个数据源、调哪个工具、是否需要追问
- `SYSTEM_PROMPT` 中有"直接分析规则"：当 `rows_sample` 已有数据时，Claude 直接对数据做拐点/极值/变化幅度计算，不再重复调工具
- 多轮追问时 Claude 自主改写问题（含完整时间、区域上下文）

**未实现/薄弱的部分：**
- **分析逻辑仍在服务端硬编码**：`parking_analyst.py` 包含大量规则引擎（判断哪个车场下滑最多、阈值是 0.015/0.92 等），Claude 看不到这些逻辑，也无法推翻它
- **Claude 的"决策"仅限于工具调用层**：具体分析计算（收入归因、异常诊断、效率排名）都在服务端完成后以文本摘要形式送给 Claude，Claude 只做叙述综合
- `reflect` 工具（分析计划）是 CODEX_UPGRADE 中待实施的内容，当前尚不存在

**本质问题：Claude 是"解说员"，不是"分析师"。**

---

## 目标 3：Agent 自己编写 SQL ❌

**当前实现：**

```
用户问题 → sql_generator.py（关键词匹配）→ task JSON
task JSON → connect_db.py → 固定 SQL 模板（5 个 query_profile 之一）
```

`connect_db.py` 中有 5 个完全硬编码的 SQL 函数：
- `_load_sqlite_parking_daily_rows()` — 日级汇总联查，永远执行这一条 SQL
- `_load_sqlite_payment_passage_reconciliation_by_date()` — 支付/通行对账
- `_load_sqlite_payment_passage_reconciliation_by_plate()` — 按车牌对账
- `_load_sqlite_lot_capacity_efficiency_ranking()` — 车场效率排名
- `_load_sqlite_payment_method_risk_breakdown()` — 支付方式风险

Claude **从未看到 SQL**，更不会编写 SQL。`query_profile` 的路由完全由 `sql_generator.py` 的关键词匹配决定。

**要实现"Agent 自己写 SQL"，需要：**
1. 把 db-schema.md 的表结构暴露给 Claude（作为 tool 或 system prompt 的一部分）
2. 新增 `execute_sql` 工具，接受 Claude 生成的 SQL 字符串并执行
3. 把 5 个 query_profile 对应的业务问题改写为 Claude 的 prompt 引导

---

## 目标 4：取消硬编码 ❌

**硬编码集中在三个文件：**

### `sql_generator.py`
- 区域列表硬编码：`("华东", "华南", "华北", "华中", "华西")`
- 意图判断：一堆 `if "停车" in question and "周报" in question` 的条件链
- 时间范围关键词：有限的中文词表
- 指标别名：`METRIC_ALIASES` 字典（虽然有 glossary 参数，但 glossary 实际上没有被完整解析）

### `parking_analyst.py`
- 异常阈值全部硬编码：
  - 支付失败率 ≥ 0.015 才告警
  - 入场车次下降到 0.92 以下才报警
  - 异常开闸增加 > 30 次才标高
  - 利用率下降 > 0.05 才报警
- 分析逻辑固定：每个 profile 对应固定的"先看什么、再看什么"

### `connect_db.py`
- SQL 模板完全固定，无法应对新的业务问题
- `query_profile` 路由逻辑写死，新增问题类型需要改代码

---

## 目标 5：无法明确时澄清，绝不猜测 ⚠️

**已实现的部分：**
- 停车场问题缺少时间范围时：`needs_clarification: true` + `clarifying_question` 返回给 Claude，Claude 会转达用户
- Claude 的 `SYSTEM_PROMPT` 有"必须有明确时间范围，否则先向用户确认"的指令

**存在的猜测行为：**

1. **意图猜测**：`sql_generator.py` 对模糊问题用 `if/elif` 链猜意图，没有回退澄清机制
   - 例："分析停车场情况" → 直接猜 `parking_revenue_analysis`，不会问"您指收入、车流还是异常？"

2. **指标猜测**：`_detect_metric()` 找不到匹配时 fallback 到 `("paid_amount", "成交额")`，从不澄清

3. **区域猜测**：销售问题没提区域时，`dimensions = []`，静默聚合全部区域，不告知用户

4. **数据对齐猜测**：当系统日期晚于样本数据时，自动对齐到最新可用日期，用户不知道发生了偏移

**根本原因**：澄清逻辑在 Python 规则层实现，Claude 无法介入这个判断过程。只有当规则层明确设置 `needs_clarification: true`，Claude 才知道需要澄清。

---

## 改造路线建议

### 短期（不破坏现有接口）

| 改动 | 文件 | 效果 |
|------|------|------|
| 在 `query_data` tool 描述中加入 db-schema 摘要 | `server.py` | Claude 能理解数据结构 |
| 新增 `execute_sql` 工具（Claude 写 SQL → 执行） | `server.py` + `connect_db.py` | 实现目标 3 |
| `sql_generator` 对低置信度意图返回 `needs_clarification` | `sql_generator.py` | 减少猜测 |
| 先执行 CODEX_UPGRADE.md（reflect/compare_periods/save_insight）| — | 实现目标 2 的 ReAct 模式 |

### 中期（架构升级）

- 去掉 `parking_analyst.py` 的硬编码阈值，改为将原始数据 + 业务规则文档传给 Claude 让其推理
- 把 `connect_db.py` 的固定 SQL 模板迁移为 Claude 动态生成，`execute_sql` 工具执行
- `sql_generator.py` 降级为"schema 预处理 + 字段补全"，核心意图理解移交 Claude

---

## 一句话结论

> 当前系统是**"有 Tool Use 的规则引擎"，不是"会写 SQL 的 Agent"**。
> Claude SDK 框架搭好了，但分析逻辑、SQL 生成、澄清决策权还在 Python 规则层，Claude 只负责最终叙述。
> **目标 3（自写 SQL）和目标 4（去硬编码）需要一次架构级重构才能满足。**
