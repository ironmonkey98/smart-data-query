# smart-data-query

停车经营智能分析 Agent —— 用自然语言对话，Claude 自主编写 SQL、分析数据、生成报告。

## 核心能力

- **Claude 直接写 SQL**：不依赖关键词匹配，Claude 根据 schema 动态生成任意查询
- **ReAct 分析模式**：复杂问题先输出分析计划，再分步执行
- **主动澄清**：问题不明确时追问，绝不随意猜测
- **跨 session 记忆**：重要发现持久化，下次对话自动加载
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
ANTHROPIC_AUTH_TOKEN=sk-ant-...
```

## 对话示例

**问题不明确 → 主动追问**
> "帮我看看停车场数据"
> → Claude 询问时间范围、车场、分析重点，不猜测不乱跑

**执行 SQL 分析**
> "最近 7 天各车场支付失败率排名"
> → Claude 写 SQL → 执行 → 返回排名结果

**综合报告（ReAct 模式）**
> "给我一份最近 30 天停车场综合分析报告"
> → 先输出「分析计划」→ 多次执行 SQL → 生成完整报告

**跨 session 记忆**
> "帮我记住：3 个车场连续 30 天零收入，需要紧急排查"
> → 写入记忆，重启服务后仍可引用

## 数据说明

数据库包含厦门 5 个停车场的真实运营数据（2024-10 ～ 2025-12）：
厦门七星大厦、高林居住区、中航城 C07、国金广场、软件园三期 A 区

如需从原始 Excel 重建数据库：

```bash
python3 scripts/build_parking_ops_from_excels.py
```
