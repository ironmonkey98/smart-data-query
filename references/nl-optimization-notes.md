# smart-data-query 自然语言优化补充

## 目标

把“用户一句模糊提问”变成“可执行、可验证、可解释”的分析任务。

## 推荐中间表示

建议在脚本里增加一个中间 JSON，而不是直接让模型吐 SQL：

```json
{
  "intent": "trend_compare",
  "metric": "paid_amount",
  "dimensions": ["region"],
  "time_range": "last_30_days",
  "time_granularity": "day",
  "filters": [
    {"field": "region", "operator": "in", "value": ["华东", "华南"]},
    {"field": "order_status", "operator": "=", "value": "paid"}
  ],
  "output": ["table", "line_chart", "summary"],
  "assumptions": ["成交额按 paid_amount 计算", "排除退款订单"]
}
```

## 为什么要加这一层

- 便于检查槽位是否完整
- 便于追问缺失字段
- 便于替换 SQL 生成器
- 便于支持 CSV / Excel，不把分析能力绑死在 SQL 上

## 推荐追问策略

每次最多追问 1 个最关键问题：

- “最近”不清楚时，优先追问时间范围
- “销售怎么样”不清楚时，优先追问指标口径
- “对比一下”不清楚时，优先追问对比对象

不要一次追问 5 个问题，否则交互会崩。

## 推荐提示词骨架

```text
你是数据分析助手。你的任务不是直接写 SQL，而是先把用户问题转换为结构化分析任务。

请完成以下步骤：
1. 识别分析意图
2. 提取指标、维度、时间范围、过滤条件
3. 用 schema 和 glossary 做字段映射
4. 标记缺失信息和假设
5. 只输出 JSON，不输出 SQL
```

## V1 判断标准

如果一个自然语言问题能稳定经过这 4 步，就算 V1 合格：

1. 识别出意图
2. 提取出关键槽位
3. 给出字段映射或假设
4. 在缺信息时提出最关键的追问
