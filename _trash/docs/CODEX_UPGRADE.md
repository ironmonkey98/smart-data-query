# Codex 执行指令：smart-data-query Agent 能力升级

> 本文件是给 Codex 的完整实施指令，包含所有改动的精确位置和代码。
> 执行顺序：模块一(server.py) → 模块二(sql_generator.py) → 模块三(前端 index.html)

---

## 前置检查

- 项目根目录：`/Users/yehong/smart-data-query 3/`
- 不要修改 `scripts/smart_query.py` 的 `run_query()` 签名
- 所有新工具处理函数保持同步（非 async），由外层 `asyncio.to_thread` 包裹
- `memory/` 目录首次写入前自动创建

---

## 模块一：server.py 改动

### 1.1 在文件顶部 import 区域末尾追加 `datetime` import

在 `import time` 这一行**之后**追加：

```python
from datetime import datetime
```

### 1.2 扩展 TOOLS 列表

**定位**：`TOOLS: list[dict] = [` 这个列表，在最后一个 `}` 的右括号 `]` **之前**追加以下 3 个工具定义（即在 `query_data` 工具定义的结尾 `},` 之后）：

```python
    {
        "name": "reflect",
        "description": "在执行复杂多步分析前，先输出分析计划和待查清单。适用于：涉及多个数据源、需要对比多个时间段、或用户问题需要分解子任务时。",
        "input_schema": {
            "type": "object",
            "properties": {
                "plan": {
                    "type": "string",
                    "description": "用中文描述本次分析计划，列出将要执行的步骤",
                }
            },
            "required": ["plan"],
        },
    },
    {
        "name": "compare_periods",
        "description": "对同一数据源的两个时间段进行对比分析，返回环比/同比差值。适用于：本月vs上月、本周vs上周、今年vs去年。",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_name": {
                    "type": "string",
                    "enum": list(DATA_SOURCES.keys()),
                    "description": "数据源名称",
                },
                "question": {
                    "type": "string",
                    "description": "分析主题，如'各区域成交额'",
                },
                "period_a": {
                    "type": "string",
                    "description": "当前周期，如'最近30天'",
                },
                "period_b": {
                    "type": "string",
                    "description": "对比周期，如'前30天'或'上个月'",
                },
            },
            "required": ["source_name", "question", "period_a", "period_b"],
        },
    },
    {
        "name": "save_insight",
        "description": "将重要分析发现保存到记忆中，供后续对话引用。适用于：发现数据异常、确认某区域长期趋势、用户明确要求记住某个结论。",
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {
                    "type": "string",
                    "description": "主题标签，如'B停车场故障'",
                },
                "insight": {
                    "type": "string",
                    "description": "核心发现，一两句话",
                },
            },
            "required": ["topic", "insight"],
        },
    },
```

### 1.3 在 SYSTEM_PROMPT 末尾追加新规则

**定位**：`SYSTEM_PROMPT` 字符串的最后一行 `- 每次分析后主动提出 1-2 个追问建议"""` 中的 `"""` **之前**插入：

```
\n\n## 何时使用 reflect 工具\n以下情况必须先调用 reflect，再执行查询：\n- 用户问题涉及 2 个以上数据源\n- 需要对比多个时间段（如同比、环比）\n- 问题包含"综合"、"全面分析"、"报告"等词\nreflect 的 plan 字段要列出：将查询哪些数据源、用什么时间范围、对比逻辑是什么。\n\n## 何时使用 save_insight\n以下情况主动调用 save_insight：\n- 发现明确的数据异常（如支付失败率超过5%）\n- 用户表达"记住这个"、"下次提醒我"\n- 连续3次追问同一主题后确认的结论
```

即将末尾改为：

```python
SYSTEM_PROMPT = """你是智能数据分析助理，专门分析销售数据和停车场经营数据。

## 数据源选择
- 停车场相关问题（收入、车流、异常开闸、支付失败）→ parking_ops
- 销售/成交额/区域/产品对比问题 → sales
- 问题不明确时先调用 list_data_sources

## 查询前置要求
- 必须有明确时间范围（如最近7天/30天），否则先向用户确认
- 图表已单独展示给用户，无需在文字中描述图表细节

## 追问改写规则（重要）
用户的追问往往省略了上文提到的区域、指标、时间范围。调用 query_data 前，
必须将追问改写为包含完整要素的独立查询句。

改写示例：
  用户上文问过"最近30天华南成交额趋势"，追问"从哪天开始下滑的？"
  → 改写为："最近30天华南每日成交额明细，按日期列出"
  再调用 query_data(source_name="sales", question="改写后的完整问题")

## 直接分析规则（优先于重新查询）
若 tool_result 中包含 rows_sample（原始数据行），且用户追问属于以下类型，
应直接对 rows_sample 进行计算，而不是重新调用 query_data：
  - 拐点/转折日期："从哪天开始下滑/上涨"
  - 极值定位："哪天最高/最低"
  - 变化幅度："下滑了多少%"、"最大跌幅是多少"
  - 环比计算："上周同比是多少"

计算方法（以拐点检测为例）：
  对 rows_sample 按日期排序后，对目标指标做滑动均值对比，
  连续3日均值下穿前期均值的首日即为拐点日。

## 输出规范
- 回答用中文，先结论后原因
- 每次分析后主动提出 1-2 个追问建议

## 何时使用 reflect 工具
以下情况必须先调用 reflect，再执行查询：
- 用户问题涉及 2 个以上数据源
- 需要对比多个时间段（如同比、环比）
- 问题包含"综合"、"全面分析"、"报告"等词
reflect 的 plan 字段要列出：将查询哪些数据源、用什么时间范围、对比逻辑是什么。

## 何时使用 save_insight
以下情况主动调用 save_insight：
- 发现明确的数据异常（如支付失败率超过5%）
- 用户表达"记住这个"、"下次提醒我"
- 连续3次追问同一主题后确认的结论"""
```

### 1.4 在 `_handle_query_data` 函数之后新增三个处理函数

**定位**：`_handle_query_data` 函数末尾的 `return {**result_for_claude, "_chart_svg": chart_svg}` 行之后，在 `# ─── FastAPI App` 注释之前，插入以下三个函数：

```python
def _handle_reflect(input_: dict) -> dict:
    """直接返回分析计划，不调用任何数据接口。"""
    return {"plan": input_.get("plan", ""), "status": "ok"}


def _handle_compare_periods(input_: dict, session_id: str) -> dict:
    """对同一数据源的两个时间段分别查询，合并计算 delta 和 pct_change。"""
    source_name = input_["source_name"]
    question = input_["question"]
    period_a = input_["period_a"]
    period_b = input_["period_b"]

    if source_name not in DATA_SOURCES:
        return {"error": f"未知数据源: {source_name}"}

    # 将时间段拼入问题，利用 _detect_time_range 解析
    question_a = f"{period_a} {question}"
    question_b = f"{period_b} {question}"

    result_a = _handle_query_data(
        {"source_name": source_name, "question": question_a},
        session_id + "/period_a",
    )
    result_b = _handle_query_data(
        {"source_name": source_name, "question": question_b},
        session_id + "/period_b",
    )

    if result_a.get("error"):
        return {"error": f"period_a 查询失败: {result_a['error']}"}
    if result_b.get("error"):
        return {"error": f"period_b 查询失败: {result_b['error']}"}

    rows_a = result_a.get("rows_sample", [])
    rows_b = result_b.get("rows_sample", [])

    # 尝试按第一个维度字段合并，计算差值
    comparison_rows = []
    if rows_a and rows_b:
        # 取第一条记录的 key 作为 index 字段（排除数值型字段）
        sample_keys = list(rows_a[0].keys()) if rows_a else []
        # 找数值字段：取第一条记录中值为数字的字段
        numeric_keys = [k for k, v in rows_a[0].items() if isinstance(v, (int, float))]
        index_keys = [k for k in sample_keys if k not in numeric_keys]

        # 构建 period_b 的 lookup
        def make_key(row):
            return tuple(row.get(k) for k in index_keys)

        b_lookup = {make_key(r): r for r in rows_b}

        for row_a in rows_a:
            key = make_key(row_a)
            row_b = b_lookup.get(key)
            merged = {**row_a, "_period": period_a}
            if row_b:
                for nk in numeric_keys:
                    val_a = row_a.get(nk, 0) or 0
                    val_b = row_b.get(nk, 0) or 0
                    merged[f"{nk}_prev"] = val_b
                    merged[f"{nk}_delta"] = round(val_a - val_b, 4)
                    merged[f"{nk}_pct"] = (
                        round((val_a - val_b) / val_b * 100, 2) if val_b != 0 else None
                    )
            comparison_rows.append(merged)

    return {
        "period_a": period_a,
        "period_b": period_b,
        "rows_sample": comparison_rows,
        "summary": result_a.get("summary", []),
        "row_count": len(comparison_rows),
        "_chart_svg": result_a.pop("_chart_svg", ""),
    }


def _handle_save_insight(input_: dict) -> dict:
    """将重要发现追加写入 memory/insights.jsonl。"""
    memory_dir = BASE_DIR / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    insight_path = memory_dir / "insights.jsonl"

    record = {
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topic": input_.get("topic", ""),
        "insight": input_.get("insight", ""),
    }
    with insight_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return {"status": "saved", "topic": record["topic"]}


def _load_memory_context() -> str:
    """读取最近10条 insights，格式化为 System Prompt 附录。"""
    path = BASE_DIR / "memory" / "insights.jsonl"
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").strip().splitlines()[-10:]
    items = [json.loads(line) for line in lines if line.strip()]
    if not items:
        return ""
    body = "\n".join(
        f"- [{item['ts'][:10]}] {item['topic']}：{item['insight']}" for item in items
    )
    return f"\n\n## 历史记忆（最近发现）\n{body}"
```

### 1.5 修改 `_chat_stream` 中的 system 参数

**定位**：`_chat_stream` 函数内 `system=SYSTEM_PROMPT,` 这一行，改为：

```python
                system=SYSTEM_PROMPT + _load_memory_context(),
```

### 1.6 在 `_chat_stream` 的 tool dispatch 分支中增加 3 个 elif

**定位**：tool dispatch 区域中：

```python
                elif tool_name == "query_data":
                    result = await asyncio.to_thread(
                        _handle_query_data, tool_input, session_id
                    )
                    chart_svg = result.pop("_chart_svg", "")
                else:
                    result = {"error": f"未知工具: {tool_name}"}
                    chart_svg = ""
```

将 `else:` 之前插入以下 3 个 elif：

```python
                elif tool_name == "reflect":
                    result = _handle_reflect(tool_input)
                    chart_svg = ""
                    # 发送专用 reflect SSE 事件给前端
                    yield _sse({"type": "reflect", "plan": result.get("plan", "")})
                elif tool_name == "compare_periods":
                    result = await asyncio.to_thread(
                        _handle_compare_periods, tool_input, session_id
                    )
                    chart_svg = result.pop("_chart_svg", "")
                elif tool_name == "save_insight":
                    result = _handle_save_insight(tool_input)
                    chart_svg = ""
```

### 1.7 修改 tool_use SSE 事件的 label

**定位**：`_chat_stream` 中的 `tool_use` 事件发送处（`if tool_name == "list_data_sources"`），已有逻辑无需改动，但 `reflect` 工具不发送 tool_result SSE（因为在 1.6 中已提前 yield），需要在 tool_results 追加时跳过 reflect 的 tool_result SSE 重复发送。

> **注意**：reflect 工具在 1.6 中已 `yield _sse({"type": "reflect", ...})`，但仍需正常构造 `tool_results` 列表传给 Claude（Claude 需要看到 tool_result 才能继续）。`tool_result` SSE 发给前端的那一行不需要特殊处理，前端会忽略没有 chart_svg/error 的空 tool_result 事件。无需修改 tool_result 发送逻辑。

---

## 模块二：scripts/sql_generator.py 改动

### 2.1 替换 `_detect_time_range` 函数

将第 91-105 行的整个 `_detect_time_range` 函数替换为以下完整版本：

```python
def _detect_time_range(question: str) -> dict:
    today = date.today()

    # ── 精确天数预设 ──────────────────────────────────────
    if "最近3天" in question or "近3天" in question:
        return {
            "preset": "last_3_days",
            "start": (today - timedelta(days=2)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近7天" in question or "近7天" in question:
        return {
            "preset": "last_7_days",
            "start": (today - timedelta(days=6)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近14天" in question or "近14天" in question:
        return {
            "preset": "last_14_days",
            "start": (today - timedelta(days=13)).isoformat(),
            "end": today.isoformat(),
        }
    if "最近30天" in question or "近30天" in question:
        return {
            "preset": "last_30_days",
            "start": (today - timedelta(days=29)).isoformat(),
            "end": today.isoformat(),
        }

    # ── 本周 / 上周 ───────────────────────────────────────
    if "本周" in question or "这周" in question:
        # 本周一到今天
        monday = today - timedelta(days=today.weekday())
        return {
            "preset": "this_week",
            "start": monday.isoformat(),
            "end": today.isoformat(),
        }
    if "上周" in question:
        # 上周一到上周日
        last_monday = today - timedelta(days=today.weekday() + 7)
        last_sunday = last_monday + timedelta(days=6)
        return {
            "preset": "last_week",
            "start": last_monday.isoformat(),
            "end": last_sunday.isoformat(),
        }

    # ── 本月 / 上月 ───────────────────────────────────────
    if "本月" in question or "这个月" in question:
        month_start = today.replace(day=1)
        return {
            "preset": "this_month",
            "start": month_start.isoformat(),
            "end": today.isoformat(),
        }
    if "上个月" in question or "上月" in question:
        # 上月1日到上月末
        first_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_of_this_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        return {
            "preset": "last_month",
            "start": first_day_of_last_month.isoformat(),
            "end": last_day_of_last_month.isoformat(),
        }

    # ── 今年 ──────────────────────────────────────────────
    if "今年" in question:
        year_start = today.replace(month=1, day=1)
        return {
            "preset": "this_year",
            "start": year_start.isoformat(),
            "end": today.isoformat(),
        }

    return {"preset": "all", "start": None, "end": None}
```

---

## 模块三：static/index.html 改动

### 3.1 在 `<style>` 标签末尾（`</style>` 之前）追加 CSS

找到 `</style>` 标签，在其**之前**插入：

```css
/* ── Reflect 计划块 ──────────────────────────────────────── */
.reflect-plan {
  border: 1px solid var(--border-mid);
  border-left: 2px solid var(--silver-lit);
  border-radius: 4px;
  background: var(--surface-1);
  font-size: 12px;
  margin-top: 8px;
}
.reflect-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 7px 12px;
  font-family: "IBM Plex Mono", monospace;
  letter-spacing: .1em;
  color: var(--silver);
  border-bottom: 1px solid var(--border-dim);
}
.reflect-body {
  padding: 10px 14px;
  color: var(--text-mid);
  white-space: pre-wrap;
  line-height: 1.6;
}
.reflect-toggle {
  margin-left: auto;
  font-size: 10px;
  background: none;
  border: 1px solid var(--border-mid);
  color: var(--text-dim);
  cursor: pointer;
  padding: 2px 8px;
  border-radius: 3px;
}
.reflect-toggle:hover {
  border-color: var(--border-lit);
  color: var(--text-mid);
}
```

### 3.2 在 `handleSseEvent` 的 `tool_use` case 中扩展 label 映射

**定位**：`handleSseEvent` 函数内：

```javascript
    case 'tool_use': {
      const label = event.tool_name === 'list_data_sources'
        ? 'QUERYING DATA SOURCES'
        : 'EXECUTING ANALYSIS';
```

替换为：

```javascript
    case 'tool_use': {
      const labelMap = {
        'list_data_sources': 'QUERYING DATA SOURCES',
        'reflect':           'PLANNING ANALYSIS',
        'compare_periods':   'COMPARING PERIODS',
        'save_insight':      'SAVING INSIGHT',
      };
      const label = labelMap[event.tool_name] || 'EXECUTING ANALYSIS';
```

### 3.3 在 `handleSseEvent` 中新增 `reflect` case

**定位**：`handleSseEvent` 函数的 switch 语句中，在 `case 'tool_use':` 的完整 block **之后**（即 `break;` 和 `}` 之后），在 `case 'tool_result':` **之前**，插入：

```javascript
    case 'reflect': {
      setLoadingEl(null);
      const planEl = document.createElement('div');
      planEl.className = 'reflect-plan';
      planEl.innerHTML = `
        <div class="reflect-header">
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="6" cy="6" r="5" stroke="currentColor" stroke-width="1.2"/>
            <path d="M6 3.5v3l2 1.2" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/>
          </svg>
          <span>分析计划</span>
          <button class="reflect-toggle">收起</button>
        </div>
        <div class="reflect-body">${escapeHtml(event.plan || '')}</div>
      `;
      planEl.querySelector('.reflect-toggle').onclick = function() {
        const body = planEl.querySelector('.reflect-body');
        const collapsed = body.style.display === 'none';
        body.style.display = collapsed ? '' : 'none';
        this.textContent = collapsed ? '收起' : '展开';
      };
      appendExtra(planEl);
      scrollToBottom();
      break;
    }
```

> **注意**：`escapeHtml` 函数已在 index.html 中定义，可以直接使用。

---

## 模块四：创建 memory 目录占位文件

在项目根目录创建 `memory/.gitkeep`（空文件），确保目录被 git 追踪：

```bash
mkdir -p "memory"
touch "memory/.gitkeep"
```

---

## 验证步骤（执行完毕后测试）

1. **reflect 工具**：发送"给我一份最近30天销售和停车场的综合分析报告"
   - 预期：先出现折叠的"分析计划"块，再依次执行查询

2. **compare_periods 工具**：发送"本月和上月华东成交额对比"
   - 预期：Claude 调用 compare_periods，返回带 delta/pct_change 的对比数据

3. **时间范围扩展**：发送"上个月各区域销售排名"
   - 预期：sql_generator 解析出 last_month，不返回 needs_clarification

4. **save_insight + 记忆注入**：
   - 第一轮：发送"帮我分析B停车场的异常，记住这个结论"，Claude 应调用 save_insight
   - 重启服务器，开新 session
   - 第二轮：发送"有什么需要关注的异常"，Claude 回答应引用 memory/insights.jsonl 里的记录

---

## 改动文件汇总

| 文件 | 改动类型 |
|------|----------|
| `server.py` | 新增 import；扩展 TOOLS；新增4个函数；修改 system 参数；修改 tool dispatch |
| `scripts/sql_generator.py` | 替换 `_detect_time_range` 函数 |
| `static/index.html` | 新增 CSS；扩展 tool_use label；新增 reflect case |
| `memory/.gitkeep` | 新建空文件（确保目录存在） |
