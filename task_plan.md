# Task Plan: 评估 Agent 能力现状 vs 目标能力差距

## Goal
逐项评估当前实现是否满足 5 个核心目标，输出结论和差距分析。

## 5 个核心目标（评估标准）
1. Claude SDK 集成（Agent 框架）
2. Agent 能自主分析推理和决策（不只是转发）
3. Agent 自己编写 SQL（不依赖硬编码逻辑）
4. 取消硬编码（通用化）
5. 无法明确时让提问者澄清，绝不猜测

## Phases
- [ ] Phase 1: 读 server.py（Tool Use 主循环、SDK 用法）
- [ ] Phase 2: 读 sql_generator.py（NL→SQL 路径，硬编码程度）
- [ ] Phase 3: 读 connect_db.py（SQL 执行层）
- [ ] Phase 4: 读 parking_analyst.py（分析决策逻辑）
- [ ] Phase 5: 综合评估，输出 gap_analysis.md

## Decisions Made
- 用 planning-with-files 模式追踪评估进度

## Status
**Phase 5 完成** - 已读完所有核心文件，输出 gap_analysis.md
