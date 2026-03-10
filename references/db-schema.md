# 数据结构说明


## parking_lots

| 字段名 | 类型 | 说明 |
|---|---|---|
| lot_id | integer | 车场主键 |
| parking_lot_name | string | 车场名称 |
| total_spaces | integer | 总车位 |

## parking_payment_records

| 字段名 | 类型 | 说明 |
|---|---|---|
| payment_id | integer | 支付流水主键 |
| lot_id | integer | 关联 `parking_lots.lot_id` |
| initiated_at | datetime | 发起支付时间 |
| paid_at | datetime | 支付完成时间 |
| license_plate | string | 车牌号 |
| entry_at | datetime | 入场时间 |
| receivable_amount | number | 应收金额 |
| actual_amount | number | 实收金额 |
| payment_result | string | 收费结果 |
| payment_method | string | 支付方式 |
| refund_amount | number | 退款金额 |
| payment_source | string | 缴费来源 |
| invoice_flag | string | 是否开票 |

## parking_passage_records

| 字段名 | 类型 | 说明 |
|---|---|---|
| passage_id | integer | 通行流水主键 |
| lot_id | integer | 关联 `parking_lots.lot_id` |
| license_plate | string | 车牌号 |
| vehicle_type | string | 车辆类型 |
| entry_at | datetime | 入场时间 |
| entry_gate | string | 入场门道 |
| exit_at | datetime | 出场时间 |
| exit_gate | string | 出场门道 |
| stay_minutes | number | 停留时长（分钟） |
| receivable_amount | number | 应收金额 |
| actual_amount | number | 实收金额 |
| notes | string | 备注 |

## 停车经营日级联查结果

停车经营分析不再直接读取单表 CSV，而是通过 SQLite 多表联查派生出如下日级指标结构，再供分析器和报表层复用。

| 字段名 | 类型 | 说明 |
|---|---|---|
| stat_date | date | 统计日期 |
| parking_lot | string | 车场名称 |
| total_revenue | number | 支付流水实收汇总 |
| temp_revenue | number | 当前与 `total_revenue` 同口径，均来自临停收费流水 |
| monthly_revenue | number | 当前原始数据未提供，固定为 0 |
| entry_count | number | 通行记录入场数 |
| payment_failure_rate | number | 支付失败记录占当日支付流水比例 |
| abnormal_open_count | number | 当前原始数据未提供，固定为 0 |
| free_release_count | number | 临时车且应收、实收均为 0 的通行记录数 |
| occupancy_rate | number | `停留分钟数 / (总车位 * 1440)` 的日级估算值 |
