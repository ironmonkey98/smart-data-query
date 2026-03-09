# 数据结构说明

## sales.csv

| 字段名 | 类型 | 说明 |
|---|---|---|
| order_date | date | 订单支付日期 |
| region | string | 区域，示例：华东、华南 |
| product | string | 产品名称 |
| paid_amount | number | 支付成交额 |
| order_status | string | 订单状态，`paid` 表示支付成功，`refund` 表示退款 |

## sample_parking_ops.csv

| 字段名 | 类型 | 说明 |
|---|---|---|
| stat_date | date | 统计日期 |
| parking_lot | string | 车场名称 |
| total_revenue | number | 总收入 |
| temp_revenue | number | 临停收入 |
| monthly_revenue | number | 包月收入 |
| entry_count | number | 入场车次 |
| payment_failure_rate | number | 支付失败率，0-1 小数 |
| abnormal_open_count | number | 异常开闸次数 |
| free_release_count | number | 免费放行次数 |
| occupancy_rate | number | 车位利用率，0-1 小数 |
