"""
生成模拟测试数据：
  - sample_sales.csv        : 30天 × 5区域 × 4产品，含趋势差异 + 退款
  - sample_parking_ops.csv  : 30天 × 5车场，含 B 车场收入骤降异常

今天日期：2026-03-08，覆盖 2026-02-07 ～ 2026-03-08（共30天）
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # 保证可复现

DATA_DIR  = Path(__file__).parent
START     = date(2026, 2, 7)
END       = date(2026, 3, 8)
DAYS      = [(START + timedelta(i)) for i in range((END - START).days + 1)]  # 30天

# ─── 销售数据 ────────────────────────────────────────────────────────────────

REGIONS = ["华东", "华南", "华北", "华西", "华中"]
PRODUCTS = ["A产品", "B产品", "C产品", "D产品"]

# 各区域基础日销售额（每产品均值），华南后半段下滑，华北持续增长
def region_base(region: str, product: str, day_idx: int) -> float:
    """day_idx: 0=第一天, 29=最后一天"""
    bases = {
        "华东": {"A产品": 2800, "B产品": 2200, "C产品": 1600, "D产品": 1200},
        "华南": {"A产品": 2200, "B产品": 1800, "C产品": 1300, "D产品":  900},
        "华北": {"A产品": 1500, "B产品": 1200, "C产品":  900, "D产品":  700},
        "华西": {"A产品":  900, "B产品":  750, "C产品":  550, "D产品":  400},
        "华中": {"A产品": 1200, "B产品":  950, "C产品":  700, "D产品":  500},
    }
    base = bases[region][product]
    # 华南：第15天起逐步下滑 25%
    if region == "华南" and day_idx >= 15:
        decay = 1 - 0.25 * (day_idx - 15) / 15
        base *= decay
    # 华北：全程线性增长 35%
    if region == "华北":
        base *= 1 + 0.35 * day_idx / 29
    # 周末 +15%
    weekday = (START + timedelta(day_idx)).weekday()
    if weekday >= 5:
        base *= 1.15
    return base


sales_rows = []
for i, d in enumerate(DAYS):
    for region in REGIONS:
        for product in PRODUCTS:
            base = region_base(region, product, i)
            # 主订单（paid）
            amount = round(base * random.uniform(0.88, 1.12))
            sales_rows.append({
                "order_date": d.isoformat(),
                "region": region,
                "product": product,
                "paid_amount": amount,
                "order_status": "paid",
            })
            # 退款订单（~8% 概率）
            if random.random() < 0.08:
                refund = round(amount * random.uniform(0.05, 0.25))
                sales_rows.append({
                    "order_date": d.isoformat(),
                    "region": region,
                    "product": product,
                    "paid_amount": -refund,
                    "order_status": "refund",
                })

sales_path = DATA_DIR / "sample_sales.csv"
with open(sales_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["order_date","region","product","paid_amount","order_status"])
    w.writeheader()
    w.writerows(sales_rows)

print(f"sales rows   : {len(sales_rows)}  → {sales_path}")

# ─── 停车场数据 ──────────────────────────────────────────────────────────────

LOTS = ["A停车场", "B停车场", "C停车场", "D停车场", "E停车场"]

# B停车场：3月1日起设备故障，收入骤降、支付失败率飙升、异常开闸激增
# D停车场：新开业高端车场，从低基数稳步爬升
# E停车场：社区小车场，规模小、包月为主
B_CRASH_DATE = date(2026, 3, 1)

def parking_row(lot: str, d: date, day_idx: int) -> dict:
    r = random.Random(hash((lot, d.isoformat())))  # 行级固定随机

    if lot == "A停车场":
        rev   = round(r.gauss(10500, 300))
        temp  = round(rev * r.uniform(0.68, 0.72))
        entry = round(r.gauss(510, 15))
        fail  = round(r.uniform(0.015, 0.022), 3)
        abn   = r.randint(1, 3)
        free  = r.randint(8, 12)
        occ   = round(r.uniform(0.80, 0.86), 2)

    elif lot == "B停车场":
        if d < B_CRASH_DATE:
            rev   = round(r.gauss(9200, 250))
            temp  = round(rev * r.uniform(0.65, 0.68))
            entry = round(r.gauss(460, 12))
            fail  = round(r.uniform(0.018, 0.025), 3)
            abn   = r.randint(2, 4)
            free  = r.randint(10, 14)
            occ   = round(r.uniform(0.77, 0.82), 2)
        else:
            # 故障后逐日恶化
            crash_days = (d - B_CRASH_DATE).days  # 0-based
            decay = 1 - min(0.40, 0.07 * crash_days)
            rev   = round(r.gauss(9200 * decay, 200))
            temp  = round(rev * r.uniform(0.60, 0.65))
            entry = round(r.gauss(460 * (1 - 0.05 * crash_days), 15))
            fail  = round(r.uniform(0.04 + 0.012 * crash_days, 0.06 + 0.015 * crash_days), 3)
            abn   = r.randint(8 + crash_days * 2, 14 + crash_days * 3)
            free  = r.randint(20 + crash_days * 4, 30 + crash_days * 5)
            occ   = round(r.uniform(max(0.50, 0.78 - 0.04 * crash_days), 0.74 - 0.03 * crash_days), 2)

    elif lot == "C停车场":
        rev   = round(r.gauss(11500 + day_idx * 15, 280))
        temp  = round(rev * r.uniform(0.66, 0.70))
        entry = round(r.gauss(530 + day_idx // 5, 12))
        fail  = round(r.uniform(0.013, 0.018), 3)
        abn   = r.randint(1, 2)
        free  = r.randint(6, 10)
        occ   = round(r.uniform(0.84, 0.90), 2)

    elif lot == "D停车场":
        # 新开业爬坡：从 7000 爬升至 13000
        base_rev = 7000 + day_idx * 200
        rev   = round(r.gauss(base_rev, 350))
        temp  = round(rev * r.uniform(0.55, 0.62))
        entry = round(r.gauss(300 + day_idx * 8, 20))
        fail  = round(r.uniform(0.010, 0.016), 3)
        abn   = r.randint(1, 2)
        free  = r.randint(5, 8)
        occ   = round(min(0.92, 0.50 + day_idx * 0.015) + r.uniform(-0.02, 0.02), 2)

    else:  # E停车场：社区小车场
        rev   = round(r.gauss(4200, 180))
        temp  = round(rev * r.uniform(0.28, 0.35))
        entry = round(r.gauss(210, 10))
        fail  = round(r.uniform(0.020, 0.030), 3)
        abn   = r.randint(1, 3)
        free  = r.randint(15, 22)
        occ   = round(r.uniform(0.55, 0.68), 2)

    monthly = rev - temp
    return {
        "stat_date":            d.isoformat(),
        "parking_lot":          lot,
        "total_revenue":        max(0, rev),
        "temp_revenue":         max(0, temp),
        "monthly_revenue":      max(0, monthly),
        "entry_count":          max(0, entry),
        "payment_failure_rate": min(0.30, max(0, fail)),
        "abnormal_open_count":  abn,
        "free_release_count":   free,
        "occupancy_rate":       min(1.0, max(0, occ)),
    }


parking_rows = []
for i, d in enumerate(DAYS):
    for lot in LOTS:
        parking_rows.append(parking_row(lot, d, i))

parking_path = DATA_DIR / "sample_parking_ops.csv"
fields = ["stat_date","parking_lot","total_revenue","temp_revenue","monthly_revenue",
          "entry_count","payment_failure_rate","abnormal_open_count","free_release_count","occupancy_rate"]
with open(parking_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(parking_rows)

print(f"parking rows : {len(parking_rows)}  → {parking_path}")
print("\n数据生成完毕，覆盖区间：", START, "→", END)
