from __future__ import annotations

from pathlib import Path


PALETTE = ["#b54d2e", "#43553f", "#a97a28", "#355c7d"]


def render_svg_chart(rows: list[dict], chart_spec: dict, output_path: str, title: str) -> str:
    chart_type = chart_spec["type"]
    if chart_type != "line":
        raise NotImplementedError("V1 先实现折线图。")

    x_field = chart_spec["x_field"]
    y_field = chart_spec["y_field"]
    series_field = chart_spec.get("series_field")

    width = 920
    height = 480
    margin_left = 70
    margin_right = 30
    margin_top = 60
    margin_bottom = 60

    series_map = {}
    for row in rows:
        series_name = row.get(series_field, "全部") if series_field else "全部"
        series_map.setdefault(series_name, []).append(row)

    x_values = sorted({row[x_field] for row in rows})
    y_values = [float(row[y_field]) for row in rows]
    y_min = 0
    y_max = max(y_values) if y_values else 1
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" preserveAspectRatio="xMidYMid meet">',
        '<rect width="100%" height="100%" fill="#fffaf1" rx="24" />',
        f'<text x="{margin_left}" y="36" font-size="24" fill="#1f1a17" font-family="PingFang SC, Microsoft YaHei, sans-serif">{title}</text>',
    ]

    for index in range(5):
        y = margin_top + plot_height * index / 4
        label_value = round(y_max - (y_max * index / 4), 2)
        parts.append(f'<line x1="{margin_left}" y1="{y}" x2="{width - margin_right}" y2="{y}" stroke="#e8dccd" stroke-width="1"/>')
        parts.append(f'<text x="16" y="{y + 4}" font-size="12" fill="#665c54">{label_value}</text>')

    for x_index, x_value in enumerate(x_values):
        x = margin_left + (plot_width * x_index / max(len(x_values) - 1, 1))
        parts.append(f'<line x1="{x}" y1="{margin_top}" x2="{x}" y2="{margin_top + plot_height}" stroke="#f1e6d8" stroke-width="1"/>')
        parts.append(f'<text x="{x}" y="{height - 22}" text-anchor="middle" font-size="12" fill="#665c54">{x_value}</text>')

    for series_index, (series_name, series_rows) in enumerate(series_map.items()):
        color = PALETTE[series_index % len(PALETTE)]
        points = []
        for row in sorted(series_rows, key=lambda item: item[x_field]):
            x_index = x_values.index(row[x_field])
            x = margin_left + (plot_width * x_index / max(len(x_values) - 1, 1))
            value = float(row[y_field])
            y = margin_top + plot_height - (value / y_max * plot_height if y_max else 0)
            points.append((x, y, value))
        point_string = " ".join(f"{x},{y}" for x, y, _ in points)
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{point_string}" />')
        for x, y, value in points:
            parts.append(f'<circle cx="{x}" cy="{y}" r="4.5" fill="{color}" />')
            parts.append(f'<text x="{x}" y="{y - 10}" text-anchor="middle" font-size="11" fill="{color}">{value:.0f}</text>')
        legend_x = width - margin_right - 120
        legend_y = 28 + series_index * 20
        parts.append(f'<rect x="{legend_x}" y="{legend_y - 10}" width="14" height="14" rx="3" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 20}" y="{legend_y + 2}" font-size="12" fill="#1f1a17">{series_name}</text>')

    parts.append("</svg>")
    Path(output_path).write_text("\n".join(parts), encoding="utf-8")
    return output_path
