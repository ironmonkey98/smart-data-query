from __future__ import annotations

from pathlib import Path


STYLE_PRESETS = {
    "executive_clean": {
        "bg": "#fffaf1",
        "title": "#1f1a17",
        "grid": "#e8dccd",
        "axis": "#665c54",
        "colors": ["#b54d2e", "#43553f", "#a97a28", "#355c7d"],
        "stroke_width": 3,
    },
    "executive_risk": {
        "bg": "#fff4f1",
        "title": "#2f1614",
        "grid": "#f0c9c1",
        "axis": "#7e4d46",
        "colors": ["#c23b22", "#ef7d57", "#7f5539", "#5c677d"],
        "stroke_width": 3.5,
    },
    "ops_dense": {
        "bg": "#f7fafc",
        "title": "#18212b",
        "grid": "#d5dee8",
        "axis": "#516070",
        "colors": ["#355c7d", "#6c5b7b", "#c06c84", "#f67280"],
        "stroke_width": 2.5,
    },
}


def render_svg_chart(rows: list[dict], chart_spec: dict, output_path: str, title: str) -> str:
    chart_type = chart_spec["type"]
    style = _resolve_style(chart_spec)
    if chart_type == "line":
        svg = _render_line_chart(rows, chart_spec, title, style)
    elif chart_type == "bar":
        svg = _render_bar_chart(rows, chart_spec, title, style)
    else:
        raise NotImplementedError(f"暂不支持图表类型: {chart_type}")

    Path(output_path).write_text(svg, encoding="utf-8")
    return output_path


def _resolve_style(chart_spec: dict) -> dict:
    preset_name = chart_spec.get("style_preset", "executive_clean")
    style = STYLE_PRESETS.get(preset_name, STYLE_PRESETS["executive_clean"]).copy()
    style["preset_name"] = preset_name
    return style


def _render_line_chart(rows: list[dict], chart_spec: dict, title: str, style: dict) -> str:
    x_field = chart_spec["x_field"]
    y_field = chart_spec["y_field"]
    series_field = chart_spec.get("series_field")

    width = 920
    height = 480
    margin_left = 70
    margin_right = 30
    margin_top = 60
    margin_bottom = 60
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    series_map = {}
    for row in rows:
        series_name = row.get(series_field, "全部") if series_field else "全部"
        series_map.setdefault(series_name, []).append(row)

    x_values = sorted({row[x_field] for row in rows})
    y_values = [float(row[y_field]) for row in rows]
    y_max = max(y_values) if y_values else 1

    parts = _svg_shell(width, height, title, style, chart_spec.get("chart_family", "trend"))

    for index in range(5):
        y = margin_top + plot_height * index / 4
        label_value = round(y_max - (y_max * index / 4), 2)
        parts.append(f'<line x1="{margin_left}" y1="{y}" x2="{width - margin_right}" y2="{y}" stroke="{style["grid"]}" stroke-width="1"/>')
        parts.append(f'<text x="16" y="{y + 4}" font-size="12" fill="{style["axis"]}">{label_value}</text>')

    for x_index, x_value in enumerate(x_values):
        x = margin_left + (plot_width * x_index / max(len(x_values) - 1, 1))
        parts.append(f'<line x1="{x}" y1="{margin_top}" x2="{x}" y2="{margin_top + plot_height}" stroke="{style["grid"]}" stroke-width="1"/>')
        parts.append(f'<text x="{x}" y="{height - 22}" text-anchor="middle" font-size="12" fill="{style["axis"]}">{x_value}</text>')

    for series_index, (series_name, series_rows) in enumerate(series_map.items()):
        color = style["colors"][series_index % len(style["colors"])]
        points = []
        for row in sorted(series_rows, key=lambda item: item[x_field]):
            x_index = x_values.index(row[x_field])
            x = margin_left + (plot_width * x_index / max(len(x_values) - 1, 1))
            value = float(row[y_field])
            y = margin_top + plot_height - (value / y_max * plot_height if y_max else 0)
            points.append((x, y, value))
        point_string = " ".join(f"{x},{y}" for x, y, _ in points)
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="{style["stroke_width"]}" points="{point_string}" />')
        for x, y, value in points:
            parts.append(f'<circle cx="{x}" cy="{y}" r="4.5" fill="{color}" />')
            parts.append(f'<text x="{x}" y="{y - 10}" text-anchor="middle" font-size="11" fill="{color}">{value:.0f}</text>')
        legend_x = width - margin_right - 120
        legend_y = 28 + series_index * 20
        parts.append(f'<rect x="{legend_x}" y="{legend_y - 10}" width="14" height="14" rx="3" fill="{color}" />')
        parts.append(f'<text x="{legend_x + 20}" y="{legend_y + 2}" font-size="12" fill="{style["title"]}">{series_name}</text>')

    parts.append("</svg>")
    return "\n".join(parts)


def _render_bar_chart(rows: list[dict], chart_spec: dict, title: str, style: dict) -> str:
    x_field = chart_spec["x_field"]
    y_field = chart_spec["y_field"]
    width = 920
    height = 480
    margin_left = 90
    margin_right = 40
    margin_top = 70
    margin_bottom = 60
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    ordered_rows = sorted(rows, key=lambda item: float(item[y_field]), reverse=True)
    y_max = max(float(row[y_field]) for row in ordered_rows) if ordered_rows else 1
    bar_height = min(42, max(24, plot_height / max(len(ordered_rows), 1) - 12))
    gap = max(12, (plot_height - bar_height * len(ordered_rows)) / max(len(ordered_rows), 1))

    parts = _svg_shell(width, height, title, style, chart_spec.get("chart_family", "ranking"))

    for index in range(4):
        x = margin_left + plot_width * index / 3
        label_value = round(y_max * index / 3, 2)
        parts.append(f'<line x1="{x}" y1="{margin_top}" x2="{x}" y2="{height - margin_bottom}" stroke="{style["grid"]}" stroke-width="1"/>')
        parts.append(f'<text x="{x}" y="{height - 18}" text-anchor="middle" font-size="12" fill="{style["axis"]}">{label_value:.0f}</text>')

    emphasis = set(chart_spec.get("emphasis", []))
    for index, row in enumerate(ordered_rows):
        y = margin_top + gap / 2 + index * (bar_height + gap)
        label = row[x_field]
        value = float(row[y_field])
        color = style["colors"][0] if label in emphasis or (not emphasis and index == 0) else style["colors"][min(index + 1, len(style["colors"]) - 1)]
        bar_width = plot_width * (value / y_max if y_max else 0)
        parts.append(f'<text x="{margin_left - 12}" y="{y + bar_height / 2 + 4}" text-anchor="end" font-size="13" fill="{style["title"]}">{label}</text>')
        parts.append(f'<rect x="{margin_left}" y="{y}" width="{bar_width}" height="{bar_height}" rx="10" fill="{color}" opacity="0.92" />')
        parts.append(f'<text x="{margin_left + bar_width + 10}" y="{y + bar_height / 2 + 4}" font-size="12" fill="{style["axis"]}">{value:.0f}</text>')

    parts.append("</svg>")
    return "\n".join(parts)


def _svg_shell(width: int, height: int, title: str, style: dict, chart_family: str) -> list[str]:
    chart_family_class = chart_family.replace("_", "-")
    preset_class = style["preset_name"].replace("_", "-")
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" preserveAspectRatio="xMidYMid meet" class="{chart_family_class} {preset_class}">',
        f'<rect width="100%" height="100%" fill="{style["bg"]}" rx="24" />',
        f'<text x="70" y="36" font-size="24" fill="{style["title"]}" font-family="PingFang SC, Microsoft YaHei, sans-serif">{title}</text>',
    ]
