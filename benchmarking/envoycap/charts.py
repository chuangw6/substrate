#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Renders the Envoy capacity charts from a run directory.

Takes a run directory and nothing else -- no figures are hard-coded here, and
this script never parses a log. Everything it draws comes from summary.json,
which the generator emits.

    benchmarking/envoycap/charts.py benchmarking/envoycap/runs/<timestamp>

Writes, into that directory:

    latency.svg     latency vs offered load, log y, with the 500 ms budget line
                    (latency-pass1.svg, latency-pass2.svg, ... for a repeated run:
                    one chart per pass, on shared axes, rather than an overlay)
    throughput.svg  achieved vs offered load, with the y=x reference
    report.html     one self-contained page: every chart, hover tooltips, table

Python 3 standard library only. The SVG is emitted by hand rather than through
matplotlib so that reading a run needs no virtualenv and no pip install.
"""

import html
import json
import math
import os
import sys

# --- Canvas ------------------------------------------------------------------
# The right margin holds direct labels on the series ends; the legend sits in a
# horizontal strip in the header, so the two never collide.
WIDTH, HEIGHT = 980, 600
MARGIN_LEFT, MARGIN_RIGHT, MARGIN_BOTTOM = 76, 104, 62

# Header baselines, in order down the page.
Y_TITLE, Y_SUBTITLE, Y_LEGEND1, Y_LEGEND2 = 24, 44, 68, 88

# The latency budget the whole measurement is anchored to. Where p95 crosses
# this line is the answer.
BUDGET_MS = 500.0

# Series are drawn in this order, so p95 -- the series the budget applies to --
# lands on top of the others.
#
# Color follows the reference palette in the data-viz guidance. The three
# context percentiles share one blue ordinal ramp, stepped so that "further
# from the chart surface" means "higher percentile": that is the same ordering
# the lines physically take on the chart, which makes the stack readable
# without a legend lookup. p95 is not a member of that context family -- it is
# the quantity the SLO is defined on -- so it takes its own categorical hue.
LATENCY_SERIES = [
    ("p50_ms", "p50", "p50", "#86b6ef", 1.9),
    ("p99_ms", "p99", "p99", "#2a78d6", 1.9),
    ("max_ms", "max", "pmax", "#0d366b", 1.4),
    ("p95_ms", "p95", "p95", "#eb6834", 3.0),
]

# Passes are distinguished by dash pattern as well as being overlaid, so a
# scattered overlay stays visible in a black-and-white printout.
DASHES = ["", "6 3", "2 3", "9 3 2 3", "1 3"]

FONT = "system-ui,-apple-system,'Segoe UI',sans-serif"


def style_block():
    """Palette and text tokens, as CSS custom properties on the SVG root.

    Dark mode is a selected set of steps from the same ramps -- on a dark
    surface the ordinal ramp runs the other way so that "further from the
    surface" keeps meaning "higher percentile" -- not an automatic inversion.
    Every use site carries the light value as a var() fallback so a renderer
    that resolves var() but drops the stylesheet still produces a light chart
    rather than a black one.
    """
    return (
        "<style>"
        f"svg.vc{{font-family:{FONT};"
        "--s:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--mut:#898781;"
        "--grid:#e1e0d9;--axis:#c3c2b7;"
        "--p50:#86b6ef;--p99:#2a78d6;--pmax:#0d366b;--p95:#eb6834;--ach:#2a78d6}"
        "@media (prefers-color-scheme:dark){svg.vc{"
        "--s:#1a1a19;--ink:#ffffff;--ink2:#c3c2b7;--mut:#898781;"
        "--grid:#2c2c2a;--axis:#383835;"
        "--p50:#256abf;--p99:#5598e7;--pmax:#cde2fb;--p95:#d95926;--ach:#3987e5}}"
        "svg.vc text{fill:var(--ink2,#52514e)}"
        "svg.vc .title{fill:var(--ink,#0b0b0b);font-size:17px;font-weight:600}"
        "svg.vc .sub{fill:var(--ink2,#52514e);font-size:12px}"
        "svg.vc .lgd{fill:var(--ink2,#52514e);font-size:11.5px}"
        "svg.vc .tick{fill:var(--mut,#898781);font-size:11px;"
        "font-variant-numeric:tabular-nums}"
        "svg.vc .axlbl{fill:var(--ink2,#52514e);font-size:12px}"
        "svg.vc .note{fill:var(--ink,#0b0b0b);font-size:12.5px;font-weight:600;"
        "font-variant-numeric:tabular-nums}"
        # Labels that float over the plot get a surface-colored outline, so
        # they stay readable wherever a line happens to pass under them.
        "svg.vc .halo{paint-order:stroke;stroke:var(--s,#fcfcfb);stroke-width:4px;"
        "stroke-linejoin:round}"
        "svg.vc .pt{stroke:var(--s,#fcfcfb);stroke-width:2}"
        "svg.vc .pt:hover{r:6}"
        "</style>"
    )


class Geom:
    """The plotting area, given how tall this chart's header turned out."""

    def __init__(self, top):
        self.x0 = MARGIN_LEFT
        self.y0 = top
        self.w = WIDTH - MARGIN_LEFT - MARGIN_RIGHT
        self.h = HEIGHT - top - MARGIN_BOTTOM


def group_by_pass(steps):
    """Groups steps by repetition, preserving first-seen order."""
    order, groups = [], {}
    for s in steps:
        key = s.get("repeat", 1)
        if key not in groups:
            order.append(key)
            groups[key] = []
        groups[key].append(s)
    for key in order:
        groups[key].sort(key=lambda s: s.get("offered_qps", 0.0))
    return [(key, groups[key]) for key in order]


def p95_crossing(steps, budget=BUDGET_MS):
    """Offered rate at which p95 crosses the budget, or None.

    Interpolates in log-latency space between the two bracketing rungs, which
    is the same arithmetic the generator reports, so the annotation on the
    chart and the number on stdout cannot disagree.
    """
    prev = None
    for s in steps:
        cur = s.get("p95_ms") or 0.0
        if cur >= budget:
            if prev is None or (prev.get("p95_ms") or 0.0) <= 0 or cur <= 0:
                return s.get("offered_qps")
            lo, hi = math.log(prev["p95_ms"]), math.log(cur)
            if hi == lo:
                return s.get("offered_qps")
            t = (math.log(budget) - lo) / (hi - lo)
            return prev["offered_qps"] + t * (s["offered_qps"] - prev["offered_qps"])
        prev = s
    return None


def snap_log(v, up):
    """Nearest 1-2-5 boundary at or beyond v.

    Snapping to whole decades instead would be simpler and routinely wastes
    half the canvas: a 3.8 ms floor against a 2.5 s peak is 1..10000 by decade,
    where two thirds of the plot holds no data.
    """
    if v <= 0:
        return 1.0
    decade = 10 ** math.floor(math.log10(v))
    # decade <= v < 10*decade, so each branch always finds a match.
    edges = [decade, 2 * decade, 5 * decade, 10 * decade]
    if up:
        return next(e for e in edges if e >= v)
    return next(e for e in reversed(edges) if e <= v)


def log_ticks(lo, hi):
    """1-2-5 ticks across a decade range, labeled at every power of ten."""
    ticks = []
    decade = math.floor(math.log10(lo))
    while 10**decade <= hi * 1.0000001:
        for mult in (1, 2, 5):
            v = mult * 10**decade
            if lo <= v <= hi:
                ticks.append((v, mult == 1 or (hi / lo) < 100))
        decade += 1
    return ticks


def linear_ticks(lo, hi, target=8):
    """Round ticks over a linear range."""
    if hi <= lo:
        return [lo]
    raw = (hi - lo) / target
    mag = 10 ** math.floor(math.log10(raw))
    stepv = 10 * mag
    for mult in (1, 2, 2.5, 5, 10):
        if raw <= mult * mag:
            stepv = mult * mag
            break
    ticks, v = [], math.ceil(lo / stepv) * stepv
    while v <= hi * 1.0000001:
        ticks.append(v)
        v += stepv
    return ticks


def fmt_qps(v):
    return f"{v:,.0f}" if v >= 10 else f"{v:g}"


def fmt_ms(v):
    if v >= 100:
        return f"{v:,.0f}"
    if v >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}"


def esc(s):
    return html.escape(str(s), quote=True)


def text_width(s, size):
    """Rough advance width, for laying the legend strip out horizontally."""
    return len(s) * size * 0.56


def var(token, fallback):
    return f"var(--{token},{fallback})"


def point_marker(cx, cy, color, tip):
    """A data point: a native SVG tooltip plus the hook report.html hangs its
    own tooltip on. The surface-colored ring keeps overlapping marks legible."""
    return (
        f'<circle class="pt" cx="{cx:.1f}" cy="{cy:.1f}" r="4" fill="{color}" '
        f'data-tip="{esc(tip)}"><title>{esc(tip)}</title></circle>'
    )


def header(title, subtitle, legend_rows):
    """Title, subtitle and up to two horizontal legend strips.

    Returns (svg_fragments, geometry). The legend lives here rather than in the
    right margin so that the margin is free for direct labels on the series
    ends -- identity is then carried twice, and never by color alone.
    """
    out = [
        f'<rect x="0" y="0" width="{WIDTH}" height="{HEIGHT}" fill="{var("s", "#fcfcfb")}"/>',
        f'<text class="title" x="{MARGIN_LEFT}" y="{Y_TITLE}">{esc(title)}</text>',
        f'<text class="sub" x="{MARGIN_LEFT}" y="{Y_SUBTITLE}">{esc(subtitle)}</text>',
    ]
    top = Y_LEGEND1 - 22
    for row_idx, entries in enumerate(legend_rows):
        if not entries:
            continue
        y = (Y_LEGEND1, Y_LEGEND2)[row_idx]
        top = y + 20
        x = MARGIN_LEFT
        for label, color, dash in entries:
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            out.append(
                f'<line x1="{x:.0f}" y1="{y}" x2="{x + 22:.0f}" y2="{y}" stroke="{color}" '
                f'stroke-width="2.4" stroke-linecap="round"{dash_attr}/>'
            )
            out.append(f'<text class="lgd" x="{x + 28:.0f}" y="{y + 4}">{esc(label)}</text>')
            x += 28 + text_width(label, 11.5) + 22
    return out, Geom(top)


def frame(g, x_label, y_label, x_ticks, y_ticks, xf, yf, y_fmt=fmt_ms):
    """Recessive grid, axis ticks and axis titles."""
    out = []
    for v, labeled in y_ticks:
        y = yf(v)
        out.append(
            f'<line x1="{g.x0}" y1="{y:.1f}" x2="{g.x0 + g.w}" y2="{y:.1f}" '
            f'stroke="{var("grid", "#e1e0d9")}" stroke-width="1"/>'
        )
        if labeled:
            out.append(
                f'<text class="tick" x="{g.x0 - 9}" y="{y + 4:.1f}" '
                f'text-anchor="end">{y_fmt(v)}</text>'
            )
    for v in x_ticks:
        x = xf(v)
        out.append(
            f'<line x1="{x:.1f}" y1="{g.y0}" x2="{x:.1f}" y2="{g.y0 + g.h}" '
            f'stroke="{var("grid", "#e1e0d9")}" stroke-width="1"/>'
        )
        out.append(
            f'<text class="tick" x="{x:.1f}" y="{g.y0 + g.h + 18}" '
            f'text-anchor="middle">{fmt_qps(v)}</text>'
        )
    out.append(
        f'<rect x="{g.x0}" y="{g.y0}" width="{g.w}" height="{g.h}" fill="none" '
        f'stroke="{var("axis", "#c3c2b7")}" stroke-width="1"/>'
    )
    out.append(
        f'<text class="axlbl" x="{g.x0 + g.w / 2:.0f}" y="{HEIGHT - 16}" '
        f'text-anchor="middle">{esc(x_label)}</text>'
    )
    ymid = g.y0 + g.h / 2
    out.append(
        f'<text class="axlbl" x="18" y="{ymid:.0f}" text-anchor="middle" '
        f'transform="rotate(-90 18 {ymid:.0f})">{esc(y_label)}</text>'
    )
    return out


def direct_labels(g, entries):
    """Series names at the right end of each line, pushed apart so they never
    overlap. Color is carried by a dot; the text itself stays in text ink."""
    out = []
    items = sorted(entries, key=lambda e: e[0])
    ys = [e[0] for e in items]
    gap = 14
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i - 1] + gap)
    overflow = ys[-1] - (g.y0 + g.h) if ys else 0
    if overflow > 0:
        ys = [y - overflow for y in ys]
    for (_, x, label, color), y in zip(items, ys):
        out.append(f'<circle cx="{x + 11:.1f}" cy="{y:.1f}" r="3.5" fill="{color}"/>')
        out.append(
            f'<text class="lgd halo" x="{x + 19:.1f}" y="{y + 4:.1f}">{esc(label)}</text>'
        )
    return out


def svg_wrap(body):
    return (
        f'<svg class="vc" xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" '
        f'height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" role="img">'
        + style_block()
        + "".join(body)
        + "</svg>"
    )


def latency_scale(steps):
    """Axis ranges for the latency charts, computed over every pass.

    Shared deliberately. One chart per pass only beats an overlay if the two
    can be compared by eye, and two charts on auto-fitted axes cannot be: the
    same curve shape would appear at two different heights.
    """
    values = [
        s[key]
        for s in steps
        for key, _, _, _, _ in LATENCY_SERIES
        if isinstance(s.get(key), (int, float)) and s[key] > 0
    ]
    # The budget line has to be on the canvas even when nothing came near it: a
    # chart that crops the answer out is worse than no chart.
    lo = min(values + [BUDGET_MS]) if values else 1.0
    hi = max(values + [BUDGET_MS]) if values else 1000.0
    return (
        max(s["offered_qps"] for s in steps) * 1.04,
        snap_log(lo * 0.85, up=False),
        snap_log(hi * 1.15, up=True),
    )


def latency_svg(report, pass_no, steps, scale, npasses=1):
    """One pass, four quantiles.

    One chart per pass rather than every pass overlaid: four series times N
    passes is a thicket by the third pass, and dash patterns stop being
    separable well before the lines do.
    """
    x_max, y_min, y_max = scale

    flags = report.get("run", {}).get("flags", {})
    title = "Latency vs offered load"
    if npasses > 1:
        title += f" — pass {pass_no}"
    subtitle = (
        f"{flags.get('actors', '?')} actors, {flags.get('steps', '?')} rungs of "
        f"{flags.get('step_duration', '?')}; latency measured from each request's "
        "scheduled send time"
    )
    # The budget line is labeled inline on the line itself, so it does not
    # also take a legend slot.
    series_legend = [
        (name, var(token, color), "") for _, name, token, color, _ in LATENCY_SERIES
    ]
    out, g = header(title, subtitle, [series_legend])

    def xf(v):
        return g.x0 + (v / x_max) * g.w

    def yf(v):
        v = max(v, y_min)
        span = math.log10(y_max) - math.log10(y_min)
        return g.y0 + g.h * (1 - (math.log10(v) - math.log10(y_min)) / span)

    out += frame(
        g,
        "offered request rate (QPS)",
        "latency (ms, log scale)",
        linear_ticks(0, x_max),
        log_ticks(y_min, y_max),
        xf,
        yf,
    )

    # The budget, drawn under the data.
    by = yf(BUDGET_MS)
    out.append(
        f'<line x1="{g.x0}" y1="{by:.1f}" x2="{g.x0 + g.w}" y2="{by:.1f}" '
        f'stroke="{var("ink", "#0b0b0b")}" stroke-width="1.4" stroke-dasharray="7 4" '
        f'opacity="0.65"/>'
    )
    out.append(
        f'<text class="lgd halo" x="{g.x0 + 6}" y="{by - 7:.1f}">'
        f"{BUDGET_MS:.0f} ms budget</text>"
    )

    label_ends = []
    for key, name, token, color, width in LATENCY_SERIES:
        stroke = var(token, color)
        pts = [
            (s["offered_qps"], s[key])
            for s in steps
            if isinstance(s.get(key), (int, float))
        ]
        if not pts:
            continue
        path = " ".join(f"{xf(q):.1f},{yf(v):.1f}" for q, v in pts)
        out.append(
            f'<polyline points="{path}" fill="none" stroke="{stroke}" '
            f'stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round"/>'
        )
        for q, v in pts:
            s = next(x for x in steps if x["offered_qps"] == q)
            tip = (
                f"pass {pass_no} rung {s.get('step')} | offered {fmt_qps(q)} QPS "
                f"| achieved {fmt_qps(s.get('achieved_qps', 0))} | {name} "
                f"{fmt_ms(v)} ms | n={s.get('count', 0)} fail={s.get('fail', 0)}"
            )
            out.append(point_marker(xf(q), yf(v), stroke, tip))
        label_ends.append((yf(pts[-1][1]), xf(pts[-1][0]), name, stroke))
    out += direct_labels(g, label_ends)

    # The crossing is the deliverable, so it is annotated where it happens
    # rather than left to be read off the axes by eye.
    qps = p95_crossing(steps)
    if qps is not None:
        p95_ink = var("p95", "#eb6834")
        cx = xf(qps)
        out.append(
            f'<line x1="{cx:.1f}" y1="{by:.1f}" x2="{cx:.1f}" y2="{g.y0 + g.h}" '
            f'stroke="{p95_ink}" stroke-width="1.2" stroke-dasharray="3 3" opacity="0.7"/>'
        )
        out.append(
            f'<circle cx="{cx:.1f}" cy="{by:.1f}" r="5" fill="{var("s", "#fcfcfb")}" '
            f'stroke="{p95_ink}" stroke-width="2.5"/>'
        )
        # Labeled at the foot of the crossing line, not beside the marker. The
        # marker sits on the budget line, which is exactly where the curves are
        # steepest and most crowded; the band above the x-axis to its right is
        # empty on every run whose latency rises, which is every run of
        # interest.
        if cx > g.x0 + g.w * 0.7:
            anchor, tx = "end", cx - 8
        else:
            anchor, tx = "start", cx + 8
        out.append(
            f'<text class="note halo" x="{tx:.1f}" y="{g.y0 + g.h - 12:.1f}" '
            f'text-anchor="{anchor}">p95 crosses at {fmt_qps(qps)} QPS</text>'
        )
    else:
        top_qps = max(s["offered_qps"] for s in steps)
        note = (
            f"p95 stays under {BUDGET_MS:.0f} ms through {fmt_qps(top_qps)} QPS "
            "— capacity not reached"
        )
        out.append(
            f'<text class="note halo" x="{g.x0 + g.w - 12:.0f}" y="{g.y0 + 22}" '
            f'text-anchor="end">{esc(note)}</text>'
        )
    return svg_wrap(out)


def throughput_svg(report):
    steps = report["steps"]
    by_pass = group_by_pass(steps)

    x_max = max(s["offered_qps"] for s in steps) * 1.04
    y_max = max([s.get("achieved_qps", 0) for s in steps] + [x_max])

    # One measured series, so no series legend -- the title names it and the
    # line is directly labeled. Passes still need identifying when there is
    # more than one.
    pass_legend = (
        [
            (f"pass {p}", var("mut", "#898781"), DASHES[i % len(DASHES)])
            for i, (p, _) in enumerate(by_pass)
        ]
        if len(by_pass) > 1
        else []
    )
    out, g = header(
        "Achieved vs offered load",
        "throughput tracking y=x while latency climbs is queueing; "
        "falling off y=x is saturation",
        [pass_legend],
    )

    def xf(v):
        return g.x0 + (v / x_max) * g.w

    def yf(v):
        return g.y0 + g.h * (1 - v / y_max)

    out += frame(
        g,
        "offered request rate (QPS)",
        "achieved request rate (QPS)",
        linear_ticks(0, x_max),
        [(v, True) for v in linear_ticks(0, y_max)],
        xf,
        yf,
        y_fmt=fmt_qps,
    )

    end = min(x_max, y_max)
    out.append(
        f'<line x1="{xf(0):.1f}" y1="{yf(0):.1f}" x2="{xf(end):.1f}" y2="{yf(end):.1f}" '
        f'stroke="{var("ink", "#0b0b0b")}" stroke-width="1.3" stroke-dasharray="7 4" '
        f'opacity="0.6"/>'
    )
    # Labeled partway up the diagonal rather than at its end. The measured line
    # meets y=x at the top right on any run that does not saturate, so an
    # end-of-line label lands on top of the "achieved" one. Above the diagonal
    # is empty by construction -- achieved never exceeds offered.
    mid = end * 0.4
    out.append(
        f'<text class="lgd halo" x="{xf(mid) + 10:.1f}" y="{yf(mid) - 16:.1f}">y = x</text>'
    )

    ach = var("ach", "#2a78d6")
    label_ends = []
    for pass_idx, (pass_no, pass_steps) in enumerate(by_pass):
        dash = DASHES[pass_idx % len(DASHES)]
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        pts = [(s["offered_qps"], s.get("achieved_qps", 0.0)) for s in pass_steps]
        path = " ".join(f"{xf(q):.1f},{yf(a):.1f}" for q, a in pts)
        out.append(
            f'<polyline points="{path}" fill="none" stroke="{ach}" stroke-width="2.4"'
            f'{dash_attr} stroke-linejoin="round" stroke-linecap="round"/>'
        )
        for s in pass_steps:
            achieved = s.get("achieved_qps", 0.0)
            shortfall = (
                (1 - achieved / s["offered_qps"]) * 100 if s["offered_qps"] else 0.0
            )
            tip = (
                f"pass {pass_no} step {s.get('step')} | offered "
                f"{fmt_qps(s['offered_qps'])} | achieved {fmt_qps(achieved)} "
                f"({-shortfall:+.1f}% vs y=x) | lag p99 "
                f"{s.get('dispatch_lag_p99_ms', 0):.1f} ms"
            )
            out.append(point_marker(xf(s["offered_qps"]), yf(achieved), ach, tip))
        if pass_idx == len(by_pass) - 1 and pts:
            label_ends.append((yf(pts[-1][1]), xf(pts[-1][0]), "achieved", ach))
    out += direct_labels(g, label_ends)
    return svg_wrap(out)


def step_table_html(steps):
    cols = [
        ("repeat", "pass"),
        ("step", "step"),
        ("offered_qps", "offered"),
        ("achieved_qps", "achieved"),
        ("count", "n"),
        ("fail", "fail"),
        ("p50_ms", "p50 ms"),
        ("p95_ms", "p95 ms"),
        ("p99_ms", "p99 ms"),
        ("max_ms", "max ms"),
        ("dispatch_lag_p99_ms", "lag p99"),
        ("loadgen_cores_used", "lg cores"),
        ("per_worker_ip_rps", "per-IP rps"),
        ("route_duration_p95_ms", "route p95"),
    ]
    head = "".join(f"<th>{esc(label)}</th>" for _, label in cols)
    head += "<th>envoy p95</th><th>rig</th>"
    rows = []
    for s in steps:
        cells = []
        for key, _ in cols:
            v = s.get(key)
            if v is None:
                cells.append("<td>&mdash;</td>")
            elif isinstance(v, float):
                cells.append(f"<td>{v:.1f}</td>")
            else:
                cells.append(f"<td>{esc(v)}</td>")
        envoy_p95 = (s.get("envoy") or {}).get("downstream_rq_time_p95_ms")
        cells.append(
            "<td>&mdash;</td>" if envoy_p95 is None else f"<td>{envoy_p95:.1f}</td>"
        )
        limited = s.get("rig_limited")
        cells.append('<td class="rig">LIMITED</td>' if limited else "<td></td>")
        rows.append(
            f'<tr class="{"limited" if limited else ""}">' + "".join(cells) + "</tr>"
        )
    return (
        f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )


def html_report(report, latencies, throughput):
    run = report.get("run", {})
    crossings = [(p, p95_crossing(s)) for p, s in group_by_pass(report["steps"])]
    headline = ", ".join(
        f"pass {p}: {fmt_qps(q)} QPS" if q else f"pass {p}: not reached"
        for p, q in crossings
    )
    caveats = "".join(f"<li>{esc(c)}</li>" for c in run.get("caveats", []))
    meta = "".join(
        f"<li><b>{esc(k)}</b>: {esc(v)}</li>"
        for k, v in [
            ("cluster", run.get("cluster")),
            ("git sha", run.get("git_sha")),
            ("image", run.get("image")),
            ("router node", run.get("router_node")),
            ("loadgen node", run.get("loadgen_node")),
            ("started", run.get("started_at")),
            ("finished", run.get("finished_at")),
            ("actors", run.get("flags", {}).get("actors")),
            ("distinct worker pod IPs", run.get("distinct_worker_ips")),
        ]
        if v not in (None, "")
    )
    # One section per pass, headed only when there is more than one -- a "pass 1"
    # heading on a single-pass run is noise.
    latency_sections = "\n".join(
        (f"<h2>Latency &mdash; pass {p}</h2>\n" if len(latencies) > 1 else "") + svg
        for p, svg in latencies
    )
    abort = ""
    if run.get("aborted"):
        abort = (
            '<p class="abort"><b>RIG LIMITED.</b> The load generator, not the system '
            f"under test, is what ran out: {esc(run.get('abort_reason', ''))}</p>"
        )
    # The Apache header goes into the generated page, not just this script. A
    # report checked in alongside RESULTS.md is a source file as far as
    # hack/verify/boilerplate.sh is concerned, and .html is not on its skip
    # list -- so emitting it here is what keeps a published run verifiable.
    return f"""<!doctype html>
<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Envoy capacity &mdash; {esc(run.get('cluster', 'run'))}</title>
<style>
 :root {{ color-scheme: light dark;
   --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink2: #52514e;
   --mut: #898781; --line: #e1e0d9; --crit: #d03b3b; --critbg: #fdf3f3; }}
 @media (prefers-color-scheme: dark) {{
   :root {{ --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink2: #c3c2b7;
     --mut: #898781; --line: #2c2c2a; --crit: #d03b3b; --critbg: #241a1a; }}
 }}
 body {{ font: 14px/1.5 {FONT}; color: var(--ink2); background: var(--page);
   margin: 0; padding: 32px 24px; }}
 main {{ max-width: 1060px; margin: 0 auto; }}
 h1 {{ font-size: 22px; margin-bottom: 4px; color: var(--ink); }}
 h2 {{ font-size: 16px; margin-top: 34px; color: var(--ink); }}
 .headline {{ font-size: 17px; font-weight: 600; color: var(--ink);
   font-variant-numeric: tabular-nums; }}
 .abort {{ background: var(--critbg); border-left: 4px solid var(--crit);
   padding: 10px 14px; color: var(--ink); }}
 svg {{ border: 1px solid var(--line); border-radius: 6px; background: var(--surface);
   max-width: 100%; height: auto; }}
 table {{ border-collapse: collapse; font-size: 12px; margin-top: 8px;
   font-variant-numeric: tabular-nums; background: var(--surface); }}
 th, td {{ border: 1px solid var(--line); padding: 3px 8px; text-align: right; }}
 th {{ color: var(--ink); font-weight: 600; }}
 tr.limited {{ background: var(--critbg); }}
 td.rig {{ color: var(--crit); font-weight: 600; }}
 ul {{ margin-top: 6px; }}
 b {{ color: var(--ink); }}
 #tip {{ position: fixed; display: none; background: var(--ink); color: var(--page);
   padding: 5px 9px; border-radius: 4px; font-size: 11.5px; pointer-events: none;
   z-index: 10; max-width: 460px; }}
</style></head><body><main>
<h1>Envoy capacity &mdash; latency vs offered load</h1>
<p class="headline">p95 crosses {BUDGET_MS:.0f} ms at: {esc(headline)}</p>
{abort}
{latency_sections}
<h2>Achieved vs offered load</h2>
{throughput}
<h2>Step table</h2>
{step_table_html(report['steps'])}
<h2>Run</h2>
<ul>{meta}</ul>
<h2>Caveats &mdash; all in force for the whole measurement</h2>
<ul>{caveats}</ul>
</main><div id="tip"></div>
<script>
const tip = document.getElementById('tip');
document.querySelectorAll('circle.pt').forEach(function (pt) {{
  pt.addEventListener('mousemove', function (e) {{
    tip.textContent = pt.getAttribute('data-tip');
    tip.style.display = 'block';
    tip.style.left = Math.min(e.clientX + 14, window.innerWidth - 480) + 'px';
    tip.style.top = (e.clientY + 14) + 'px';
  }});
  pt.addEventListener('mouseleave', function () {{ tip.style.display = 'none'; }});
}});
</script>
</body></html>
"""


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    run_dir = argv[1]
    summary = os.path.join(run_dir, "summary.json")
    if not os.path.isfile(summary):
        print(f"Error: {summary} not found", file=sys.stderr)
        return 1
    with open(summary, encoding="utf-8") as fh:
        report = json.load(fh)
    if not report.get("steps"):
        print(f"Error: {summary} has no steps to chart", file=sys.stderr)
        return 1

    by_pass = group_by_pass(report["steps"])
    scale = latency_scale(report["steps"])
    latencies = [
        (p, latency_svg(report, p, steps, scale, len(by_pass)))
        for p, steps in by_pass
    ]
    outputs = [
        (
            "latency.svg" if len(latencies) == 1 else f"latency-pass{p}.svg",
            svg,
        )
        for p, svg in latencies
    ]
    throughput = throughput_svg(report)
    outputs += [
        ("throughput.svg", throughput),
        ("report.html", html_report(report, latencies, throughput)),
    ]
    for name, content in outputs:
        path = os.path.join(run_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
