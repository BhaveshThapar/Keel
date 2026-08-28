#!/usr/bin/env python3
"""Render a deterministic SVG throughput timeline with nemesis annotations."""

import csv
import html
import sys
from pathlib import Path

raw, output, provenance = map(Path, sys.argv[1:4])
rows = list(csv.DictReader(raw.open(encoding="utf-8")))
samples = [(float(r["elapsed_seconds"]), int(r["acknowledged"])) for r in rows if r["event"] == "sample"]
events = [(float(r["elapsed_seconds"]), r["event"]) for r in rows if r["event"] != "sample"]
if len(samples) < 2:
    raise SystemExit("timeline has fewer than two samples")

rates = []
for (t0, a0), (t1, a1) in zip(samples, samples[1:]):
    if t1 > t0:
        rates.append((t1, (a1 - a0) / (t1 - t0)))

width, height = 960, 480
left, top, right, bottom = 70, 35, 25, 65
plot_w, plot_h = width - left - right, height - top - bottom
max_t = max(t for t, _ in samples)
max_rate = max((rate for _, rate in rates), default=1.0) or 1.0
x = lambda t: left + t / max_t * plot_w
y = lambda rate: top + plot_h - rate / max_rate * plot_h
points = " ".join(f"{x(t):.2f},{y(rate):.2f}" for t, rate in rates)

head = provenance.read_text(encoding="utf-8").splitlines()[:3]
out = [*head, "", f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" font-family="monospace" font-size="11">']
out += [
    f'<rect width="{width}" height="{height}" fill="#fff"/>',
    f'<text x="{left}" y="18" font-size="14">Acknowledged throughput during real-cluster nemeses</text>',
    f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#333"/>',
    f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#333"/>',
    f'<polyline fill="none" stroke="#1769aa" stroke-width="2" points="{points}"/>',
    f'<text x="{left + plot_w / 2:.0f}" y="{height - 15}" text-anchor="middle">elapsed seconds</text>',
    f'<text x="16" y="{top + plot_h / 2:.0f}" transform="rotate(-90 16 {top + plot_h / 2:.0f})" text-anchor="middle">acknowledged writes/s</text>',
    f'<text x="{left}" y="{top + plot_h + 18}">0</text>',
    f'<text x="{left + plot_w}" y="{top + plot_h + 18}" text-anchor="end">{max_t:.1f}</text>',
    f'<text x="{left - 8}" y="{top + 4}" text-anchor="end">{max_rate:.1f}</text>',
]
for i, (when, label) in enumerate(events):
    px = x(when)
    escaped = html.escape(label.removeprefix("nemesis: "))
    label_y = top + 12 + (i % 5) * 13
    out.append(f'<line x1="{px:.2f}" y1="{top}" x2="{px:.2f}" y2="{top + plot_h}" stroke="#c62828" stroke-dasharray="3 3" opacity="0.55"/>')
    out.append(f'<text x="{px + 3:.2f}" y="{label_y}" fill="#8e0000" font-size="9">{escaped}</text>')
out.append("</svg>")
output.write_text("\n".join(out) + "\n", encoding="utf-8")
