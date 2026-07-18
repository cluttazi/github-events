"""Static observability report: text to stdout, HTML to the reports dir.

Renders the most recent run's ``pipeline_run_metrics`` rows — one line per
tracked step across bronze, raw vault, business vault, gold, and DQ.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from observability.metrics.writer import metrics_table_path
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark


def _latest_run_rows(spark: SparkSession, config: LakehouseConfig) -> tuple[str, list[Row]]:
    metrics = spark.read.format("delta").load(str(metrics_table_path(config)))
    latest = metrics.orderBy(F.col("started_at").desc()).select("run_id").first()
    if latest is None:
        raise RuntimeError("metrics table is empty — run some pipelines first")
    run_id = latest["run_id"]
    rows = metrics.filter(F.col("run_id") == run_id).orderBy("started_at").collect()
    return run_id, rows


def render_text(run_id: str, rows: list[Row]) -> str:
    lines = [
        f"pipeline run report  run_id={run_id}",
        "-" * 96,
        f"{'pipeline':16s} {'step':28s} {'status':8s} {'secs':>6s} "
        f"{'read':>7s} {'written':>7s} {'quar':>5s} {'dq':>7s}",
        "-" * 96,
    ]
    for r in rows:
        dq = (
            f"{r['dq_checks_passed']}/{r['dq_checks_passed'] + r['dq_checks_failed']}"
            if r["dq_checks_passed"] is not None
            else "-"
        )
        lines.append(
            f"{r['pipeline']:16s} {r['step']:28s} {r['status']:8s} "
            f"{r['duration_s']:6.1f} "
            f"{r['rows_read'] if r['rows_read'] is not None else '-':>7} "
            f"{r['rows_written'] if r['rows_written'] is not None else '-':>7} "
            f"{r['rows_quarantined'] if r['rows_quarantined'] is not None else '-':>5} "
            f"{dq:>7s}"
        )
    failed = [r for r in rows if r["status"] == "failed"]
    lines.append("-" * 96)
    lines.append(f"steps={len(rows)} failed={len(failed)}")
    return "\n".join(lines)


def render_html(run_id: str, rows: list[Row]) -> str:
    def cell(value: object) -> str:
        return html.escape(str(value)) if value is not None else "—"

    body_rows = "\n".join(
        "<tr class='{cls}'><td>{p}</td><td>{s}</td><td>{st}</td><td>{d:.1f}</td>"
        "<td>{rr}</td><td>{rw}</td><td>{rq}</td><td>{dqp}</td><td>{dqf}</td></tr>".format(
            cls="failed" if r["status"] == "failed" else "ok",
            p=cell(r["pipeline"]),
            s=cell(r["step"]),
            st=cell(r["status"]),
            d=r["duration_s"],
            rr=cell(r["rows_read"]),
            rw=cell(r["rows_written"]),
            rq=cell(r["rows_quarantined"]),
            dqp=cell(r["dq_checks_passed"]),
            dqf=cell(r["dq_checks_failed"]),
        )
        for r in rows
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>github-events run {html.escape(run_id)}</title>
<style>
 body {{ font-family: ui-monospace, monospace; margin: 2rem; background:#fafafa; }}
 h1 {{ font-size: 1.1rem; }}
 table {{ border-collapse: collapse; width: 100%; background: white; }}
 th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; font-size: 0.85rem; }}
 th {{ background: #f0f0f0; }}
 tr.failed td {{ background: #ffe8e8; }}
</style></head>
<body>
<h1>Pipeline run <code>{html.escape(run_id)}</code></h1>
<table>
<tr><th>pipeline</th><th>step</th><th>status</th><th>secs</th><th>rows read</th>
<th>rows written</th><th>quarantined</th><th>dq passed</th><th>dq failed</th></tr>
{body_rows}
</table>
</body></html>
"""


def run_report(config: LakehouseConfig | None = None, spark: SparkSession | None = None) -> Path:
    config = config or load_config()
    spark = spark or get_spark("observability-report", config)
    run_id, rows = _latest_run_rows(spark, config)

    print(render_text(run_id, rows))

    config.storage.reports_dir.mkdir(parents=True, exist_ok=True)
    html_path = config.storage.reports_dir / "observability.html"
    html_path.write_text(render_html(run_id, rows), encoding="utf-8")
    print(f"\nhtml report -> {html_path}")
    return html_path


def main() -> int:
    run_report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
