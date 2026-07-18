"""CLI entry point: ``python -m ingestion.github_archive``."""

from __future__ import annotations

import argparse
import sys

from ingestion.github_archive.generator import GeneratorConfig, run_generator
from pipelines.common.config import load_config


def main(argv: list[str] | None = None) -> int:
    config = load_config()
    parser = argparse.ArgumentParser(
        prog="github_archive",
        description="Emit seeded GH-Archive-style NDJSON event files into the landing zone.",
    )
    parser.add_argument("--events", type=int, default=2000, help="number of events to emit")
    parser.add_argument("--seed", type=int, default=42, help="deterministic seed")
    parser.add_argument(
        "--corrupt-pct",
        type=float,
        default=0.0,
        help="percent of lines to corrupt (exercises bronze quarantine)",
    )
    args = parser.parse_args(argv)

    summary = run_generator(
        GeneratorConfig(
            events=args.events,
            seed=args.seed,
            corrupt_pct=args.corrupt_pct,
            landing_dir=config.source.landing_dir,
        )
    )

    print(
        f"github_archive: emitted {summary.events_emitted} events "
        f"across {summary.files_written} hourly files"
    )
    for event_type, count in sorted(summary.by_type.items()):
        print(f"  {event_type:18s} {count}")
    if summary.corrupt_events:
        print(f"  corrupted (quarantine fodder): {summary.corrupt_events}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
