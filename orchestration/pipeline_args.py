"""
Parse CLI arguments for pipeline execution (start phase).
Keeps run_pipeline.py free of argparse logic.
"""

import argparse
import re

# Phase order: 1 = raw, 2 = normalize, 3 = core, 4 = analytics
PHASE_RAW = 1
PHASE_NORMALIZE = 2
PHASE_CORE = 3
PHASE_ANALYTICS = 4

PHASE_NAMES = {
    PHASE_RAW: "raw",
    PHASE_NORMALIZE: "normalize",
    PHASE_CORE: "core",
    PHASE_ANALYTICS: "analytics",
}


def _is_valid_period(s: str) -> bool:
    """Check if string is YYYYMM (6 digits, month 01-12)."""
    if not s or not isinstance(s, str):
        return False
    if not re.match(r"^\d{6}$", s.strip()):
        return False
    month = int(s.strip()[4:6])
    return 1 <= month <= 12


def parse_pipeline_args(argv=None):
    """
    Parse command-line arguments for pipeline execution range.

    Returns:
        namespace with:
          - start_from: int (1=raw, 2=normalize, 3=core)
          - start_from_name: str (e.g. "normalize")
          - stop_at: int (1=raw, 2=normalize, 3=core)
          - stop_at_name: str (e.g. "normalize")
    """
    parser = argparse.ArgumentParser(
        description="DETRAN Vehicle DW Pipeline. Choose from which phase to start and up to which phase to run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-from",
        choices=["raw", "normalize", "core", "analytics"],
        default="raw",
        metavar="PHASE",
        help="Phase to start from: raw, normalize, core, analytics",
    )
    parser.add_argument(
        "--stop-at",
        choices=["raw", "normalize", "core", "analytics"],
        default="analytics",
        metavar="PHASE",
        help="Last phase to run (inclusive). E.g. --stop-at core skips analytics refresh.",
    )
    parser.add_argument(
        "--period",
        type=str,
        default=None,
        metavar="YYYYMM",
        help="Report period (year+month). E.g. 202501. If omitted, uses the most recent period directory under data/input.",
    )
    args = parser.parse_args(argv)

    name_to_phase = {"raw": PHASE_RAW, "normalize": PHASE_NORMALIZE, "core": PHASE_CORE, "analytics": PHASE_ANALYTICS}
    start_from = name_to_phase[args.start_from]
    stop_at = name_to_phase[args.stop_at]

    if start_from > stop_at:
        parser.error(f"--start-from {args.start_from} cannot be after --stop-at {args.stop_at}")

    period = None
    if args.period is not None:
        if not _is_valid_period(args.period):
            parser.error(
                f"--period must be YYYYMM (6 digits, month 01-12). Got: {args.period}"
            )
        period = int(args.period)

    return argparse.Namespace(
        start_from=start_from,
        start_from_name=args.start_from,
        stop_at=stop_at,
        stop_at_name=args.stop_at,
        period=period,
    )
