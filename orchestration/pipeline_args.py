"""
Parse CLI arguments for pipeline execution (start phase).
Keeps run_pipeline.py free of argparse logic.
"""

import argparse

# Phase order: 1 = raw, 2 = normalize, 3 = core
PHASE_RAW = 1
PHASE_NORMALIZE = 2
PHASE_CORE = 3

PHASE_NAMES = {
    PHASE_RAW: "raw",
    PHASE_NORMALIZE: "normalize",
    PHASE_CORE: "core",
}


def parse_pipeline_args(argv=None):
    """
    Parse command-line arguments for which phase to start from.

    Returns:
        namespace with:
          - start_from: int (1=raw, 2=normalize, 3=core)
          - start_from_name: str (e.g. "normalize")
    """
    parser = argparse.ArgumentParser(
        description="DETRAN Vehicle DW Pipeline. Choose from which phase to start.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-from",
        choices=["raw", "normalize", "core"],
        default="raw",
        metavar="PHASE",
        help="Phase to start from: raw (extract+load raw), normalize (read raw, normalize, load norm), core",
    )
    args = parser.parse_args(argv)

    name_to_phase = {"raw": PHASE_RAW, "normalize": PHASE_NORMALIZE, "core": PHASE_CORE}
    start_from = name_to_phase[args.start_from]

    return argparse.Namespace(
        start_from=start_from,
        start_from_name=args.start_from,
    )
