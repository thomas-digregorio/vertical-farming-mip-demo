from __future__ import annotations

import argparse
from pathlib import Path

from .optimize import solve_optimization
from .report import generate_report
from .spark_aggregate import aggregate_data
from .spark_generate import generate_data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Vertical farming synthetic data + MIP optimization workflow"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_config_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", type=Path, default=Path("configs/base.yaml"))

    p_gen = subparsers.add_parser("gen-data", help="Generate synthetic daily parquet data")
    add_config_arg(p_gen)

    p_agg = subparsers.add_parser("agg-data", help="Aggregate daily parquet data into weekly inputs")
    add_config_arg(p_agg)

    p_solve = subparsers.add_parser("solve", help="Build and solve Pyomo MIP")
    add_config_arg(p_solve)
    p_solve.add_argument("--tee", action="store_true", help="Stream solver logs")
    p_solve.add_argument(
        "--feasibility-only",
        action="store_true",
        help="Run feasibility diagnostics only and skip optimization",
    )

    p_report = subparsers.add_parser("report", help="Generate report artifacts and plots")
    add_config_arg(p_report)
    p_report.add_argument("--no-plots", action="store_true", help="Skip plot generation")

    p_run = subparsers.add_parser("run-all", help="Run gen-data, agg-data, solve, report")
    add_config_arg(p_run)
    p_run.add_argument("--tee", action="store_true", help="Stream solver logs")
    p_run.add_argument("--no-plots", action="store_true", help="Skip plot generation")

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    if args.command == "gen-data":
        result = generate_data(args.config)
        print("Generated raw datasets:")
        for key, value in result.items():
            print(f"- {key}: {value}")
        return

    if args.command == "agg-data":
        result = aggregate_data(args.config)
        print("Aggregated weekly datasets:")
        for key, value in result.items():
            print(f"- {key}: {value}")
        return

    if args.command == "solve":
        result = solve_optimization(
            args.config,
            tee=bool(args.tee),
            feasibility_only=bool(args.feasibility_only),
        )
        print(result)
        return

    if args.command == "report":
        summary = generate_report(args.config, make_plots=not args.no_plots)
        print("Report summary:")
        for key, value in summary.items():
            print(f"- {key}: {value}")
        return

    if args.command == "run-all":
        generate_data(args.config)
        aggregate_data(args.config)
        solve_optimization(args.config, tee=bool(args.tee), feasibility_only=False)
        summary = generate_report(args.config, make_plots=not args.no_plots)
        print("Run complete. Summary:")
        for key, value in summary.items():
            print(f"- {key}: {value}")
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
