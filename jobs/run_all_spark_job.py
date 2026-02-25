from __future__ import annotations

import argparse

from vfarm.spark_aggregate import aggregate_data
from vfarm.spark_generate import generate_data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dataproc job: generate and aggregate vfarm data in one batch"
    )
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    gen_result = generate_data(args.config)
    agg_result = aggregate_data(args.config)
    print({"gen": gen_result, "agg": agg_result})


if __name__ == "__main__":
    main()
