from __future__ import annotations

import argparse

from vfarm.spark_aggregate import aggregate_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataproc job: aggregate vfarm weekly inputs")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    result = aggregate_data(args.config)
    print(result)


if __name__ == "__main__":
    main()
