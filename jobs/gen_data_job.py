from __future__ import annotations

import argparse

from vfarm.spark_generate import generate_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataproc job: generate vfarm raw data")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    result = generate_data(args.config)
    print(result)


if __name__ == "__main__":
    main()
