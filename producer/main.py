import argparse
import json
from collections.abc import Sequence
from itertools import islice

from producer.generator import GeneratorConfig, Scenario, generate_events


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate StreamForge marketplace events.")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--scenario", choices=list(Scenario), default=Scenario.NORMAL)
    args = parser.parse_args(argv)

    if args.count < 1:
        parser.error("--count must be positive")

    config = GeneratorConfig(seed=args.seed, scenario=Scenario(args.scenario))
    for event in islice(generate_events(config), args.count):
        print(json.dumps(event.to_record()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
