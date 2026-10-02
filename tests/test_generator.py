from itertools import islice
import json

from producer.generator import GeneratorConfig, Scenario, generate_events
from producer.main import main


def test_seeded_generator_repeats_the_same_event_sequence() -> None:
    config = GeneratorConfig(seed=7, scenario=Scenario.NORMAL)

    first = [event.to_record() for event in islice(generate_events(config), 4)]
    second = [event.to_record() for event in islice(generate_events(config), 4)]

    assert first == second


def test_duplicate_scenario_reuses_an_event_id() -> None:
    config = GeneratorConfig(seed=7, scenario=Scenario.DUPLICATES)

    events = list(islice(generate_events(config), 2))

    assert events[0].event_id == events[1].event_id


def test_late_scenario_generates_an_event_ten_minutes_behind() -> None:
    normal = list(
        islice(generate_events(GeneratorConfig(seed=7, scenario=Scenario.NORMAL)), 2)
    )
    late = list(islice(generate_events(GeneratorConfig(seed=7, scenario=Scenario.LATE)), 2))

    assert (normal[1].event_time - late[1].event_time).total_seconds() == 600


def test_cli_prints_the_requested_number_of_events(capsys) -> None:
    exit_code = main(["--count", "2", "--seed", "7"])

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 0
    assert [record["event_type"] for record in records] == ["order", "payment"]
