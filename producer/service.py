from itertools import islice
from os import getenv
from time import sleep

from kafka import KafkaProducer

from producer.generator import GeneratorConfig, Scenario, generate_events
from producer.kafka_publisher import publish_events


def main() -> None:
    rate = int(getenv("EVENTS_PER_SECOND", "5"))
    scenario = Scenario(getenv("SCENARIO", Scenario.NORMAL))
    producer = KafkaProducer(bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    events = generate_events(GeneratorConfig(seed=int(getenv("SEED", "1")), scenario=scenario))

    while True:
        publish_events(islice(events, rate), producer)
        sleep(1)


if __name__ == "__main__":
    main()
