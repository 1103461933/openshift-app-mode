import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, SinkFunction

LOG = logging.getLogger("equipment-agregate")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def normalize_event(raw: str) -> str:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        event = {"raw": raw}

    if not isinstance(event, dict):
        event = {"value": event}

    equipment_id = (
        event.get("equipment_id")
        or event.get("equipmentId")
        or event.get("equipment")
        or "unknown"
    )
    event["equipment_id"] = str(equipment_id)
    event["_source_raw"] = raw
    return json.dumps(event, separators=(",", ":"), default=str)


class EquipmentCounter(KeyedProcessFunction):
    # Stateful per-equipment counter: used to validate checkpoint restoration.
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("equipment-event-count", Types.LONG())
        self.event_count = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = json.loads(value)
        current = self.event_count.value()
        current = 0 if current is None else current
        current += 1
        self.event_count.update(current)

        original = event.get("_source_raw", value)
        event["processed_count_for_equipment"] = current
        event["processed_at_utc"] = datetime.now(timezone.utc).isoformat()
        event["document_id"] = (
            str(event.get("event_id"))
            if event.get("event_id") is not None
            else hashlib.sha256(original.encode("utf-8")).hexdigest()
        )
        yield json.dumps(event, separators=(",", ":"), default=str)


class CouchbaseUpsertSink(SinkFunction):
    # PoC sink. Deterministic document IDs make replayed upserts idempotent.
    def open(self, runtime_context):
        from datetime import timedelta
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        connection_string = os.environ["COUCHBASE_CONNECTION_STRING"]
        username = os.environ["COUCHBASE_USERNAME"]
        password = os.environ["COUCHBASE_PASSWORD"]
        bucket_name = os.environ["COUCHBASE_BUCKET"]
        scope_name = os.environ.get("COUCHBASE_SCOPE", "_default")
        collection_name = os.environ.get("COUCHBASE_COLLECTION", "_default")

        auth = PasswordAuthenticator(username, password)
        self.cluster = Cluster.connect(connection_string, ClusterOptions(auth))
        self.cluster.wait_until_ready(timedelta(seconds=20))
        bucket = self.cluster.bucket(bucket_name)
        self.collection = bucket.scope(scope_name).collection(collection_name)

    def invoke(self, value, context):
        document = json.loads(value)
        document_id = document.pop("document_id")
        self.collection.upsert(document_id, document)

    def close(self):
        if getattr(self, "cluster", None) is not None:
            self.cluster.close()


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "equipment-events")
    group_id = os.getenv("KAFKA_GROUP_ID", "equipment-agregate-lab")
    sink_mode = os.getenv("SINK_MODE", "console").lower()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "equipment-kafka-source",
    )

    normalized = raw_stream.map(normalize_event, output_type=Types.STRING())

    processed = (
        normalized
        .key_by(lambda value: json.loads(value)["equipment_id"])
        .process(EquipmentCounter(), output_type=Types.STRING())
    )

    if sink_mode == "couchbase":
        processed.add_sink(CouchbaseUpsertSink()).name("couchbase-upsert-sink")
    else:
        processed.print().name("console-sink")

    LOG.info(
        "Starting equipment-agregate: bootstrap=%s topic=%s group=%s sink=%s",
        bootstrap_servers,
        topic,
        group_id,
        sink_mode,
    )
    env.execute("equipment-agregate")


if __name__ == "__main__":
    main()
