import hashlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    FlatMapFunction,
)
from pyflink.datastream.state import ValueStateDescriptor


LOG = logging.getLogger("equipment-agregate")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def normalize_event(raw: str) -> str:
    """
    Normalize incoming Kafka events into JSON strings.
    """

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

    return json.dumps(
        event,
        separators=(",", ":"),
        default=str,
    )


class EquipmentCounter(KeyedProcessFunction):
    """
    Stateful per-equipment counter.

    The ValueState is managed by Flink and included in checkpoints.
    """

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "equipment-event-count",
            Types.LONG(),
        )

        self.event_count = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = json.loads(value)

        current = self.event_count.value()
        current = 0 if current is None else current
        current += 1

        self.event_count.update(current)

        original = event.get("_source_raw", value)

        event["processed_count_for_equipment"] = current
        event["processed_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()

        event["document_id"] = (
            str(event.get("event_id"))
            if event.get("event_id") is not None
            else hashlib.sha256(
                original.encode("utf-8")
            ).hexdigest()
        )

        return json.dumps(
            event,
            separators=(",", ":"),
            default=str,
        )


class CouchbaseUpsertFlatMap(FlatMapFunction):
    """
    Couchbase sink using the Python SDK.

    Each TaskManager sink instance establishes its own connection.
    Documents are written using deterministic document IDs and upsert.

    This supports idempotent document replacement for repeated IDs.
    It does not implement checkpoint-coordinated exactly-once commits.
    """

    def open(self, runtime_context):
        required_env = [
            "COUCHBASE_CONNECTION_STRING",
            "COUCHBASE_USERNAME",
            "COUCHBASE_PASSWORD",
            "COUCHBASE_BUCKET",
        ]

        missing = [
            name
            for name in required_env
            if not os.getenv(name)
        ]

        if missing:
            raise RuntimeError(
                "Missing required Couchbase environment variables: "
                + ", ".join(missing)
            )

        connection_string = os.environ[
            "COUCHBASE_CONNECTION_STRING"
        ]

        username = os.environ["COUCHBASE_USERNAME"]
        password = os.environ["COUCHBASE_PASSWORD"]
        bucket_name = os.environ["COUCHBASE_BUCKET"]

        scope_name = os.getenv(
            "COUCHBASE_SCOPE",
            "_default",
        )

        collection_name = os.getenv(
            "COUCHBASE_COLLECTION",
            "_default",
        )

        connection_timeout = int(
            os.getenv("COUCHBASE_CONNECTION_TIMEOUT", "20")
        )

        self.cluster = None
        self.collection = None

        try:
            auth = PasswordAuthenticator(
                username,
                password,
            )

            self.cluster = Cluster.connect(
                connection_string,
                ClusterOptions(auth),
            )

            self.cluster.wait_until_ready(
                timedelta(seconds=connection_timeout)
            )

            bucket = self.cluster.bucket(bucket_name)

            self.collection = (
                bucket
                .scope(scope_name)
                .collection(collection_name)
            )

            LOG.info(
                "Connected to Couchbase successfully: "
                "bucket=%s scope=%s collection=%s",
                bucket_name,
                scope_name,
                collection_name,
            )

        except Exception:
            LOG.exception(
                "Failed to initialize Couchbase connection"
            )

            if self.cluster is not None:
                self.cluster.close()
                self.cluster = None

            raise

    def flat_map(self, value):
        # This operator performs the Couchbase write as a side effect.
        # It intentionally emits no downstream records.
        if self.collection is None:
            raise RuntimeError(
                "Couchbase collection is not initialized"
            )

        try:
            if isinstance(value, str):
                document = json.loads(value)
            elif isinstance(value, dict):
                document = value
            else:
                raise ValueError(
                    f"Unsupported document type: {type(value)}"
                )

            if not isinstance(document, dict):
                raise ValueError(
                    "Couchbase sink expects a JSON object"
                )

            document = dict(document)

            document_id = document.pop("document_id", None)

            if document_id is None:
                raise ValueError(
                    "Document does not contain document_id"
                )

            document_id = str(document_id).strip()

            if not document_id:
                raise ValueError(
                    "Document document_id cannot be empty"
                )

            self.collection.upsert(
                document_id,
                document,
            )

            LOG.debug(
                "Document upserted successfully: document_id=%s",
                document_id,
            )

            # FlatMapFunction must return an iterable.
            # The record has been written to Couchbase, so emit nothing.
            return iter(())

        except Exception:
            LOG.exception(
                "Couchbase document write failed"
            )
            # Propagate the exception to Flink; do not discard failed records.
            raise

    def close(self):
        if getattr(self, "cluster", None) is not None:
            try:
                self.cluster.close()

                LOG.info(
                    "Couchbase connection closed successfully"
                )

            finally:
                self.cluster = None
                self.collection = None


def validate_sink_configuration(sink_mode: str):
    """
    Validate sink mode and Couchbase configuration before job submission.
    """

    supported_modes = {"console", "couchbase"}

    if sink_mode not in supported_modes:
        raise ValueError(
            f"Unsupported SINK_MODE='{sink_mode}'. "
            f"Supported modes: {', '.join(sorted(supported_modes))}"
        )

    if sink_mode == "couchbase":
        required_env = [
            "COUCHBASE_CONNECTION_STRING",
            "COUCHBASE_BUCKET",
            "COUCHBASE_USERNAME",
            "COUCHBASE_PASSWORD",
        ]

        missing = [
            name
            for name in required_env
            if not os.getenv(name)
        ]

        if missing:
            raise RuntimeError(
                "Couchbase sink configuration is incomplete. "
                "Missing variables: " + ", ".join(missing)
            )


def main():
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic = os.getenv(
        "KAFKA_TOPIC",
        "equipment-events",
    )

    group_id = os.getenv(
        "KAFKA_GROUP_ID",
        "equipment-agregate-lab",
    )

    sink_mode = os.getenv(
        "SINK_MODE",
        "console",
    ).strip().lower()

    parallelism = int(
        os.getenv("FLINK_PARALLELISM", "2")
    )

    validate_sink_configuration(sink_mode)

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(parallelism)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "equipment-kafka-source",
    )

    normalized = raw_stream.map(
        normalize_event,
        output_type=Types.STRING(),
    )

    processed = (
        normalized
        .key_by(
            lambda value: json.loads(value)["equipment_id"]
        )
        .process(
            EquipmentCounter(),
            output_type=Types.STRING(),
        )
    )

    if sink_mode == "couchbase":

        LOG.info(
            "Configuring Couchbase sink: bucket=%s scope=%s collection=%s",
            os.getenv("COUCHBASE_BUCKET"),
            os.getenv("COUCHBASE_SCOPE", "_default"),
            os.getenv("COUCHBASE_COLLECTION", "_default"),
        )

        (
            processed
            .flat_map(
                CouchbaseUpsertFlatMap(),
                output_type=Types.STRING(),
            )
            .print()
            .name("equipment-couchbase-upsert-sink")
        )

    elif sink_mode == "console":

        processed.print().name(
            "equipment-console-sink"
        )

    LOG.info(
        "Starting equipment-agregate: "
        "bootstrap=%s topic=%s group=%s parallelism=%s sink=%s",
        bootstrap_servers,
        topic,
        group_id,
        parallelism,
        sink_mode,
    )

    env.execute("equipment-agregate")


if __name__ == "__main__":
    main()