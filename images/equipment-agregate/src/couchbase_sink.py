
import json
import logging
import os
from datetime import timedelta

from pyflink.datastream.functions import SinkFunction

logger = logging.getLogger("equipment-couchbase-sink")


class CouchbaseSink(SinkFunction):
    """
    PyFlink sink that writes equipment aggregate documents to Couchbase.

    Uses deterministic document IDs and upsert operations.
    This provides idempotent writes for repeated writes to the same ID,
    but does not implement Flink checkpoint-aligned exactly-once commits.
    """

    def __init__(
        self,
        connection_string,
        username,
        password,
        bucket_name,
        scope_name,
        collection_name,
        document_id_field="aggregateId",
        connection_timeout_seconds=15,
    ):
        self.connection_string = connection_string
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name
        self.document_id_field = document_id_field
        self.connection_timeout_seconds = connection_timeout_seconds

        # Connections are created inside each TaskManager instance.
        self.cluster = None
        self.collection = None

    def open(self, runtime_context):
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        auth = PasswordAuthenticator(
            self.username,
            self.password,
        )

        self.cluster = Cluster.connect(
            self.connection_string,
            ClusterOptions(auth),
        )

        self.cluster.wait_until_ready(
            timedelta(seconds=self.connection_timeout_seconds)
        )

        bucket = self.cluster.bucket(self.bucket_name)

        self.collection = (
            bucket
            .scope(self.scope_name)
            .collection(self.collection_name)
        )

        logger.info(
            "Connected to Couchbase: bucket=%s scope=%s collection=%s",
            self.bucket_name,
            self.scope_name,
            self.collection_name,
        )

    def invoke(self, value, context):
        if isinstance(value, str):
            document = json.loads(value)
        else:
            document = value

        if not isinstance(document, dict):
            raise ValueError("Couchbase sink expects a JSON object")

        document_id = document.get(self.document_id_field)

        if document_id is None or str(document_id).strip() == "":
            raise ValueError(
                f"Document is missing required ID field: "
                f"{self.document_id_field}"
            )

        try:
            self.collection.upsert(
                str(document_id),
                document,
            )

            logger.debug(
                "Document successfully written to Couchbase: %s",
                document_id,
            )

        except Exception:
            logger.exception(
                "Failed to write document to Couchbase: %s",
                document_id,
            )
            # Do not swallow the exception. Let Flink restart/recover
            # instead of silently losing the output record.
            raise

    def close(self):
        if self.cluster is not None:
            self.cluster.close()
            self.cluster = None
            self.collection = None

            logger.info("Couchbase connection closed")
