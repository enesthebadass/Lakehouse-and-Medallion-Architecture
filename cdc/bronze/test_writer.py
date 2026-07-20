import json
import unittest
from datetime import datetime, timezone

from botocore.exceptions import ClientError

import writer


class FakeMessage:
    def __init__(self, topic, key, value, partition=0, offset=7, timestamp_ms=1_000):
        self._topic = topic
        self._key = key
        self._value = value
        self._partition = partition
        self._offset = offset
        self._timestamp_ms = timestamp_ms

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def timestamp(self):
        return 0, self._timestamp_ms


class ExistingObjectClient:
    def __init__(self, record, checksum_override=None):
        self.record = record
        self.checksum_override = checksum_override
        self.put_arguments = None

    def put_object(self, **kwargs):
        self.put_arguments = kwargs
        raise ClientError(
            {
                "Error": {"Code": "PreconditionFailed", "Message": "exists"},
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )

    def head_object(self, **_kwargs):
        return {
            "Metadata": {
                "event-id": self.record.event_id,
                "payload-sha256": self.checksum_override or self.record.payload_sha256,
            }
        }


class BronzeWriterTests(unittest.TestCase):
    def setUp(self):
        self.envelope = {
            "before": {"customer_id": 42, "status": "ACTIVE"},
            "after": {"customer_id": 42, "status": "PASSIVE"},
            "op": "u",
            "source": {
                "schema": "mms",
                "table": "customers",
                "ts_ms": 1_000,
                "lsn": 12345,
            },
            "transaction": {"id": "10:20", "total_order": 1},
        }
        self.message = FakeMessage(
            "bank.core.mms.customers",
            json.dumps({"customer_id": 42}).encode(),
            json.dumps(self.envelope).encode(),
        )

    def test_build_record_preserves_envelope_and_kafka_coordinate(self):
        record = writer.build_record(
            self.message,
            ingested_at=datetime(2026, 7, 17, tzinfo=timezone.utc),
        )
        document = json.loads(record.body)

        self.assertEqual(document["value"], self.envelope)
        self.assertEqual(document["key"], {"customer_id": 42})
        self.assertEqual(document["_metadata"]["kafka_offset"], 7)
        self.assertEqual(document["_metadata"]["operation"], "u")
        self.assertIn("schema=mms/table=customers", record.object_key)
        self.assertTrue(record.object_key.endswith("offset=00000000000000000007.json"))

    def test_build_record_preserves_delete_tombstone(self):
        message = FakeMessage(
            "bank.core.mms.customer_contacts",
            json.dumps({"contact_id": 9}).encode(),
            None,
        )
        record = writer.build_record(message)
        document = json.loads(record.body)

        self.assertIsNone(document["value"])
        self.assertEqual(document["_metadata"]["operation"], "tombstone")
        self.assertEqual(record.source_table, "customer_contacts")

    def test_build_record_canonicalizes_oracle_identifier_case(self):
        envelope = {
            "before": None,
            "after": {"CUSTOMER_NO": "C0001"},
            "op": "c",
            "source": {
                "connector": "oracle",
                "schema": "MMS",
                "table": "CUSTOMERS",
                "ts_ms": 1_000,
                "scn": "123456",
                "commit_scn": "123460",
            },
        }
        message = FakeMessage(
            "bank.core.MMS.CUSTOMERS",
            json.dumps({"CUSTOMER_NO": "C0001"}).encode(),
            json.dumps(envelope).encode(),
        )

        record = writer.build_record(message)
        document = json.loads(record.body)

        self.assertEqual(record.source_schema, "mms")
        self.assertEqual(record.source_table, "customers")
        self.assertIn("schema=mms/table=customers", record.object_key)
        self.assertEqual(document["value"], envelope)

    def test_replay_verifies_existing_object_without_overwrite(self):
        record = writer.build_record(self.message)
        client = ExistingObjectClient(record)

        self.assertFalse(writer.persist_record(client, record))
        self.assertEqual(client.put_arguments["IfNoneMatch"], "*")

    def test_replay_rejects_checksum_conflict(self):
        record = writer.build_record(self.message)
        client = ExistingObjectClient(record, checksum_override="different")

        with self.assertRaises(RuntimeError):
            writer.persist_record(client, record)


if __name__ == "__main__":
    unittest.main()
