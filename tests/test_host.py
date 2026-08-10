from __future__ import annotations

import unittest

from ota_mqtt import (
    Status,
    aligned_chunk_size,
    broker_endpoint,
    chunk_properties,
    json_manifest,
)


class Message:
    def __init__(self, payload: bytes, properties) -> None:
        self.payload = payload
        self.properties = properties


def status_properties():
    return {
        "response_topic": "devices/example/ota/chunk",
        "correlation_data": b"resource",
        "user_property": [("offset", "1024")],
    }


class HostToolTests(unittest.TestCase):
    def test_aligned_chunk_size_fits_mtu_and_write_size(self) -> None:
        self.assertEqual(aligned_chunk_size(1500, 1024, 32), 1024)
        self.assertEqual(aligned_chunk_size(1000, 1024, 32), 992)
        self.assertEqual(aligned_chunk_size(64, 16, 32), 0)

    def test_broker_endpoint(self) -> None:
        self.assertEqual(broker_endpoint("mqtt"), ("mqtt", 1883))
        self.assertEqual(broker_endpoint("mqtt.example:1884"), ("mqtt.example", 1884))
        self.assertEqual(broker_endpoint("[::1]:1884"), ("::1", 1884))
        self.assertEqual(
            broker_endpoint("mqtt://mqtt.example:1884"),
            ("mqtt.example", 1884),
        )

    def test_status_parses_receipt_payload(self) -> None:
        status = Status.from_message(
            Message(
                b'{"state":"receiving","code":"accepted","id":"23",'
                b'"next_offset":1024,"size":2048,"mtu":1024,"write_size":32}',
                status_properties(),
            )
        )

        self.assertEqual(status.state, "receiving")
        self.assertEqual(status.code, "accepted")
        self.assertEqual(status.response_topic, "devices/example/ota/chunk")
        self.assertEqual(status.correlation_data, b"resource")
        self.assertEqual(status.offset, 1024)
        self.assertEqual(status.next_offset, 1024)
        self.assertEqual(status.size, 2048)
        self.assertEqual(status.mtu, 1024)
        self.assertEqual(status.write_size, 32)

    def test_status_raises_on_rejection(self) -> None:
        status = Status.from_message(
            Message(
                b'{"state":"error","code":"offset","id":"23",'
                b'"next_offset":0,"size":2048,"mtu":1024,"write_size":32}',
                status_properties(),
            )
        )

        with self.assertRaisesRegex(RuntimeError, "rejected"):
            status.raise_for_error()

    def test_chunk_properties_echo_request_correlation_and_offset(self) -> None:
        properties = chunk_properties(b"opaque", 37)

        self.assertEqual(properties["payload_format_id"], 0)
        self.assertEqual(properties["correlation_data"], b"opaque")
        self.assertEqual(properties["user_property"], [("offset", "37")])

    def test_json_manifest_is_compact_and_deterministic(self) -> None:
        manifest = json_manifest("devices/example/ota", b"foobar", 23)

        self.assertEqual(
            manifest,
            b'{"size":6,"resource":"devices/example/ota/85944171f73967e8",'
            b'"sequence":23,"digest":9625390261332436968}',
        )


if __name__ == "__main__":
    unittest.main()
