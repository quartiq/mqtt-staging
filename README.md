# ota-mqtt

`ota-mqtt` stages firmware over MQTT 5. It provides a `no_std` Rust state
machine for the device and a Python sender for the host. It does not own the
network stack, flash task, allocation, timeouts, or reboot policy.

The JSON trigger and FNV-1a digest detect transfer errors; they do not
authenticate firmware. Signature and device-policy checks belong in the
bootloader.

## Host

With an MQTT broker running:

```console
pipx install .
ota-mqtt --prefix devices/example/ota --image firmware.bin
```

`--broker` defaults to `$BROKER`, then `localhost:1883`. From a checkout,
`uv run --project . ota-mqtt ...` or `python ota_mqtt.py ...` runs the same
command.

`--prefix` is the protocol root chosen by the application. The tool appends
only `/trigger`, `/status`, and `/chunk`; `/ota` and `/dfu` are not imposed.

## Device

```toml
[dependencies]
ota-mqtt = "0.1"
```

Create the service with the staging capacity, retained chunk capacity, and
flash write alignment:

```rust,ignore
use embassy_futures::select::{Either, select};

let mut ota = ota_mqtt::Service::new(
    "devices/example/ota",
    ota_mqtt::Config {
        capacity: staging.capacity(),
        max_chunk_size: flash.max_chunk_size(),
        write_size: staging.write_size(),
    },
)?;

let mut connection = session.connect(io).await?;
ota.begin_startup(connection.connect_event());

loop {
    let _ = ota.step(&mut connection).await?;

    match select(connection.poll(), flash.wait_result()).await {
        Either::First(publish) => {
            if let Some(publish) = publish? {
                match ota.handle(&publish) {
                    ota_mqtt::Handle::Unhandled => handle_application_publish(publish),
                    ota_mqtt::Handle::Consumed => {}
                    ota_mqtt::Handle::Flash(request) => {
                        // `flash` is application-owned. If it runs concurrently,
                        // submit() copies the borrowed payload before returning.
                        if !flash.submit(request) {
                            ota.complete_flash(false);
                        }
                    }
                }
            }
        }
        Either::Second(success) => ota.complete_flash(success),
    }

    if ota.ready_to_reboot() {
        apply_shutdown_and_reboot_policy();
    }
}
```

Call `begin_startup()` after every connection. Keep calling `step()` to drain
subscriptions and status publishes. Route every inbound publish through
`handle()`, and report each emitted flash request once with
`complete_flash()`.

`FlashWrite::payload` borrows Minimq's current RX packet and cannot survive
another connection operation. A synchronous flash owner may consume it
directly. A concurrent worker must copy it once into application-owned static
or allocated storage. Its completion path must wake the owner independently of
new broker traffic, as `flash.wait_result()` does above. Flash preparation,
erasure, final verification, bootloader state, and reboot remain application
responsibilities.

## Protocol

For a prefix such as `devices/example/ota`:

| Topic | Direction | Payload |
| --- | --- | --- |
| `<prefix>/trigger` | host to device | JSON trigger |
| `<prefix>/status` | device to host | JSON status and request properties |
| `<prefix>/chunk` | host to device | firmware bytes |

The trigger is:

```json
{
  "size": 4096,
  "resource": "devices/example/ota/85944171f73967e8",
  "sequence": 23,
  "digest": 9625390261332434920
}
```

Status reports `state`, `code`, `id`, `next_offset`, `size`, `mtu`, and
`write_size`. Its MQTT properties request the next chunk:

- `ResponseTopic`: `<prefix>/chunk`
- `CorrelationData`: the trigger's resource identifier
- `UserProperty("offset", decimal)`: requested byte offset

The host mirrors the correlation data and offset in its QoS 1 chunk publish.
Chunks are sequential. Duplicates are acknowledged without another flash
write, future offsets are rejected, and final alignment padding must be
`0xff`.

`begin_startup()`, `handle()`, `status()`, `abort()`, and `complete_flash()`
only update local state. `step()` performs at most one MQTT operation and is
cancellation-safe when the underlying Minimq I/O is cancellation-safe.

## Tests

```console
python -m unittest discover -s tests -p 'test_*.py'
cargo test --all-targets
BROKER=localhost:1883 cargo test --test end_to_end -- --ignored
```

The broker integration runs the Python command through MQTT and Minimq into
mock flash. Set `OTA_MQTT_FEEDER` to override the command.

## License

Licensed under either Apache-2.0 or MIT.
