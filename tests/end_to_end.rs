//! Broker integration from the Python sender through Minimq to mock flash.

use embassy_futures::block_on;
use embedded_io_async::{ErrorType, Read, Write};
use minimq::{Buffers, ConfigBuilder, Connection, Session};
use ota_mqtt::{Config, FlashRequest, Handle, Service, Step};
use std::fs;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::vec::Vec;

const IMAGE: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
const CHUNK_SIZE: usize = 4;

fn init_host_logging() {
    static HOST_LOGGING: OnceLock<()> = OnceLock::new();
    HOST_LOGGING.get_or_init(|| {
        let _ = env_logger::builder().is_test(true).try_init();
        defmt2log::init_from_current_exe();
    });
}

struct TcpIo(TcpStream);

impl TcpIo {
    fn connect(endpoint: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(endpoint)?;
        let timeout = Some(Duration::from_secs(30));
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(timeout)?;
        Ok(Self(stream))
    }
}

impl ErrorType for TcpIo {
    type Error = std::io::Error;
}

impl Read for TcpIo {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buffer)
    }
}

impl Write for TcpIo {
    async fn write(&mut self, buffer: &[u8]) -> Result<usize, Self::Error> {
        self.0.write(buffer)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

enum OwnedFlashRequest {
    Prepare {
        size: u32,
    },
    Write {
        offset: u32,
        payload: Vec<u8>,
        image_size: u32,
        fnv1a64: Option<u64>,
    },
}

#[derive(Default)]
struct MockFlash {
    pending: Option<OwnedFlashRequest>,
    prepared: Option<u32>,
    image: Vec<u8>,
    finished: bool,
}

impl MockFlash {
    fn submit(&mut self, request: FlashRequest<'_>) {
        assert!(self.pending.is_none());
        self.pending = Some(match request {
            FlashRequest::Prepare { size } => OwnedFlashRequest::Prepare { size },
            FlashRequest::Write(write) => OwnedFlashRequest::Write {
                offset: write.offset,
                payload: write.payload.to_vec(),
                image_size: write.image_size,
                fnv1a64: write.fnv1a64,
            },
        });
    }

    fn complete(&mut self, ota: &mut Service) {
        let success = match self.pending.take().unwrap() {
            OwnedFlashRequest::Prepare { size } => {
                self.prepared = Some(size);
                self.image = vec![0xff; size as usize];
                true
            }
            OwnedFlashRequest::Write {
                offset,
                payload,
                image_size,
                fnv1a64,
            } => {
                let start = offset as usize;
                let end = (start + payload.len()).min(image_size as usize);
                self.image[start..end].copy_from_slice(&payload[..end - start]);
                self.finished =
                    fnv1a64.is_some_and(|expected| expected == fnv1a64_hash(&self.image));
                fnv1a64.is_none() || self.finished
            }
        };
        ota.complete_flash(success);
    }
}

struct TempImage(PathBuf);

impl TempImage {
    fn new(unique: u128) -> Self {
        let path =
            std::env::temp_dir().join(format!("ota-mqtt-{unique}-{}.bin", std::process::id()));
        fs::write(&path, IMAGE).unwrap();
        Self(path)
    }
}

impl Drop for TempImage {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

struct Feeder(Option<Child>);

impl Feeder {
    fn assert_running(&mut self) {
        let child = self.0.as_mut().unwrap();
        if child.try_wait().unwrap().is_none() {
            return;
        }
        let output = self.0.take().unwrap().wait_with_output().unwrap();
        panic!(
            "OTA feeder exited before completion\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn wait(mut self) -> Output {
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}

impl Drop for Feeder {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
#[ignore = "requires BROKER and the ota-mqtt host command"]
fn python_feeder_through_broker_to_mock_flash() {
    init_host_logging();
    let broker = std::env::var("BROKER").expect("set BROKER=host[:port]");
    let endpoint = if broker.contains(':') {
        broker.clone()
    } else {
        format!("{broker}:1883")
    };
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("ota-mqtt/test/{unique}/ota");
    let client_id = format!("ota-device-{unique}");

    let mut rx = [0; 512];
    let mut tx = [0; 1024];
    let mut session = Session::new(
        ConfigBuilder::new(Buffers::new(&mut rx, &mut tx))
            .client_id(&client_id)
            .unwrap(),
    );
    let mut connection =
        block_on(session.connect(TcpIo::connect(&endpoint).expect("connect OTA device to broker")))
            .unwrap();
    let mut ota = Service::new(
        &prefix,
        Config {
            capacity: 16,
            max_chunk_size: CHUNK_SIZE,
            write_size: CHUNK_SIZE,
        },
    )
    .unwrap();
    ota.begin_startup(connection.connect_event());
    drain_local(&mut ota, &mut connection);

    let image = TempImage::new(unique);
    let feeder = std::env::var("OTA_MQTT_FEEDER").unwrap_or_else(|_| "ota-mqtt".to_owned());
    let mut feeder = Feeder(Some(
        Command::new(feeder)
            .args(["--broker", &broker, "--prefix", &prefix, "--image"])
            .arg(&image.0)
            .args(["--chunk-size", "4", "--timeout", "5"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start Python OTA feeder"),
    ));

    let mut flash = MockFlash::default();
    loop {
        feeder.assert_running();
        match block_on(ota.step(&mut connection)).unwrap() {
            Step::Pending => {
                handle_inbound(&mut ota, &mut flash, block_on(connection.poll()).unwrap())
            }
            Step::Quiescent if flash.pending.is_some() => flash.complete(&mut ota),
            Step::Quiescent if ota.ready_to_reboot() => break,
            Step::Quiescent => {
                handle_inbound(&mut ota, &mut flash, block_on(connection.poll()).unwrap())
            }
        }
    }

    let output = feeder.wait();
    assert!(
        output.status.success(),
        "Python OTA feeder failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(flash.prepared, Some(8));
    assert_eq!(flash.image, IMAGE);
    assert!(flash.finished);
}

fn drain_local(ota: &mut Service, connection: &mut Connection<'_, '_, TcpIo>) {
    loop {
        match block_on(ota.step(connection)).unwrap() {
            Step::Quiescent => return,
            Step::Pending => {
                assert!(block_on(connection.poll()).unwrap().is_none());
            }
        }
    }
}

fn handle_inbound(
    ota: &mut Service,
    flash: &mut MockFlash,
    inbound: Option<minimq::InboundPublish<'_>>,
) {
    let Some(inbound) = inbound else {
        return;
    };
    match ota.handle(&inbound) {
        Handle::Flash(request) => flash.submit(request),
        Handle::Consumed => {}
        Handle::Unhandled => panic!("unexpected non-OTA publish"),
    }
}

fn fnv1a64_hash(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf29ce484222325, |digest, byte| {
        (digest ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}
