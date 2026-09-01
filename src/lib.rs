#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), no_std)]

use defmt::{debug, info, trace, warn};
use heapless::{String, Vec};
use minimq::{
    ConnectEvent, Connection, Error as MqttError, InboundPublish, Io, Op, Properties, Property,
    PubError, Publication, QoS, ResourceError, RetainHandling, SubscriptionOptions, TopicFilter,
};
use serde::{Deserialize, Serialize};

type TopicString = String<128>;

const MAX_UPDATE_ID: usize = 48;
const MAX_RESOURCE_ID_BYTES: usize = 128;
const MAX_STATUS_BYTES: usize = 160;
const INFO_CHUNK_STRIDE: usize = 128;
const TRIGGER_SUFFIX: &str = "/trigger";
const STATUS_SUFFIX: &str = "/status";
const CHUNK_SUFFIX: &str = "/chunk";

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedManifestProfile {
    sequence: u64,
    size: u32,
    resource_id: Vec<u8, MAX_RESOURCE_ID_BYTES>,
    fnv1a64: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Trigger<'a> {
    size: u32,
    resource: &'a str,
    sequence: u64,
    digest: u64,
}

/// Immediate result of one cooperative `step()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "inspect whether OTA work is still pending"]
pub enum Step {
    /// No queued OTA MQTT work remains after this step.
    Quiescent,
    /// OTA still has queued or in-flight MQTT work.
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Error returned while constructing an OTA service.
pub enum CreateError {
    /// A derived OTA topic does not fit the fixed topic buffer.
    Topic(ResourceError),
    /// The maximum chunk size or flash write size cannot support aligned writes.
    InvalidChunkSize,
}

/// Storage and flash limits for one OTA service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Maximum staged image size in bytes.
    pub capacity: u32,
    /// Maximum firmware bytes accepted in one MQTT chunk.
    pub max_chunk_size: usize,
    /// Required flash-write alignment in bytes.
    pub write_size: usize,
}

impl core::fmt::Display for CreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Topic(_) => f.write_str("OTA topic does not fit buffer"),
            Self::InvalidChunkSize => f.write_str("invalid OTA chunk or write size"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, defmt::Format)]
#[serde(rename_all = "kebab-case")]
/// Current OTA transfer phase.
pub enum State {
    /// No update is active.
    Idle,
    /// The flash backend is preparing the staging area.
    Preparing,
    /// The device is ready for the next chunk.
    Receiving,
    /// The flash backend is writing one chunk.
    Writing,
    /// The full image was staged.
    Complete,
    /// The transfer failed or was aborted.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, defmt::Format)]
#[serde(rename_all = "kebab-case")]
/// Result conveyed by an OTA status publication.
pub enum StatusCode {
    /// No update is active.
    Idle,
    /// The manifest or latest chunk was accepted.
    Accepted,
    /// An already accepted chunk was received again.
    Duplicate,
    /// The chunk did not match the requested offset.
    Offset,
    /// The image or chunk exceeds a configured limit.
    Oversize,
    /// The MQTT RX packet budget cannot carry a configured chunk.
    Mtu,
    /// The chunk does not meet the flash write alignment.
    Unaligned,
    /// The flash backend rejected an operation.
    Backend,
    /// The image was staged.
    Complete,
    /// The request or service state was invalid.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
/// Current OTA progress and device transfer limits.
pub struct Status<'a> {
    /// Current transfer state.
    pub state: State,
    /// Result associated with this status publication.
    pub code: StatusCode,
    /// Decimal manifest sequence number, or an empty string while idle.
    pub id: &'a str,
    /// First image byte not yet written.
    pub next_offset: u32,
    /// Declared image size in bytes.
    pub size: u32,
    /// Maximum chunk payload accepted by this service.
    pub mtu: usize,
    /// Required chunk alignment in bytes.
    pub write_size: usize,
}

#[derive(Debug)]
struct Update {
    id: String<MAX_UPDATE_ID>,
    resource_id: Vec<u8, MAX_RESOURCE_ID_BYTES>,
    size: u32,
    next_offset: u32,
    fnv1a64: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Publish(StatusCode),
    Subscribe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkError {
    Queue(StatusCode),
    Fail(StatusCode),
}

struct InFlightAction {
    action: Action,
    op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FlashOp {
    Prepare,
    Write {
        offset: u32,
        next_offset: u32,
        final_chunk: bool,
    },
}

/// Flash operation requested by the OTA state machine.
#[derive(Debug)]
pub enum FlashRequest<'a> {
    /// Prepare the staging slot for one image.
    Prepare {
        /// Declared image size in bytes.
        size: u32,
    },
    /// Write one accepted chunk and optionally finalize the image.
    Write(FlashWrite<'a>),
}

/// One accepted chunk ready for flash.
#[derive(Debug)]
pub struct FlashWrite<'a> {
    /// Firmware byte offset.
    pub offset: u32,
    /// Chunk bytes borrowed from the current Minimq inbound publish. The
    /// owner must consume or copy them before polling the connection again.
    pub payload: &'a [u8],
    /// Manifest image size, excluding any final erased-flash padding.
    pub image_size: u32,
    /// Expected FNV-1a checksum. Present only on the final chunk.
    pub fnv1a64: Option<u64>,
}

/// Result of handling one inbound MQTT publish.
#[must_use = "submit flash work or route unhandled traffic"]
#[derive(Debug)]
pub enum Handle<'a> {
    /// The publish is not OTA traffic.
    Unhandled,
    /// OTA traffic was consumed without new flash work.
    Consumed,
    /// The owner must execute this flash request and report its result with
    /// `Service::complete_flash`.
    Flash(FlashRequest<'a>),
}

/// Cooperative MQTT OTA service.
pub struct Service {
    prefix: TopicString,
    inflight: Option<InFlightAction>,
    pending: Option<Action>,
    startup_publish: Option<StatusCode>,
    state: State,
    update: Option<Update>,
    flash: Option<FlashOp>,
    capacity: u32,
    max_chunk_size: usize,
    max_rx_packet_size: usize,
    write_size: usize,
}

impl Service {
    /// Create an OTA service at the exact protocol topic root supplied by the
    /// application.
    pub fn new(prefix: &str, config: Config) -> Result<Self, CreateError> {
        if config.write_size == 0
            || config.max_chunk_size == 0
            || !config.max_chunk_size.is_multiple_of(config.write_size)
        {
            return Err(CreateError::InvalidChunkSize);
        }
        let prefix = TopicString::try_from(prefix)
            .map_err(|_| CreateError::Topic(ResourceError::BufferTooSmall))?;
        let _ = topic(&prefix, TRIGGER_SUFFIX).map_err(CreateError::Topic)?;
        let _ = topic(&prefix, STATUS_SUFFIX).map_err(CreateError::Topic)?;
        let _ = topic(&prefix, CHUNK_SUFFIX).map_err(CreateError::Topic)?;
        Ok(Self {
            prefix,
            inflight: None,
            pending: None,
            startup_publish: None,
            state: State::Idle,
            update: None,
            flash: None,
            capacity: config.capacity,
            max_chunk_size: config.max_chunk_size,
            max_rx_packet_size: 0,
            write_size: config.write_size,
        })
    }

    /// Begin OTA startup for one MQTT connect event.
    ///
    /// Pure local state update. Cancel-safe.
    pub fn begin_startup(&mut self, event: ConnectEvent) {
        let replay = self.replay_status();
        match event {
            ConnectEvent::Connected => {
                self.inflight = None;
                self.startup_publish = replay;
                self.queue(Action::Subscribe);
            }
            ConnectEvent::Reconnected => {
                self.startup_publish = None;
                if let Some(code) = replay {
                    self.queue(Action::Publish(code));
                }
            }
        }
    }

    /// Return the current OTA status.
    ///
    /// Pure query. Cancel-safe.
    pub fn status(&self) -> Status<'_> {
        self.status_with(self.current_code())
    }

    /// Return whether OTA has reached a terminal complete state and there is
    /// no remaining queued or in-flight MQTT work.
    pub fn ready_to_reboot(&self) -> bool {
        self.state == State::Complete
            && self.flash.is_none()
            && self.pending.is_none()
            && self.inflight.is_none()
            && self.startup_publish.is_none()
    }

    fn current_code(&self) -> StatusCode {
        match self.state {
            State::Idle => StatusCode::Idle,
            State::Preparing | State::Receiving | State::Writing => StatusCode::Accepted,
            State::Complete => StatusCode::Complete,
            State::Error => StatusCode::Error,
        }
    }

    fn status_with(&self, code: StatusCode) -> Status<'_> {
        let update = self.update.as_ref();
        Status {
            state: self.state,
            code,
            id: update.map_or("", |update| update.id.as_str()),
            next_offset: update.map_or(0, |update| update.next_offset),
            size: update.map_or(0, |update| update.size),
            mtu: self.max_chunk_size,
            write_size: self.write_size,
        }
    }

    /// Abort an active OTA transfer.
    ///
    /// Pure local state update. Cancel-safe.
    ///
    /// Returns `true` if an OTA transfer was active and is now marked as
    /// failed. A later `step()` or reconnect startup replay will publish
    /// `error` status for the same transfer.
    pub fn abort(&mut self) -> bool {
        if !matches!(
            self.state,
            State::Preparing | State::Receiving | State::Writing
        ) {
            return false;
        }
        warn!(
            "Aborting OTA transfer state={:?} offset={=u32} size={=u32}",
            self.state,
            self.status().next_offset,
            self.status().size
        );
        self.state = State::Error;
        self.flash = None;
        if matches!(self.inflight.as_ref(), Some(inflight) if inflight.action == Action::Subscribe)
            || self.pending == Some(Action::Subscribe)
        {
            self.startup_publish = Some(StatusCode::Error);
        } else {
            self.queue(Action::Publish(StatusCode::Error));
        }
        true
    }

    /// Handle one inbound publish.
    ///
    /// Pure local state update returning chunk data borrowed from `inbound`.
    /// This is cancel-safe.
    pub fn handle<'a>(&mut self, inbound: &InboundPublish<'a>) -> Handle<'a> {
        self.handle_publish_with_properties(
            inbound.topic(),
            inbound.payload(),
            inbound.properties(),
        )
    }

    /// Complete one previously emitted flash request.
    ///
    /// Pure local state update. Cancel-safe.
    pub fn complete_flash(&mut self, success: bool) {
        let Some(flash) = self.flash.take() else {
            return;
        };
        let Some(update) = self.update.as_mut() else {
            self.state = State::Error;
            self.queue(Action::Publish(StatusCode::Error));
            return;
        };
        if !success {
            warn!(
                "OTA flash step failed state={:?} offset={=u32} size={=u32}",
                self.state, update.next_offset, update.size
            );
            self.state = State::Error;
            self.queue(Action::Publish(StatusCode::Backend));
            return;
        }
        match flash {
            FlashOp::Prepare => {
                self.state = State::Receiving;
                info!("Accepted OTA manifest size={=u32}", update.size);
                self.queue(Action::Publish(StatusCode::Accepted));
            }
            FlashOp::Write {
                next_offset,
                final_chunk,
                ..
            } => {
                if final_chunk {
                    update.next_offset = update.size;
                    self.state = State::Complete;
                    info!("Completed OTA image size={=u32}", update.size);
                    self.queue(Action::Publish(StatusCode::Complete));
                } else {
                    debug!(
                        "OTA chunk write completed next_offset={=u32} size={=u32}",
                        next_offset, update.size
                    );
                    update.next_offset = next_offset;
                    self.state = State::Receiving;
                    self.queue(Action::Publish(StatusCode::Accepted));
                }
            }
        }
    }

    fn handle_publish_with_properties<'a>(
        &mut self,
        topic: &str,
        payload: &'a [u8],
        properties: &Properties<'_>,
    ) -> Handle<'a> {
        if topic.strip_prefix(self.prefix.as_str()) == Some(TRIGGER_SUFFIX) {
            debug!("OTA trigger received payload={=usize}B", payload.len());
            return self.handle_trigger(payload);
        }
        if topic.strip_prefix(self.prefix.as_str()) != Some(CHUNK_SUFFIX) {
            return Handle::Unhandled;
        }
        let Some(chunk) = chunk_properties(properties) else {
            warn!("Rejecting OTA chunk without required properties");
            return Handle::Consumed;
        };
        self.handle_chunk(chunk.resource_id, chunk.offset, payload)
    }

    /// Advance one queued MQTT operation.
    ///
    /// This is the cooperative queue-drain API. It performs at most one local
    /// queued subscribe or status-publish step and does not wait for future
    /// inbound reads on its own.
    ///
    /// Cancel-safe if the underlying transport I/O futures are cancel-safe.
    /// The current action stays at the front of the local queue until the MQTT
    /// operation is known to have completed or been invalidated.
    pub async fn step<IO>(
        &mut self,
        connection: &mut Connection<'_, '_, IO>,
    ) -> Result<Step, MqttError<IO::Error>>
    where
        IO: Io,
    {
        if let Some(inflight) = self.inflight.take() {
            if connection.is_pending(&inflight.op) {
                self.inflight = Some(inflight);
                return Ok(Step::Pending);
            }
            if connection.is_invalidated(&inflight.op) {
                self.queue(inflight.action);
                return Err(MqttError::Disconnected);
            }
            if inflight.action == Action::Subscribe
                && let Some(code) = self.startup_publish.take()
            {
                self.queue(Action::Publish(code));
            }
        }

        let Some(action) = self.pending.take() else {
            return Ok(Step::Quiescent);
        };
        let op = match self.start_action(connection, action).await {
            Ok(op) => op,
            Err(error) => {
                self.pending = Some(action);
                return Err(error);
            }
        };
        self.inflight = op.map(|op| InFlightAction { action, op });
        Ok(if self.inflight.is_none() && self.pending.is_none() {
            Step::Quiescent
        } else {
            Step::Pending
        })
    }

    fn handle_trigger<'a>(&mut self, payload: &'a [u8]) -> Handle<'a> {
        let Ok(manifest) = parse_trigger(payload) else {
            warn!("Rejecting OTA trigger: invalid manifest");
            if self.state == State::Idle {
                self.queue(Action::Publish(StatusCode::Error));
            }
            return Handle::Consumed;
        };
        let Ok(id) = manifest_id(manifest.sequence) else {
            warn!("Rejecting OTA trigger: invalid sequence");
            self.queue(Action::Publish(StatusCode::Error));
            return Handle::Consumed;
        };
        if self.state != State::Idle {
            if self.update.as_ref().is_some_and(|update| {
                update.id == id
                    && update.size == manifest.size
                    && update.resource_id == manifest.resource_id
                    && update.fnv1a64 == manifest.fnv1a64
            }) {
                debug!("Replaying status for duplicate OTA trigger");
                self.queue(Action::Publish(self.current_code()));
                return Handle::Consumed;
            }
            warn!(
                "Rejecting overlapping OTA trigger state={:?} offset={=u32}",
                self.state,
                self.status().next_offset
            );
            return Handle::Consumed;
        }
        if manifest.size == 0 {
            warn!("Rejecting OTA trigger: zero-length image");
            self.queue(Action::Publish(StatusCode::Error));
            return Handle::Consumed;
        }
        if manifest.size > self.capacity {
            warn!(
                "Rejecting OTA trigger: image oversize size={=u32} capacity={=u32}",
                manifest.size, self.capacity
            );
            self.queue(Action::Publish(StatusCode::Oversize));
            return Handle::Consumed;
        }
        if required_rx_bytes(
            self.chunk_topic().as_str(),
            manifest.resource_id.as_slice(),
            self.max_chunk_size,
        ) > self.max_rx_packet_size
        {
            warn!(
                "Rejecting OTA trigger: chunk exceeds MQTT rx budget chunk={=usize} max_rx={=usize}",
                self.max_chunk_size, self.max_rx_packet_size
            );
            self.queue(Action::Publish(StatusCode::Mtu));
            return Handle::Consumed;
        }
        self.update = Some(Update {
            id,
            resource_id: manifest.resource_id,
            size: manifest.size,
            next_offset: 0,
            fnv1a64: manifest.fnv1a64,
        });
        self.state = State::Preparing;
        self.queue(Action::Publish(StatusCode::Accepted));
        self.flash = Some(FlashOp::Prepare);
        Handle::Flash(FlashRequest::Prepare {
            size: manifest.size,
        })
    }

    fn handle_chunk<'a>(
        &mut self,
        resource_id: &[u8],
        offset: u32,
        payload: &'a [u8],
    ) -> Handle<'a> {
        trace!(
            "OTA chunk received offset={=u32} len={=usize}",
            offset,
            payload.len()
        );
        if self.state == State::Preparing {
            debug!("Ignoring OTA chunk while prepare is still pending");
            return Handle::Consumed;
        }
        if matches!(self.state, State::Complete | State::Error) {
            self.queue(Action::Publish(self.current_code()));
            return Handle::Consumed;
        }
        let Some(update) = self.update.as_mut() else {
            debug!("Ignoring OTA chunk without active transfer");
            self.queue(Action::Publish(StatusCode::Idle));
            return Handle::Consumed;
        };
        if resource_id != update.resource_id.as_slice() {
            debug!("Ignoring OTA chunk for another transfer");
            return Handle::Consumed;
        }
        if self.state == State::Writing {
            let duplicate = matches!(
                self.flash.as_ref(),
                Some(FlashOp::Write {
                    offset: pending_offset,
                    ..
                }) if offset == *pending_offset
            );
            if duplicate {
                debug!(
                    "Ignoring duplicate in-flight OTA chunk offset={=u32}",
                    offset
                );
                return Handle::Consumed;
            }
            warn!(
                "Rejecting OTA chunk while another write is in flight expected={=u32} got={=u32}",
                update.next_offset, offset
            );
            self.queue(Action::Publish(StatusCode::Error));
            return Handle::Consumed;
        }
        if self.state != State::Receiving {
            debug!(
                "Ignoring OTA chunk while state={:?} offset={=u32}",
                self.state, update.next_offset
            );
            self.queue(Action::Publish(StatusCode::Idle));
            return Handle::Consumed;
        }
        let next_offset = match validate_chunk(
            update,
            offset,
            payload,
            self.capacity,
            self.max_chunk_size,
            self.write_size,
        ) {
            Ok(next_offset) => next_offset,
            Err(ChunkError::Queue(code)) => {
                log_chunk_queue_reject(code, update.next_offset, offset, payload.len());
                self.queue(Action::Publish(code));
                return Handle::Consumed;
            }
            Err(ChunkError::Fail(code)) => {
                warn!(
                    "Rejecting OTA chunk offset={=u32} len={=usize} code={:?}",
                    offset,
                    payload.len(),
                    code
                );
                self.state = State::Error;
                self.queue(Action::Publish(code));
                return Handle::Consumed;
            }
        };
        let final_chunk = next_offset == update.size;
        let fnv1a64 = final_chunk.then_some(update.fnv1a64);
        self.state = State::Writing;
        self.flash = Some(FlashOp::Write {
            offset,
            next_offset,
            final_chunk,
        });
        debug!(
            "Queueing OTA flash write offset={=u32} len={=usize} final={=bool}",
            offset,
            payload.len(),
            final_chunk
        );
        Handle::Flash(FlashRequest::Write(FlashWrite {
            offset,
            payload,
            image_size: update.size,
            fnv1a64,
        }))
    }

    fn replay_status(&self) -> Option<StatusCode> {
        match self.state {
            State::Idle => None,
            State::Preparing | State::Receiving | State::Writing => Some(StatusCode::Accepted),
            State::Complete => Some(StatusCode::Complete),
            State::Error => Some(StatusCode::Error),
        }
    }

    fn queue(&mut self, action: Action) {
        if matches!(
            (self.inflight.as_ref(), action),
            (Some(inflight), Action::Subscribe)
                if inflight.action == Action::Subscribe
        ) {
            return;
        }
        match (self.pending, action) {
            (Some(Action::Subscribe), Action::Subscribe) => {}
            (Some(Action::Publish(_)), Action::Publish(_)) => {
                self.pending = Some(action);
            }
            (_, action) => {
                self.pending = Some(action);
            }
        }
    }

    fn trigger_topic(&self) -> TopicString {
        topic(&self.prefix, TRIGGER_SUFFIX).expect("validated OTA trigger topic")
    }

    fn status_topic(&self) -> TopicString {
        topic(&self.prefix, STATUS_SUFFIX).expect("validated OTA status topic")
    }

    fn chunk_topic(&self) -> TopicString {
        topic(&self.prefix, CHUNK_SUFFIX).expect("validated OTA chunk topic")
    }

    async fn start_action<IO>(
        &mut self,
        connection: &mut Connection<'_, '_, IO>,
        action: Action,
    ) -> Result<Option<Op>, MqttError<IO::Error>>
    where
        IO: Io,
    {
        match action {
            Action::Subscribe => {
                info!("Subscribing OTA MQTT topics");
                self.max_rx_packet_size = connection.session().max_rx_packet_size();
                let trigger_topic = self.trigger_topic();
                let chunk_topic = self.chunk_topic();
                let filters = [
                    TopicFilter::new(trigger_topic.as_str()).options(
                        SubscriptionOptions::default()
                            .maximum_qos(QoS::AtLeastOnce)
                            .retain_behavior(RetainHandling::Never)
                            .ignore_local_messages(),
                    ),
                    TopicFilter::new(chunk_topic.as_str()).options(
                        SubscriptionOptions::default()
                            .maximum_qos(QoS::AtLeastOnce)
                            .retain_behavior(RetainHandling::Never)
                            .ignore_local_messages(),
                    ),
                ];
                Ok(Some(connection.subscribe(&filters, &[]).await?))
            }
            Action::Publish(code) => self.publish_status(connection, code).await,
        }
    }

    async fn publish_status<IO>(
        &self,
        connection: &mut Connection<'_, '_, IO>,
        code: StatusCode,
    ) -> Result<Option<Op>, MqttError<IO::Error>>
    where
        IO: Io,
    {
        let status = self.status_with(code);
        let mut payload = [0; MAX_STATUS_BYTES];
        let len = serde_json_core::ser::to_slice(&status, &mut payload)
            .map_err(|_| MqttError::Resource(ResourceError::BufferTooSmall))?;
        let mut offset = itoa::Buffer::new();
        let chunk_topic = self.chunk_topic();
        let properties = [
            Property::PayloadFormatIndicator(1),
            Property::ResponseTopic(chunk_topic.as_str()),
            Property::CorrelationData(status_correlation_data(self.update.as_ref())),
            Property::UserProperty("offset", offset.format(status.next_offset)),
        ];
        match connection
            .publish(
                Publication::new(self.status_topic().as_str(), &payload[..len])
                    .properties(&properties)
                    .qos(QoS::AtLeastOnce),
            )
            .await
        {
            Ok(op) => {
                log_status(code, status);
                Ok(op)
            }
            Err(PubError::Payload(_)) => unreachable!(),
            Err(PubError::Session(err)) => Err(err),
        }
    }
}

fn log_status(code: StatusCode, status: Status<'_>) {
    match code {
        StatusCode::Accepted => {
            if should_log_chunk_progress(status.next_offset, status.size, status.mtu) {
                info!(
                    "OTA status code={:?} offset={=u32}",
                    code, status.next_offset
                );
            } else {
                debug!(
                    "OTA status code={:?} offset={=u32}",
                    code, status.next_offset
                );
            }
        }
        StatusCode::Complete => {
            info!(
                "OTA status code={:?} offset={=u32}",
                code, status.next_offset
            );
        }
        StatusCode::Idle | StatusCode::Duplicate => {
            debug!(
                "OTA status code={:?} offset={=u32}",
                code, status.next_offset
            );
        }
        StatusCode::Offset
        | StatusCode::Oversize
        | StatusCode::Mtu
        | StatusCode::Unaligned
        | StatusCode::Backend
        | StatusCode::Error => {
            warn!(
                "OTA status code={:?} offset={=u32}",
                code, status.next_offset
            );
        }
    }
}

fn should_log_chunk_progress(next_offset: u32, size: u32, chunk_size: usize) -> bool {
    if next_offset == 0 {
        return true;
    }
    if next_offset + chunk_size as u32 >= size {
        return true;
    }
    (next_offset as usize / chunk_size).is_multiple_of(INFO_CHUNK_STRIDE)
}

fn log_chunk_queue_reject(code: StatusCode, expected_offset: u32, offset: u32, len: usize) {
    match code {
        StatusCode::Duplicate => {
            debug!(
                "Ignoring duplicate OTA chunk expected={=u32} got={=u32} len={=usize}",
                expected_offset, offset, len
            );
        }
        StatusCode::Offset => {
            warn!(
                "Rejecting OTA chunk with unexpected offset expected={=u32} got={=u32} len={=usize}",
                expected_offset, offset, len
            );
        }
        _ => {
            warn!(
                "Rejecting OTA chunk expected={=u32} got={=u32} len={=usize} code={:?}",
                expected_offset, offset, len, code
            );
        }
    }
}

fn validate_chunk(
    update: &Update,
    offset: u32,
    payload: &[u8],
    capacity: u32,
    max_chunk_size: usize,
    write_size: usize,
) -> Result<u32, ChunkError> {
    if offset < update.next_offset {
        return Err(ChunkError::Queue(StatusCode::Duplicate));
    }
    if offset != update.next_offset {
        return Err(ChunkError::Queue(StatusCode::Offset));
    }

    if payload.is_empty() || payload.len() > max_chunk_size {
        return Err(ChunkError::Fail(StatusCode::Mtu));
    }
    if !payload.len().is_multiple_of(write_size) {
        return Err(ChunkError::Fail(StatusCode::Unaligned));
    }

    let Some(end) = offset.checked_add(payload.len() as u32) else {
        return Err(ChunkError::Fail(StatusCode::Oversize));
    };
    if end > capacity {
        return Err(ChunkError::Fail(StatusCode::Oversize));
    }
    if end <= update.size {
        return Ok(end);
    }

    let Some(padding) = payload_padding(payload, update.size - offset) else {
        return Err(ChunkError::Fail(StatusCode::Oversize));
    };
    if !padding.iter().all(|&byte| byte == 0xff) {
        return Err(ChunkError::Fail(StatusCode::Oversize));
    }
    Ok(update.size)
}

fn parse_trigger(payload: &[u8]) -> Result<OwnedManifestProfile, ()> {
    let (trigger, _) = serde_json_core::from_slice::<Trigger<'_>>(payload).map_err(|_| ())?;
    let mut resource_id = Vec::new();
    resource_id
        .extend_from_slice(trigger.resource.as_bytes())
        .map_err(|_| ())?;
    Ok(OwnedManifestProfile {
        sequence: trigger.sequence,
        size: trigger.size,
        resource_id,
        fnv1a64: trigger.digest,
    })
}

struct ChunkProperties<'a> {
    resource_id: &'a [u8],
    offset: u32,
}

fn chunk_properties<'a>(properties: &'a Properties<'a>) -> Option<ChunkProperties<'a>> {
    let resource_id = properties.correlation_data()?;
    let mut offset = None;
    for property in properties.iter() {
        let Ok(Property::UserProperty(key, value)) = property else {
            continue;
        };
        if key == "offset" {
            offset = value.parse().ok();
        }
    }
    Some(ChunkProperties {
        resource_id,
        offset: offset?,
    })
}

fn manifest_id(sequence: u64) -> Result<String<MAX_UPDATE_ID>, ()> {
    let mut buffer = itoa::Buffer::new();
    let mut id = String::new();
    id.push_str(buffer.format(sequence)).map_err(|_| ())?;
    Ok(id)
}

fn status_correlation_data(update: Option<&Update>) -> &[u8] {
    update.map_or(&[], |update| update.resource_id.as_slice())
}

fn required_rx_bytes(topic: &str, resource_id: &[u8], payload_size: usize) -> usize {
    let properties = 2 // PayloadFormatIndicator property and value
        + 1 // CorrelationData property id
        + 2 // binary length prefix
        + resource_id.len()
        + 1 // UserProperty property id
        + 2 // key UTF-8 length prefix
        + "offset".len()
        + 2 // value UTF-8 length prefix
        + 10; // max u32 decimal digits
    let remaining = 2 // topic UTF-8 length prefix
        + topic.len()
        + 2 // QoS 1 packet identifier
        + mqtt_varint_len(properties)
        + properties
        + payload_size;
    1 + mqtt_varint_len(remaining) + remaining
}

fn mqtt_varint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 128 {
        value /= 128;
        len += 1;
    }
    len
}

fn payload_padding(payload: &[u8], image_bytes: u32) -> Option<&[u8]> {
    let image_bytes = usize::try_from(image_bytes).ok()?;
    (image_bytes < payload.len()).then(|| &payload[image_bytes..])
}

fn topic(prefix: &TopicString, suffix: &str) -> Result<TopicString, ResourceError> {
    let mut topic = prefix.clone();
    topic
        .push_str(suffix)
        .map_err(|_| ResourceError::BufferTooSmall)?;
    Ok(topic)
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use std::sync::OnceLock;

    const RESOURCE: &[u8] = b"dt/sinara/mpll/ota/u1";
    const SLOT: usize = 32;

    fn init_host_logging() {
        static HOST_LOGGING: OnceLock<()> = OnceLock::new();

        HOST_LOGGING.get_or_init(|| {
            let _ = env_logger::builder().is_test(true).try_init();
            defmt2log::init_from_current_exe();
        });
    }

    #[derive(Default)]
    struct Backend {
        prepared: Option<u32>,
        writes: std::vec::Vec<(u32, std::vec::Vec<u8>)>,
        finished: Option<u32>,
        fail_finish: bool,
    }

    impl Backend {
        fn apply(&mut self, flash: FlashRequest<'_>) -> bool {
            match flash {
                FlashRequest::Prepare { size } => {
                    self.prepared = Some(size);
                    true
                }
                FlashRequest::Write(write) => {
                    self.writes.push((write.offset, write.payload.to_vec()));
                    if write.fnv1a64.is_some() {
                        if self.fail_finish {
                            return false;
                        }
                        self.finished = Some(write.image_size);
                    }
                    true
                }
            }
        }
    }

    fn apply_handle(service: &mut Service, backend: &mut Backend, handle: Handle<'_>) {
        if let Handle::Flash(flash) = handle {
            service.complete_flash(backend.apply(flash));
        }
    }

    fn trigger<'a>(service: &mut Service, payload: &'a [u8]) -> Handle<'a> {
        service.handle_publish_with_properties(
            service.trigger_topic().as_str(),
            payload,
            &Properties::from_slice(&[]),
        )
    }

    fn chunk<'a>(
        service: &mut Service,
        resource: &[u8],
        offset: &str,
        payload: &'a [u8],
    ) -> Handle<'a> {
        let properties = [
            Property::CorrelationData(resource),
            Property::UserProperty("offset", offset),
        ];
        service.handle_publish_with_properties(
            service.chunk_topic().as_str(),
            payload,
            &Properties::from_slice(&properties),
        )
    }

    fn drive_trigger(service: &mut Service, backend: &mut Backend, payload: &[u8]) {
        let handle = trigger(service, payload);
        apply_handle(service, backend, handle);
    }

    fn drive_chunk(
        service: &mut Service,
        backend: &mut Backend,
        resource: &[u8],
        offset: &str,
        payload: &[u8],
    ) {
        let handle = chunk(service, resource, offset, payload);
        apply_handle(service, backend, handle);
    }

    fn service(rx: usize) -> Service {
        init_host_logging();
        let mut service = Service::new(
            "dt/sinara/mpll/dev/ota",
            Config {
                capacity: 128,
                max_chunk_size: SLOT,
                write_size: 4,
            },
        )
        .unwrap();
        service.max_rx_packet_size = rx;
        service
    }

    fn receiving_service() -> Service {
        let mut service = service(128);
        service.state = State::Receiving;
        service.update = Some(Update {
            id: "23".try_into().unwrap(),
            resource_id: Vec::from_slice(RESOURCE).unwrap(),
            size: 64,
            next_offset: 32,
            fnv1a64: 0,
        });
        service
    }

    #[test]
    fn connected_startup_queues_subscribe_only_for_idle_service() {
        let mut service = service(128);
        service.begin_startup(ConnectEvent::Connected);
        assert_eq!(service.pending, Some(Action::Subscribe));
        assert_eq!(service.startup_publish, None);
    }

    #[test]
    fn connected_startup_defers_status_replay_until_after_subscribe() {
        let mut service = receiving_service();
        service.begin_startup(ConnectEvent::Connected);
        assert_eq!(service.pending, Some(Action::Subscribe));
        assert_eq!(service.startup_publish, Some(StatusCode::Accepted));
    }

    #[test]
    fn reconnected_startup_replays_current_status_without_subscribe() {
        let mut service = receiving_service();
        service.begin_startup(ConnectEvent::Reconnected);
        assert_eq!(service.pending, Some(Action::Publish(StatusCode::Accepted)));
        assert_eq!(service.startup_publish, None);
    }

    #[test]
    fn status_reports_receiving_transfer_state() {
        let service = receiving_service();
        let status = service.status();
        assert_eq!(status.state, State::Receiving);
        assert_eq!(status.code, StatusCode::Accepted);
        assert_eq!(status.id, "23");
        assert_eq!(status.next_offset, 32);
        assert_eq!(status.size, 64);
        assert_eq!(status.mtu, SLOT);
        assert_eq!(status.write_size, 4);
    }

    #[test]
    fn abort_marks_receiving_transfer_failed() {
        let mut service = receiving_service();
        assert!(service.abort());
        assert_eq!(service.status().state, State::Error);
        assert_eq!(service.status().code, StatusCode::Error);
        assert_eq!(service.pending, Some(Action::Publish(StatusCode::Error)));
    }

    #[test]
    fn abort_is_noop_without_active_transfer() {
        let mut service = service(128);
        assert!(!service.abort());
        assert_eq!(service.status().state, State::Idle);
    }

    #[test]
    fn abort_is_noop_after_transfer_completes() {
        let mut service = service(128);
        service.state = State::Complete;
        assert!(!service.abort());
        assert_eq!(service.status().state, State::Complete);
    }

    #[test]
    fn trigger_reports_preparing_before_flash_prepare_completes() {
        let mut service = service(128);
        let handle = trigger(&mut service, br#"{"size":8,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":9126140903112366317}"#);
        assert!(matches!(
            handle,
            Handle::Flash(FlashRequest::Prepare { size: 8 })
        ));
        assert_eq!(service.status().state, State::Preparing);
        assert_eq!(service.pending, Some(Action::Publish(StatusCode::Accepted)));
    }

    mod protocol {
        use super::*;

        const JSON_TRIGGER_8: &[u8] = br#"{"size":8,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":9126140903112366317}"#;
        const JSON_TRIGGER_6: &[u8] = br#"{"size":6,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":10900469685490858058}"#;
        const JSON_TRIGGER_0: &[u8] =
            br#"{"size":0,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":14695981039346656037}"#;
        const JSON_TRIGGER_MISSING_DIGEST: &[u8] =
            br#"{"size":8,"resource":"dt/sinara/mpll/ota/u1","sequence":23}"#;
        const JSON_TRIGGER_UNKNOWN_FIELD: &[u8] =
            br#"{"size":8,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":9126140903112366317,"hash":"yafnv1a64"}"#;

        #[test]
        fn trigger_rejects_if_one_aligned_chunk_cannot_fit() {
            let mut service = service(64);
            assert!(matches!(
                trigger(&mut service, JSON_TRIGGER_8),
                Handle::Consumed
            ));
            assert_eq!(service.status().state, State::Idle);
            assert_eq!(service.status().code, StatusCode::Idle);
            assert_eq!(service.pending, Some(Action::Publish(StatusCode::Mtu)));
        }

        #[test]
        fn trigger_accepts_exact_qos1_chunk_packet_budget() {
            let required = required_rx_bytes("dt/sinara/mpll/dev/ota/chunk", RESOURCE, SLOT);
            assert!(matches!(
                trigger(&mut service(required), JSON_TRIGGER_8),
                Handle::Flash(FlashRequest::Prepare { .. })
            ));

            let mut undersized = service(required - 1);
            assert!(matches!(
                trigger(&mut undersized, JSON_TRIGGER_8),
                Handle::Consumed
            ));
            assert_eq!(undersized.pending, Some(Action::Publish(StatusCode::Mtu)));
        }

        #[test]
        fn trigger_rejects_invalid_json_profiles() {
            for payload in [
                JSON_TRIGGER_0,
                JSON_TRIGGER_MISSING_DIGEST,
                JSON_TRIGGER_UNKNOWN_FIELD,
            ] {
                let mut service = service(128);
                assert!(matches!(trigger(&mut service, payload), Handle::Consumed));
                assert_eq!(service.status().state, State::Idle);
                assert_eq!(service.pending, Some(Action::Publish(StatusCode::Error)));
            }
        }

        #[test]
        fn oversized_trigger_is_one_shot_and_next_trigger_can_succeed() {
            let mut service = service(128);
            let mut backend = Backend::default();
            let oversized = br#"{"size":129,"resource":"dt/sinara/mpll/ota/u1","sequence":23,"digest":9126140903112366317}"#;

            assert!(matches!(trigger(&mut service, oversized), Handle::Consumed));
            assert_eq!(service.status().state, State::Idle);
            assert_eq!(service.pending, Some(Action::Publish(StatusCode::Oversize)));

            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            assert_eq!(service.status().state, State::Receiving);
            assert_eq!(backend.prepared, Some(8));
        }

        #[test]
        fn ordered_chunks_write_immediately_and_finish() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            assert_eq!(service.state, State::Receiving);

            drive_chunk(&mut service, &mut backend, RESOURCE, "0", &[1, 2, 3, 4]);
            assert_eq!(service.state, State::Receiving);

            drive_chunk(&mut service, &mut backend, RESOURCE, "4", &[5, 6, 7, 8]);
            assert_eq!(service.state, State::Complete);

            assert_eq!(backend.prepared, Some(8));
            assert_eq!(
                backend.writes,
                std::vec![(0, std::vec![1, 2, 3, 4]), (4, std::vec![5, 6, 7, 8])]
            );
            assert_eq!(backend.finished, Some(8));
        }

        #[test]
        fn duplicate_final_chunk_replays_complete_status() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            drive_chunk(&mut service, &mut backend, RESOURCE, "0", &[1, 2, 3, 4]);
            drive_chunk(&mut service, &mut backend, RESOURCE, "4", &[5, 6, 7, 8]);
            service.pending = None;

            assert!(matches!(
                chunk(&mut service, RESOURCE, "4", &[5, 6, 7, 8]),
                Handle::Consumed
            ));
            assert_eq!(service.state, State::Complete);
            assert_eq!(service.pending, Some(Action::Publish(StatusCode::Complete)));
            assert_eq!(backend.writes.len(), 2);
        }

        #[test]
        fn unrelated_chunks_do_not_disrupt_active_transfer() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            service.pending = None;

            assert!(matches!(
                service.handle_publish_with_properties(
                    service.chunk_topic().as_str(),
                    &[1, 2, 3, 4],
                    &Properties::from_slice(&[]),
                ),
                Handle::Consumed
            ));
            assert_eq!(service.state, State::Receiving);
            assert_eq!(service.pending, None);

            assert!(matches!(
                chunk(&mut service, b"another-transfer", "0", &[1, 2, 3, 4]),
                Handle::Consumed
            ));
            assert_eq!(service.state, State::Receiving);
            assert_eq!(service.pending, None);
        }

        #[test]
        fn future_offset_is_rejected_without_write() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            assert!(matches!(
                chunk(&mut service, RESOURCE, "4", &[5, 6, 7, 8]),
                Handle::Consumed
            ));

            assert!(backend.writes.is_empty());
            assert_eq!(backend.finished, None);
        }

        #[test]
        fn overlapping_trigger_does_not_disrupt_active_transfer() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            service.pending = None;
            assert!(matches!(
                trigger(&mut service, JSON_TRIGGER_6),
                Handle::Consumed
            ));

            assert_eq!(backend.prepared, Some(8));
            assert_eq!(service.state, State::Receiving);
            assert_eq!(service.pending, None);
            let update = service.update.unwrap();
            assert_eq!(update.size, 8);
            assert_eq!(update.next_offset, 0);
        }

        #[test]
        fn duplicate_trigger_replays_active_status() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            service.pending = None;

            assert!(matches!(
                trigger(&mut service, JSON_TRIGGER_8),
                Handle::Consumed
            ));
            assert_eq!(service.state, State::Receiving);
            assert_eq!(service.pending, Some(Action::Publish(StatusCode::Accepted)));
            assert_eq!(backend.prepared, Some(8));
        }

        #[test]
        fn final_chunk_may_be_erased_value_padded() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_6);
            drive_chunk(&mut service, &mut backend, RESOURCE, "0", &[1, 2, 3, 4]);
            drive_chunk(
                &mut service,
                &mut backend,
                RESOURCE,
                "4",
                &[5, 6, 0xff, 0xff],
            );
            assert_eq!(service.state, State::Complete);

            assert_eq!(
                backend.writes,
                std::vec![(0, std::vec![1, 2, 3, 4]), (4, std::vec![5, 6, 0xff, 0xff])]
            );
            assert_eq!(backend.finished, Some(6));
        }

        #[test]
        fn final_chunk_rejects_non_erased_padding() {
            let mut service = service(128);
            let mut backend = Backend::default();
            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_6);
            drive_chunk(&mut service, &mut backend, RESOURCE, "0", &[1, 2, 3, 4]);
            assert!(matches!(
                chunk(&mut service, RESOURCE, "4", &[5, 6, 0, 0]),
                Handle::Consumed
            ));

            assert_eq!(backend.finished, None);
        }

        #[test]
        fn backend_failure_marks_transfer_error() {
            let mut service = service(128);
            let mut backend = Backend {
                fail_finish: true,
                ..Backend::default()
            };

            drive_trigger(&mut service, &mut backend, JSON_TRIGGER_8);
            drive_chunk(&mut service, &mut backend, RESOURCE, "0", &[1, 2, 3, 4]);
            drive_chunk(&mut service, &mut backend, RESOURCE, "4", &[5, 6, 7, 8]);
            assert_eq!(service.state, State::Error);
            assert_eq!(service.pending, Some(Action::Publish(StatusCode::Backend)));
        }
    }
}
