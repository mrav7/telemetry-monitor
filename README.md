# Telemetry Monitor

Core Java service for concurrent TCP telemetry ingestion, source monitoring and
graceful shutdown.

The service is being built as a long-running process that accepts telemetry from
multiple TCP clients, validates it, and processes it through a bounded
producer/consumer pipeline with explicitly limited resources.

> **Project status:** early development. The build baseline, the operational
> configuration, the protocol layer and the ingestion pipeline — a concurrent
> TCP listener, byte-level framing, strict UTF-8 decoding, NDJSON parsing,
> message validation, a bounded queue and a fixed set of processing workers —
> are implemented and tested. Accepted messages currently reach a minimal
> processing boundary that logs them at `DEBUG` level and nothing more. Source
> monitoring (`ONLINE`/`STALE`) and graceful shutdown are not implemented yet.

## Requirements

- **Java 25** (the build targets release 25)
- No Maven installation — the repository ships the Maven Wrapper

Maven itself is downloaded and pinned by the wrapper on first use, so local and
CI builds resolve the same Maven version.

## Build

```bash
./mvnw package
```

## Run

Build once, then write out the runtime classpath and start the service:

```bash
./mvnw package
./mvnw -q dependency:build-classpath \
  -Dmdep.outputFile=target/classpath.txt -DincludeScope=runtime
java -cp "target/classes:$(cat target/classpath.txt)" \
  io.github.mrav7.telemetrymonitor.TelemetryMonitorApplication
```

The service reads its settings from the environment, binds the configured
address and port, and then serves until the process is terminated:

```bash
TM_BIND_ADDRESS=127.0.0.1 TM_PORT=9100 TM_LOG_LEVEL=DEBUG \
  java -cp "target/classes:$(cat target/classpath.txt)" \
  io.github.mrav7.telemetrymonitor.TelemetryMonitorApplication
```

Startup stops with a non-zero exit status, and says why, when a variable is
unusable or when the listener cannot be opened — for example when the port is
already taken.

Once it is listening, any TCP client can send telemetry. One message per line:

```bash
printf '{"version":1,"sourceId":"source-01","occurredAt":"2026-09-21T18:15:42.123Z","metric":"temperature","value":18.72}\n' \
  | ncat --send-only 127.0.0.1 9100
```

Several messages can be sent over the same connection, and one connection may
carry several sources. An invalid message is rejected on its own and the
connection stays usable; an oversized message ends the connection that sent it,
because the byte stream can no longer be realigned to a message boundary.

With `TM_LOG_LEVEL=DEBUG` the monitor reports each connection and each processed
message:

```text
event=server_listening bind_address=127.0.0.1 port=9100 max_connections=256
event=connection_accepted connection_id=1 remote=/127.0.0.1:37786
event=event_processed source_id=source-01 metric=temperature ...
event=message_rejected connection_id=1 remote=/127.0.0.1:37786 reason=invalid_json
```

To check that the listener is open:

```bash
ss -ltn 'sport = :9100'
```

## Test

```bash
./mvnw test
```

To run the same lifecycle that continuous integration runs:

```bash
./mvnw verify
```

## Configuration

All settings come from environment variables. There is no configuration file.

| Variable | Default | Meaning and accepted values |
|---|---|---|
| `TM_BIND_ADDRESS` | `127.0.0.1` | Address the listener will bind to. A literal IPv4 or IPv6 address; host names are not resolved. Use `0.0.0.0` to accept connections from outside a container. |
| `TM_PORT` | `9100` | TCP port. `1`–`65535`. |
| `TM_MAX_CONNECTIONS` | `256` | Maximum simultaneously active connections. Positive. |
| `TM_WORKER_THREADS` | `4` | Fixed number of processing workers. Positive. |
| `TM_QUEUE_CAPACITY` | `1024` | Capacity of the bounded processing queue. Positive. |
| `TM_MAX_MESSAGE_BYTES` | `8192` | Maximum bytes in one message, excluding the newline. Positive. |
| `TM_MAX_SOURCES` | `10000` | Maximum distinct sources tracked. Positive. |
| `TM_STALE_AFTER_SECONDS` | `30` | Silence after which a source is considered stale. Positive. |
| `TM_STALE_CHECK_INTERVAL_SECONDS` | `5` | How often staleness is evaluated. Positive. |
| `TM_SHUTDOWN_GRACE_SECONDS` | `10` | Global deadline for the whole shutdown sequence. Positive. |
| `TM_LOG_LEVEL` | `INFO` | One of `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, case-insensitive. |

Configuration is validated at startup. A variable that is not set falls back to
its default, but a variable that is set to an unusable value stops startup with
a diagnostic naming the variable and a non-zero exit status. Values are never
clamped, trimmed or silently replaced by the default.

`TM_MAX_SOURCES`, `TM_STALE_AFTER_SECONDS`, `TM_STALE_CHECK_INTERVAL_SECONDS`
and `TM_SHUTDOWN_GRACE_SECONDS` describe components that are not implemented
yet; they are validated now so that configuration stays a single, stable
contract.

## Protocol

Version 1 of the wire protocol is UTF-8 newline-delimited JSON:

- one compact JSON object per message;
- messages are separated by a line feed (`LF`);
- a carriage return immediately before the line feed (`CRLF`) is accepted and
  removed;
- only `version` `1` is accepted, and there is no version negotiation.

### Message format

```json
{"version":1,"sourceId":"source-01","occurredAt":"2026-09-21T18:15:42.123Z","metric":"temperature","value":18.72}
```

All five properties are required:

| Property | Type | Rules |
|---|---|---|
| `version` | number | Must be the integer `1`. |
| `sourceId` | string | Identifier of the sending source: 1–64 characters from `A–Z`, `a–z`, `0–9`, `_`, `.`, `-`. |
| `occurredAt` | string | ISO-8601 instant, as declared by the source. Used as data, never to decide whether a source is alive. |
| `metric` | string | Identifier of the reading, same character rules as `sourceId`. There is no predefined metric catalogue. |
| `value` | number | A finite number. `NaN` and infinities are rejected. |

Validation is strict: unknown properties, repeated properties, explicit nulls and
values of the wrong JSON type are all rejected, and no value is coerced — `"1"`
is not accepted for `version`.

The monitor records its own reception timestamp for each accepted message. That
timestamp, not `occurredAt`, is what source monitoring will be based on, because
the clock of a remote process is not a reliable liveness signal.

### Message size

The maximum message size defaults to 8192 bytes and is set by
`TM_MAX_MESSAGE_BYTES`.

- The limit counts **bytes**, not Java characters, so a message of multi-byte
  characters reaches the limit sooner than its character count suggests.
- The `LF` delimiter does not count toward the limit.
- A `CR` in a `CRLF` ending does count, and is removed only after the size has
  been checked.
- The limit is enforced while the message is being read, so an oversized message
  is detected without being buffered in full.

### Invalid input

Messages are rejected individually. An empty message, invalid UTF-8, malformed
JSON, an unsupported version and any invalid property all cause that one message
to be discarded; a connection carrying them stays usable.

An oversized message is different: because reading stops partway through, the
byte stream can no longer be realigned to a message boundary, so the connection
carrying it cannot be continued.

Bytes left at the end of a stream without a closing newline are discarded
without being decoded.

## Ingestion

```text
TCP connection
      ↓  one virtual thread per connection
framing → UTF-8 → JSON → validation
      ↓
bounded queue (TM_QUEUE_CAPACITY)
      ↓
fixed processing workers (TM_WORKER_THREADS)
```

**Connections.** Every accepted connection is handled on its own virtual thread,
so a client that sends slowly, or stops halfway through a message, does not
delay anyone else. At most `TM_MAX_CONNECTIONS` connections are active at a
time; a connection arriving when that limit is reached is closed immediately,
with a log record, and connections already running are unaffected. A connection
is not a source identity: nothing is remembered about a client between
connections.

**Queue and workers.** Validated messages are handed to a bounded queue of
`TM_QUEUE_CAPACITY` messages, drained by exactly `TM_WORKER_THREADS` workers.
The worker count does not grow with load.

**Backpressure.** When the queue is full, the connection being read waits for
capacity instead of discarding the message. That connection stops consuming its
socket, and TCP then slows the sender down. Valid messages are not dropped.

**Failure isolation.** An invalid message affects only that message. A
disconnect, a connection error, an oversized message or a refused connection
affects only the connection it belongs to; the listener and the workers keep
running.

## Current limitations

- Source state is not implemented: nothing tracks which sources are `ONLINE` or
  `STALE`. Accepted messages reach a minimal processing boundary that logs them
  at `DEBUG` level; they are not stored, counted or aggregated anywhere.
- Shutdown is not graceful yet. The process stops when it is terminated; there
  is no deadline, no queue drain and no guarantee about messages already
  accepted. `TM_SHUTDOWN_GRACE_SECONDS` is validated but not yet applied.
- There is no simulator, no container image and no acknowledgement to clients.

## Technology

| Concern | Choice |
|---|---|
| Language / runtime | Java 25 |
| Build | Maven (via Maven Wrapper) |
| JSON | Jackson |
| Logging | SLF4J with Logback, to stdout |
| Testing | JUnit 5, Awaitility |
