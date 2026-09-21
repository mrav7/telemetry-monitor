# Telemetry Monitor

Core Java service for concurrent TCP telemetry ingestion, source monitoring and
graceful shutdown.

The service is being built as a long-running process that accepts telemetry from
multiple TCP clients, validates it, and processes it through a bounded
producer/consumer pipeline with explicitly limited resources.

> **Project status:** early development. The build baseline, the operational
> configuration and the protocol layer — byte-level framing, strict UTF-8
> decoding, NDJSON parsing and message validation — are implemented and tested.
> The TCP listener, the processing pipeline and source monitoring are not
> implemented yet, so the service does not accept connections.

## Requirements

- **Java 25** (the build targets release 25)
- No Maven installation — the repository ships the Maven Wrapper

Maven itself is downloaded and pinned by the wrapper on first use, so local and
CI builds resolve the same Maven version.

## Build

```bash
./mvnw package
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

Several of these settings describe components that are not implemented yet; they
are validated now so that configuration stays a single, stable contract.

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

## Technology

| Concern | Choice |
|---|---|
| Language / runtime | Java 25 |
| Build | Maven (via Maven Wrapper) |
| JSON | Jackson |
| Logging | SLF4J with Logback, to stdout |
| Testing | JUnit 5, Awaitility |
