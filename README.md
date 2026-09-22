# Telemetry Monitor

Core Java service for concurrent TCP telemetry ingestion, source monitoring and
graceful shutdown.

The service is being built as a long-running process that accepts telemetry from
multiple TCP clients, validates it, and processes it through a bounded
producer/consumer pipeline with explicitly limited resources.

> **Project status:** The build baseline, the operational
> configuration, the protocol layer, the ingestion pipeline — a concurrent TCP
> listener, byte-level framing, strict UTF-8 decoding, NDJSON parsing, message
> validation, a bounded queue and a fixed set of processing workers — and
> source monitoring, which tracks every source as `ONLINE` or `STALE`, and
> coordinated graceful shutdown are implemented and tested. A TCP telemetry
> simulator core now provides normal, burst, malformed, silent and disconnect
> traffic modes. Automated simulator-to-monitor tests cover ONLINE, STALE,
> recovery, multiple clients, bounded backpressure, malformed input isolation
> and disconnect isolation. The packaged monitor and simulator can be run as
> separate JVM processes over TCP, and the monitor can be built and run as a
> Docker container.

## Requirements

- **Java 25** (the build targets release 25)
- No Maven installation — the repository ships the Maven Wrapper

Maven itself is downloaded and pinned by the wrapper on first use, so local and
CI builds resolve the same Maven version.

## Build

```bash
./mvnw package
```

This produces `target/telemetry-monitor-0.1.0-SNAPSHOT.jar` and copies its
runtime dependencies to `target/lib/`. No separate Maven classpath-generation
step is needed to run either application.

## Run the monitor

Start the service from the packaged build:

```bash
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.TelemetryMonitorApplication
```

The service reads its settings from the environment, binds the configured
address and port, and then serves until the process is terminated:

```bash
TM_BIND_ADDRESS=127.0.0.1 TM_PORT=9100 TM_LOG_LEVEL=DEBUG \
  java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
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

Source state changes are reported at `INFO`; with `TM_LOG_LEVEL=DEBUG` each
connection is reported as well:

```text
event=server_listening bind_address=127.0.0.1 port=9100 max_connections=256
event=connection_accepted connection_id=1 remote=/127.0.0.1:37786
event=source_first_seen source_id=source-01 first_accepted_at=... known_sources=1
event=source_stale source_id=source-01 last_accepted_at=... silent_seconds=30
event=source_online source_id=source-01 last_accepted_at=...
event=message_rejected connection_id=1 remote=/127.0.0.1:37786 reason=invalid_json
```

Individual accepted messages are not logged: under load that would produce one
record per message and drown everything else.

To check that the listener is open:

```bash
ss -ltn 'sport = :9100'
```

## Run the simulator

The simulator is the second application in the packaged build. It connects to
the monitor over TCP and requires `--source-id` and `--mode`:

```bash
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id source-01 --mode disconnect
```

Common defaults are `--host 127.0.0.1`, `--port 9100`, `--metric temperature`,
and `--value 20.0`. `normal` defaults to one event per second for 30 seconds;
`burst` defaults to 1000 events; `silent` defaults to 40 seconds; and
`disconnect` defaults to three events. A simulator exits with `0` when its
configured client-side scenario completes successfully, `1` for invalid CLI
configuration, and `2` when it cannot execute the scenario. Success does not
mean the monitor acknowledged, accepted, or processed an event: protocol v1
has no acknowledgement or persistence.

### Simulator modes

| Mode | Purpose | Relevant controls |
|---|---|---|
| `normal` | Periodic valid telemetry | `--rate`, `--duration-seconds` |
| `burst` | Valid events without deliberate pacing | `--count` |
| `malformed` | One deliberately invalid frame | `--case` |
| `silent` | One valid event, then an open silent TCP connection | `--duration-seconds` |
| `disconnect` | Valid events, then a normal close | `--count` |

Malformed cases are `broken-json`, `missing-field`, `unsupported-version`,
`invalid-source-id`, `invalid-timestamp`, and `invalid-value`.

### Simulator examples

Run these after `./mvnw package` while a monitor is listening on
`127.0.0.1:9100`:

```bash
# Periodic valid telemetry.
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id normal-01 --mode normal --rate 5 --duration-seconds 2

# A valid burst without deliberate pacing.
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id burst-01 --mode burst --count 100

# One deliberately invalid frame.
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id malformed-01 --mode malformed --case invalid-value

# Establish a source, then keep its connection open without telemetry.
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id silent-01 --mode silent --duration-seconds 40

# Send valid telemetry and close normally.
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id disconnect-01 --mode disconnect --count 3
```

## End-to-end flow

```text
Telemetry Simulator
        ↓ TCP
Telemetry Server
        ↓
framing / UTF-8 / JSON validation
        ↓
bounded queue
        ↓
fixed workers
        ↓
Source Registry
        ↑
scheduled stale detection
```

Automated end-to-end verification covers `ONLINE`, `STALE` while a connection
remains active, recovery, multiple simulators, bounded backpressure, malformed
input isolation, and disconnect isolation.

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
timestamp, not `occurredAt`, is what source monitoring is based on, because the
clock of a remote process is not a reliable liveness signal.

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
      ↓
source state (TM_MAX_SOURCES)  ←  scheduled staleness check
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

**Source state.** Workers are what apply an accepted message to the state of its
source; connections never touch it. That keeps the decision of what a source's
state is in one place, whatever order the workers happen to run in.

**Failure isolation.** An invalid message affects only that message. A
disconnect, a connection error, an oversized message or a refused connection
affects only the connection it belongs to; the listener and the workers keep
running. A message from a source that cannot be admitted is refused on its own
and costs neither the worker nor the connection. An unexpected exception while
applying one event is logged and isolated to that event; the worker remains
available for later work.

## Source state

The monitor keeps the current operational state of every source it has accepted
a message from, in memory, keyed by `sourceId`. A source is in one of two
states:

| State | Meaning |
|---|---|
| `ONLINE` | A known source that has not been marked `STALE`. Accepted telemetry creates this state and restores it. |
| `STALE` | A scheduled check found that nothing had been accepted from this source for at least `TM_STALE_AFTER_SECONDS`. |

There is no third state. A source the monitor has never accepted a message from
is simply not tracked.

**How state moves.** The first accepted message admits the source as `ONLINE`.
Every further accepted message keeps it `ONLINE` and, if it was `STALE`, brings
it back. In the other direction, a check runs every
`TM_STALE_CHECK_INTERVAL_SECONDS` and marks as `STALE` every source whose last
accepted message is at least `TM_STALE_AFTER_SECONDS` old. Only accepted
telemetry makes a source `ONLINE`, and only that scheduled check makes one
`STALE`.

The state is stored, not recalculated on demand. Between the moment a source
falls silent past the threshold and the next scheduled check, it is still
recorded as `ONLINE`, so the check interval is also the granularity with which
a source is observed to go `STALE`.

**Which clock decides.** Freshness is measured with the monitor's own reception
time, never with the `occurredAt` the message declares. A source whose clock is
wrong, or which backdates its messages, is judged on when its telemetry actually
arrived. Messages that are processed out of order cannot make a source look
older than it is: the newest reception time always wins.

**Per source, the monitor tracks:** its identifier and state, when it was first
accepted, when it was last accepted, how many of its events were accepted, and
how many were processed.

**Connection state is not source state.** A connection that is open but silent
does not keep its source `ONLINE`, and a source going `STALE` never closes a
connection — both situations are normal and can be observed at the same time.
One connection may carry several sources, and a source may arrive over several
connections; nothing binds one to the other. Disconnecting does not remove a
source or change its counters.

**Bound.** At most `TM_MAX_SOURCES` distinct sources are tracked. Once that
limit is reached, messages from sources already known keep being accepted as
usual, while a message from an unknown source is refused with a log record and
changes nothing. Nothing is evicted to make room — not even a `STALE` source,
which keeps its slot — and the refusal affects neither the connection that sent
it nor any other source.

**Not durable.** Source state lives in memory for the lifetime of the process.
It is not persisted, and restarting the monitor starts from no known sources.

## Graceful shutdown

`SIGTERM` and normal JVM shutdown invoke the same coordinated, idempotent
shutdown operation. The first request changes the service from running to
stopping and starts the single monotonic deadline configured by
`TM_SHUTDOWN_GRACE_SECONDS`; repeated or concurrent requests join that same
operation and do not restart its budget.

Shutdown proceeds in this order:

1. The listener and active client sockets are closed, connection tasks are
   interrupted, and all connection producers are awaited while processing
   workers remain active. A producer blocked by queue backpressure exits on
   interruption and does not retry its enqueue.
2. Once no producer can enqueue again, the stale-source scheduler stops.
3. Workers drain accepted work until both the queue is empty and no event is
   still being processed.
4. Workers stop and all owned sockets and executors are released.

Every phase receives only the time still remaining from the original global
deadline. If it expires, shutdown interrupts the remaining work, may discard
queued events, logs the affected phase and continues terminating. Pending work
and source state are in-memory and non-durable: graceful shutdown is a bounded
best effort, not an at-least-once or exactly-once delivery guarantee.

Operational shutdown progress is logged with events such as
`shutdown_requested`, `shutdown_producers_stopped`,
`shutdown_drain_completed`, and `shutdown_completed`. A deadline expiry or
forced stop is logged at `WARN` with queue and in-flight context where useful.

## Docker

The repository's `Dockerfile` builds the monitor from source in two stages. The
first stage compiles, tests and packages the project with the Maven Wrapper on
a Java 25 JDK. The final image is an Eclipse Temurin Java 25 JRE image (Ubuntu
based) with only the application JAR and its runtime dependencies added. It
contains no build tools, sources or tests, and runs the monitor as an
unprivileged user.

Build the image from the repository root:

```bash
docker build -t telemetry-monitor:local .
```

The build runs the full test suite and fails if any test fails.

Run the monitor with its port published on the host's loopback interface:

```bash
docker run --rm \
  --name telemetry-monitor \
  -e TM_BIND_ADDRESS=0.0.0.0 \
  -p 127.0.0.1:9100:9100 \
  telemetry-monitor:local
```

`TM_BIND_ADDRESS=0.0.0.0` is required here. The service's default,
`127.0.0.1`, is the container's own loopback interface, and traffic arriving
through a published port never reaches it. The image does not change that
default: listening more widely is an explicit choice made when the container is
started. `-p 127.0.0.1:9100:9100` then publishes the port on the host's
loopback interface only. The image declares `EXPOSE 9100` to document the
default port, but that does not publish anything by itself.

The container is configured with the same `TM_*` environment variables as a
host run (see [Configuration](#configuration)); there are no Docker-specific
settings. If `TM_PORT` is changed, publish that container port instead.

```bash
docker run --rm \
  --name telemetry-monitor \
  -e TM_BIND_ADDRESS=0.0.0.0 \
  -e TM_MAX_CONNECTIONS=64 \
  -e TM_LOG_LEVEL=DEBUG \
  -p 127.0.0.1:9100:9100 \
  telemetry-monitor:local
```

Logs go to standard output and no log file is written inside the container.
Read them with:

```bash
docker logs telemetry-monitor
```

The image defines no `HEALTHCHECK`, and the service has no HTTP endpoint.
Readiness shows up in the logs as `event=server_listening`.

### Simulator against the container

The packaged simulator on the host reaches the container through the published
port:

```bash
./mvnw package
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --host 127.0.0.1 --port 9100 \
  --source-id docker-source --mode disconnect --count 3
```

`docker logs telemetry-monitor` then shows
`event=source_first_seen source_id=docker-source`. As in a host run, the
simulator's exit status only reports whether its own scenario completed; the
monitor does not acknowledge telemetry.

### Stopping the container

```bash
docker stop --timeout 15 telemetry-monitor
```

`docker stop` sends `SIGTERM`. The image starts Java directly, with no shell in
between, so the JVM is the container's main process. The signal reaches it and
runs the same [graceful shutdown](#graceful-shutdown) as on a host, logged as
`shutdown_requested` … `shutdown_completed`. After that the container exits
with status 143, meaning it was ended by `SIGTERM`.

`docker stop` waits 10 seconds by default before sending `SIGKILL`, which is
the same as the default `TM_SHUTDOWN_GRACE_SECONDS`. A stop timeout longer than
the configured grace, as above, lets a shutdown that uses its whole budget
finish first. A container that is killed (exit status 137) skips whatever
shutdown steps remain.

### Container smoke test

`scripts/container-smoke.sh` checks a built image end to end. It needs the
host package, for the simulator, and Java 25 on the host (`JAVA_HOME` or
`PATH`):

```bash
./mvnw package
docker build -t telemetry-monitor:local .
scripts/container-smoke.sh telemetry-monitor:local
```

The script starts a container with explicit `TM_*` settings on a free
loopback host port and waits for `server_listening`, with a time limit. It then
checks that:

- the configured values reached the application;
- Java is the container's PID 1;
- the simulator can send telemetry that the monitor admits;
- `docker stop` completes the graceful shutdown without a `SIGKILL`;
- the host port is released;
- an invalid `TM_PORT` stops startup with a diagnostic.

It stops and removes only the container it created.

## Current limitations

- Source state is in memory only, and is not exposed anywhere but the logs:
  there is no API, no endpoint and no export. Individual telemetry values are
  counted, not stored — no history is kept.
- Sources are never removed, so a process that is sent many distinct source
  identifiers holds the ones it admitted until it stops.
- Graceful shutdown is bounded and non-durable. Work still queued or in flight
  when the global deadline expires may be discarded.
- Protocol v1 has no client acknowledgement.
- The container smoke test is run locally. Continuous integration builds and
  tests the project but does not build the Docker image.

## Technology

| Concern | Choice |
|---|---|
| Language / runtime | Java 25 |
| Build | Maven (via Maven Wrapper) |
| JSON | Jackson |
| Logging | SLF4J with Logback, to stdout |
| Testing | JUnit 6.1.3, Awaitility |
| Container | Docker, Eclipse Temurin 25 base images |
