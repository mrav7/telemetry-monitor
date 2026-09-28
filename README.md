# Telemetry Monitor

[![CI](https://github.com/mrav7/telemetry-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/mrav7/telemetry-monitor/actions/workflows/ci.yml)

A Core Java service for concurrent TCP telemetry ingestion, bounded event processing, source-state monitoring, and coordinated graceful shutdown.

Built as a technical portfolio project, it explores long-running service design under explicit resource limits: virtual-thread connection handling, bounded queues and backpressure, concurrent state updates, failure isolation, and deterministic shutdown behavior.

## What this project demonstrates

- **Concurrent network I/O:** each active TCP connection is isolated on its own virtual thread.
- **Bounded processing:** validated events enter a bounded producer/consumer pipeline drained by a fixed worker pool.
- **Backpressure:** full queues slow producers instead of allowing unbounded memory growth.
- **Failure isolation:** malformed input, disconnects, oversized frames, and processing failures are contained to the smallest safe scope.
- **Concurrent source-state management:** source liveness is tracked independently from connection state and remains correct under concurrent processing.
- **Graceful shutdown:** producers, schedulers, queued work, and workers are coordinated under one global shutdown deadline.
- **End-to-end verification:** real TCP tests, traffic simulation, container smoke tests, and GitHub Actions verify behavior beyond unit tests.

## Architecture

```text
Telemetry Simulator (or any TCP client)
                │
                │ TCP / NDJSON
                ▼
        TelemetryServer
   one virtual thread / connection
                │
                ▼
 framing → UTF-8 → JSON → validation
                │
                ▼
         bounded queue
                │
                ▼
       fixed worker pool
                │
                ▼
        SourceRegistry
                ▲
                │
          StaleMonitor

ShutdownCoordinator → one global shutdown deadline
```

The main runtime responsibilities are deliberately separated:

- `TelemetryServer` owns the listening socket, active client sockets, and per-connection virtual threads.
- `ProcessingPipeline` owns the bounded queue and fixed processing workers.
- `SourceRegistry` is the single place where source state is updated.
- `StaleMonitor` periodically moves silent sources from `ONLINE` to `STALE`.
- `ShutdownCoordinator` stops the service in a fixed order under one shared deadline.

## Design overview

### Explicit resource bounds

Every resource that external input can grow has an explicit limit:

`connections · message size · queued events · tracked sources`

These limits are part of the runtime model rather than informal operational assumptions.

### Backpressure

Validated events are inserted into a bounded queue. When that queue is full, the producing connection waits for capacity instead of allowing memory usage to grow without bound.

This is flow control, not durability: accepted events remain in memory and may still be lost if the process stops abruptly or the shutdown deadline expires.

### Failure isolation

Malformed messages, connection failures, refused connections, and processing failures are isolated whenever protocol framing still allows the service to continue safely.

Oversized messages are handled differently: the connection is closed because the byte stream can no longer be reliably realigned to a message boundary.

## Source monitoring

Sources are tracked independently of TCP connections:

| State | Meaning |
|---|---|
| `ONLINE` | A known source with recently accepted telemetry |
| `STALE` | No accepted telemetry within the configured staleness threshold |

Freshness is based on the monitor's own reception time rather than the timestamp declared by the remote source.

An open TCP connection does not keep a source `ONLINE`, and a source may send telemetry through different connections over time.

Source state is held in memory and is not persisted across process restarts.

## Graceful shutdown

`SIGTERM` and normal JVM shutdown trigger the same coordinated, idempotent shutdown path.

The service:

1. stops accepting new work and interrupts connection producers;
2. stops the staleness scheduler once producers can no longer enqueue;
3. drains accepted work while time remains;
4. stops workers and releases owned resources.

All phases share one monotonic deadline configured by `TM_SHUTDOWN_GRACE_SECONDS`; the timeout is not reset between shutdown stages.

## Tech stack

**Java 25 · Maven · Jackson · SLF4J / Logback · JUnit · Awaitility · Docker · GitHub Actions**

## Quick start

### Requirements

- Java 25
- Git
- no global Maven installation required; the repository includes the Maven Wrapper

Build the project:

```bash
./mvnw package
```

Start the monitor:

```bash
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.TelemetryMonitorApplication
```

The default listener address is `127.0.0.1:9100`.

Send one telemetry message with any TCP client, for example:

```bash
printf '{"version":1,"sourceId":"source-01","occurredAt":"2026-09-21T18:15:42.123Z","metric":"temperature","value":18.72}\n' \
  | ncat --send-only 127.0.0.1 9100
```

Source-state changes and operational events are written to standard output.

## Traffic simulator

The packaged build also includes a companion simulator for repeatable TCP scenarios:

| Mode | Scenario |
|---|---|
| `normal` | Periodic valid telemetry |
| `burst` | Valid events without deliberate pacing |
| `malformed` | Deliberately invalid input |
| `silent` | A connected source that stops sending |
| `disconnect` | Valid traffic followed by a normal close |

Example:

```bash
java -cp 'target/telemetry-monitor-0.1.0-SNAPSHOT.jar:target/lib/*' \
  io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
  --source-id source-01 \
  --mode disconnect
```

The simulator reports whether its own client-side scenario completed successfully. Protocol v1 does not provide acknowledgements or persistence.

## Protocol

Telemetry uses UTF-8 newline-delimited JSON over TCP.

```json
{
  "version": 1,
  "sourceId": "source-01",
  "occurredAt": "2026-09-21T18:15:42.123Z",
  "metric": "temperature",
  "value": 18.72
}
```

Protocol v1 uses strict validation:

- all five fields are required;
- unknown or repeated properties are rejected;
- explicit `null` values and incorrect JSON types are rejected;
- `version` must be the integer `1`;
- `sourceId` and `metric` use a restricted identifier format;
- `value` must be finite;
- messages are bounded by `TM_MAX_MESSAGE_BYTES`.

The monitor records its own reception timestamp for source-state decisions. `occurredAt` is treated as source-provided data, not as a liveness signal.

## Configuration

Configuration is read once at startup from environment variables.

| Variable | Default | Purpose |
|---|---:|---|
| `TM_BIND_ADDRESS` | `127.0.0.1` | Listener address |
| `TM_PORT` | `9100` | TCP listener port |
| `TM_MAX_CONNECTIONS` | `256` | Maximum active connections |
| `TM_WORKER_THREADS` | `4` | Fixed processing worker count |
| `TM_QUEUE_CAPACITY` | `1024` | Bounded event queue capacity |
| `TM_MAX_MESSAGE_BYTES` | `8192` | Maximum message size |
| `TM_MAX_SOURCES` | `10000` | Maximum tracked sources |
| `TM_STALE_AFTER_SECONDS` | `30` | Silence threshold before `STALE` |
| `TM_STALE_CHECK_INTERVAL_SECONDS` | `5` | Source-state check interval |
| `TM_SHUTDOWN_GRACE_SECONDS` | `10` | Global shutdown deadline |
| `TM_LOG_LEVEL` | `INFO` | Runtime log level |

Invalid configured values stop startup with a diagnostic rather than being silently clamped or replaced.

## Testing and CI

Run the test suite:

```bash
./mvnw test
```

Run the same Maven verification lifecycle used by CI:

```bash
./mvnw verify
```

Automated tests include real TCP end-to-end scenarios covering:

- `ONLINE → STALE → ONLINE` source-state behavior;
- multiple simulators and connections;
- bounded backpressure;
- malformed-input isolation;
- disconnect isolation;
- shutdown behavior.

GitHub Actions runs two independent checks:

- **Build and test:** compiles the project and runs the full Maven verification lifecycle.
- **Container smoke:** builds the Docker image and executes the container smoke test.

## Docker

Build the container image:

```bash
docker build -t telemetry-monitor:local .
```

Run the service with the container port published only on the host loopback interface:

```bash
docker run --rm \
  --name telemetry-monitor \
  -e TM_BIND_ADDRESS=0.0.0.0 \
  -p 127.0.0.1:9100:9100 \
  telemetry-monitor:local
```

The image uses a multi-stage build, contains the runtime application rather than build tooling, and runs the monitor as an unprivileged user.

A container smoke test verifies startup, configuration propagation, TCP ingestion, signal handling, graceful shutdown, host-port release, and invalid-configuration behavior.

## Limitations

Telemetry Monitor is intended for development, testing, and controlled environments. It is not hardened for exposure to untrusted networks.

The current implementation intentionally does not provide:

- TLS, authentication, or authorization;
- persistent telemetry or persistent source state;
- delivery acknowledgements, retries, or at-least-once / exactly-once guarantees;
- deduplication or global event ordering;
- an HTTP API or source-state export;
- published throughput or latency guarantees.

Configured resource limits bound process growth; they are not access controls or performance claims.