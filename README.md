# Telemetry Monitor

Core Java service for concurrent TCP telemetry ingestion, source monitoring and
graceful shutdown.

The service is being built as a long-running process that accepts telemetry from
multiple TCP clients, validates it, and processes it through a bounded
producer/consumer pipeline with explicitly limited resources.

> **Project status:** early development. This repository currently contains the
> build baseline only — the Java toolchain, dependency set, logging
> configuration and continuous integration. The TCP listener, protocol handling
> and source monitoring described above are not implemented yet.

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

## Technology

| Concern | Choice |
|---|---|
| Language / runtime | Java 25 |
| Build | Maven (via Maven Wrapper) |
| JSON | Jackson |
| Logging | SLF4J with Logback, to stdout |
| Testing | JUnit 5, Awaitility |
