# Telemetry Monitor container image.
#
# The builder stage compiles, tests and packages the project from source with
# the Maven Wrapper. The runtime stage keeps only a Java 25 runtime, the
# application JAR and its runtime dependencies.

FROM eclipse-temurin:25-jdk-noble AS build

# The Maven Wrapper downloads the pinned .zip distribution and verifies it
# against distributionSha256Sum. Without unzip it would fetch the .tar.gz
# variant instead, which that checksum does not describe, so the build fails.
# unzip is needed only here; the runtime stage installs nothing.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY mvnw pom.xml ./
COPY .mvn/ .mvn/
COPY src/ src/

RUN ./mvnw --batch-mode --no-transfer-progress package


FROM eclipse-temurin:25-jre-noble

# Dedicated unprivileged account. The application files stay owned by root, so
# the service can read them but not modify them. The service keeps no state on
# disk, so it needs no writable directory.
RUN groupadd --system telemetry \
    && useradd --system --gid telemetry --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin telemetry

WORKDIR /app

COPY --from=build /build/target/telemetry-monitor-0.1.0-SNAPSHOT.jar /app/telemetry-monitor.jar
COPY --from=build /build/target/lib/ /app/lib/

USER telemetry

# Documents the default TM_PORT. Publishing it is up to `docker run -p`, and
# the listener must be bound to TM_BIND_ADDRESS=0.0.0.0 to be reachable that way.
EXPOSE 9100

# Exec form: Java runs as PID 1, with no shell in between, so the SIGTERM sent
# by `docker stop` reaches the JVM and runs the graceful-shutdown hook.
ENTRYPOINT ["java", "-cp", "/app/telemetry-monitor.jar:/app/lib/*", "io.github.mrav7.telemetrymonitor.TelemetryMonitorApplication"]
