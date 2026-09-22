#!/usr/bin/env bash
#
# Container smoke test for the Telemetry Monitor image.
#
# Starts the image, sends real telemetry to it with the packaged simulator
# through a published loopback port, stops it with `docker stop` and checks
# that the graceful-shutdown sequence ran to completion.
#
# Usage:
#   scripts/container-smoke.sh [image]        (default: telemetry-monitor:local)
#
# Prerequisites:
#   - the image has been built, for example: docker build -t telemetry-monitor:local .
#   - the host package exists: ./mvnw package
#   - Java 25 for the simulator, taken from JAVA_HOME if set, otherwise from PATH
#
# The script only stops and removes the container it created. Every wait is
# bounded, and a failure prints the container logs before cleaning up.

set -euo pipefail

readonly image="${1:-telemetry-monitor:local}"
readonly container="telemetry-monitor-smoke-$$-${RANDOM}"
readonly source_id="docker-source"
readonly max_connections=17
readonly shutdown_grace_seconds=5
readonly stop_timeout_seconds=15
readonly ready_timeout_seconds=20
readonly processed_timeout_seconds=10
readonly simulator_timeout_seconds=30

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repo_root
readonly app_jar="${repo_root}/target/telemetry-monitor-0.1.0-SNAPSHOT.jar"
readonly app_lib="${repo_root}/target/lib"
readonly java_bin="${JAVA_HOME:+${JAVA_HOME}/bin/}java"

container_owned=false
container_removed=false

log() {
    printf '[container-smoke] %s\n' "$*"
}

fail() {
    printf '[container-smoke] FAIL: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    local status=$?
    if [[ "${container_owned}" == true && "${container_removed}" == false ]]; then
        if (( status != 0 )); then
            printf '[container-smoke] --- logs of %s ---\n' "${container}" >&2
            docker logs "${container}" >&2 2>&1 || true
            printf '[container-smoke] --- end of logs ---\n' >&2
        fi
        docker rm --force "${container}" >/dev/null 2>&1 || true
    fi
    exit "${status}"
}
trap cleanup EXIT

container_logs() {
    docker logs "${container}" 2>&1
}

container_running() {
    [[ "$(docker inspect --format '{{.State.Running}}' "${container}")" == true ]]
}

# Polls the container logs for an extended regular expression until the
# deadline passes. Fails early if the container stops.
wait_for_log() {
    local pattern="$1" timeout_seconds="$2" description="$3"
    local deadline=$((SECONDS + timeout_seconds))
    until grep -Eq -- "${pattern}" <<<"$(container_logs)"; do
        container_running || fail "container stopped while waiting for ${description}"
        (( SECONDS < deadline )) || fail "timed out after ${timeout_seconds}s waiting for ${description}"
        sleep 0.5
    done
}

# --- Prerequisites ----------------------------------------------------------

docker image inspect "${image}" >/dev/null 2>&1 \
    || fail "image ${image} not found; build it first: docker build -t ${image} ."
[[ -f "${app_jar}" && -d "${app_lib}" ]] \
    || fail "packaged simulator not found under target/; run ./mvnw package first"
if docker container inspect "${container}" >/dev/null 2>&1; then
    fail "a container named ${container} already exists; refusing to touch it"
fi

# --- Runtime Java version ---------------------------------------------------

java_version="$(docker run --rm --entrypoint java "${image}" -version 2>&1)"
grep -Eq 'version "25[."]' <<<"${java_version}" \
    || fail "image does not run Java 25: ${java_version}"
log "image Java: $(head -n 1 <<<"${java_version}")"

# --- Start the monitor --------------------------------------------------------

# The name was checked to be free above, so from here on it belongs to this run.
container_owned=true
docker run --detach \
    --name "${container}" \
    --env TM_BIND_ADDRESS=0.0.0.0 \
    --env TM_PORT=9100 \
    --env TM_LOG_LEVEL=INFO \
    --env TM_MAX_CONNECTIONS="${max_connections}" \
    --env TM_SHUTDOWN_GRACE_SECONDS="${shutdown_grace_seconds}" \
    --publish 127.0.0.1::9100 \
    "${image}" >/dev/null
log "started container ${container}"

host_port="$(docker port "${container}" 9100/tcp)"
host_port="${host_port%%$'\n'*}"
host_port="${host_port##*:}"
[[ "${host_port}" =~ ^[0-9]+$ ]] || fail "could not determine the published host port"
log "container port 9100 published on 127.0.0.1:${host_port}"

wait_for_log 'event=server_listening' "${ready_timeout_seconds}" "server_listening"
listening="$(grep -m 1 -E 'event=server_listening' <<<"$(container_logs)")"
log "ready: ${listening#* - }"
for expected in "bind_address=0.0.0.0" "port=9100" "max_connections=${max_connections}"; do
    grep -q -- "${expected}" <<<"${listening}" || fail "startup log does not show ${expected}"
done

# Java must be PID 1 itself, with no shell between Docker and the JVM.
pid1="$(docker exec "${container}" cat /proc/1/cmdline | tr '\0' ' ')"
[[ "${pid1}" == java\ * ]] || fail "PID 1 is not the JVM: ${pid1}"
log "PID 1: ${pid1}"

# --- Real telemetry through the published port -------------------------------

simulator_status=0
timeout "${simulator_timeout_seconds}" "${java_bin}" \
    -cp "${app_jar}:${app_lib}/*" \
    io.github.mrav7.telemetrymonitor.simulator.TelemetrySimulatorApplication \
    --host 127.0.0.1 --port "${host_port}" \
    --source-id "${source_id}" --mode disconnect --count 3 \
    || simulator_status=$?
(( simulator_status == 0 )) || fail "simulator exited with status ${simulator_status}"
log "simulator exited with status 0"

wait_for_log "event=source_first_seen source_id=${source_id} " \
    "${processed_timeout_seconds}" "source_first_seen for ${source_id}"
log "monitor admitted source ${source_id}"
container_running || fail "container is not running after telemetry"

# --- Graceful stop -----------------------------------------------------------

log "docker stop --timeout ${stop_timeout_seconds} (application grace ${shutdown_grace_seconds}s)"
stop_started=${SECONDS}
docker stop --timeout "${stop_timeout_seconds}" "${container}" >/dev/null
stop_elapsed=$((SECONDS - stop_started))

exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${container}")"
oom_killed="$(docker inspect --format '{{.State.OOMKilled}}' "${container}")"
log "container stopped after ~${stop_elapsed}s: exit code ${exit_code}, OOMKilled=${oom_killed}"

final_logs="$(container_logs)"
grep -q 'event=shutdown_requested' <<<"${final_logs}" || fail "shutdown_requested not logged"
completed="$(grep -m 1 -E 'event=shutdown_completed' <<<"${final_logs}")" \
    || fail "shutdown_completed not logged"
log "shutdown: ${completed#* - }"
# 143 is the JVM's status when SIGTERM ends it; 137 would mean Docker had to SIGKILL it.
[[ "${exit_code}" == 0 || "${exit_code}" == 143 ]] \
    || fail "unexpected exit code ${exit_code} (137 means the container was killed)"
(( stop_elapsed < stop_timeout_seconds )) \
    || fail "stop took ${stop_elapsed}s, reaching the ${stop_timeout_seconds}s stop timeout"

log "--- logs of ${container} ---"
printf '%s\n' "${final_logs}"
log "--- end of logs ---"

docker rm "${container}" >/dev/null
container_removed=true
log "removed container ${container}"

# --- Port release --------------------------------------------------------------

if timeout 5 bash -c "exec 3<>/dev/tcp/127.0.0.1/${host_port}" 2>/dev/null; then
    fail "127.0.0.1:${host_port} still accepts connections after the container stopped"
fi
if command -v ss >/dev/null && [[ -n "$(ss -H -ltn "sport = :${host_port}")" ]]; then
    fail "a listener is still bound to port ${host_port}"
fi
log "127.0.0.1:${host_port} no longer accepts connections"

# --- Invalid configuration ---------------------------------------------------

invalid_status=0
invalid_output="$(timeout 30 docker run --rm --name "${container}-invalid-config" \
    --env TM_PORT=invalid "${image}" 2>&1)" \
    || invalid_status=$?
(( invalid_status == 1 )) || fail "invalid TM_PORT exited with status ${invalid_status}, expected 1"
grep -q 'TM_PORT' <<<"${invalid_output}" \
    || fail "invalid TM_PORT diagnostic does not name the variable: ${invalid_output}"
log "invalid TM_PORT rejected: ${invalid_output}"

log "PASS"
