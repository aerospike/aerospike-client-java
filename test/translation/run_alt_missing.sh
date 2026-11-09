#!/usr/bin/env bash
#
# Missing alternate-access-address (useServicesAlternate) integration cases.
#
# Starts two two-node Aerospike Enterprise clusters in Docker, runs SuiteAlternateMissing once
# against them and removes both clusters on exit, including on failure.
#   missing cluster: node A advertises alternate-access-address 127.0.0.1 and its own published host
#                    port, node B has no alternate access address.
#   control cluster: node C and node D both advertise 127.0.0.1 and their own published host port.
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   ALT_MISSING_PORT          host port published for node A; B, C and D use the next three (default 3630)
#   TRANSLATION_SKIP_BUILD    set to 1 to skip installing the client module first
#   TRANSLATION_MAVEN_OFFLINE set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/translation/run_alt_missing.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/translation-alt-missing"
PORT_A="${ALT_MISSING_PORT:-3630}"
PORT_B=$((PORT_A + 1))
PORT_C=$((PORT_A + 2))
PORT_D=$((PORT_A + 3))
NODE_A=A1
NODE_B=B2
NODE_C=C3
NODE_D=D4
PREFIX="translation-alt-missing-$$"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
[ -n "${AEROSPIKE_FEATURES_FILE:-}" ] || die "AEROSPIKE_FEATURES_FILE is not set"
[ -r "$AEROSPIKE_FEATURES_FILE" ] || die "AEROSPIKE_FEATURES_FILE '$AEROSPIKE_FEATURES_FILE' is not readable"

MVN=(mvn)
[ "${TRANSLATION_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

STARTED=()
STARTED_NETWORKS=()

cleanup() {
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for n in "${STARTED_NETWORKS[@]+"${STARTED_NETWORKS[@]}"}"; do
		docker network rm "$n" >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ "${TRANSLATION_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
FEATURES="$(cd "$(dirname "$AEROSPIKE_FEATURES_FILE")" && pwd)/$(basename "$AEROSPIKE_FEATURES_FILE")"

# An empty alt_port leaves the node without an alternate access address.
write_conf() {
	local conf="$1" node_id="$2" alt_port="$3" mesh_seed="$4"
	local alternate=""

	if [ -n "$alt_port" ]; then
		alternate="        alternate-access-address 127.0.0.1
        alternate-access-port $alt_port"
	fi
	cat > "$WORK_DIR/$conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name docker
    proto-fd-max 15000
    node-id $node_id
}

logging {
    console {
        context any info
    }
}

network {
    service {
        address any
        port 3000
$alternate
    }
    heartbeat {
        mode mesh
        port 3002
        mesh-seed-address-port $mesh_seed 3002
    }
    fabric {
        port 3001
    }
}

namespace test {
    replication-factor 1
    storage-engine memory {
        data-size 512M
    }
}
EOF
}

wait_cluster_size() {
	local name="$1" size="$2"
	local i

	for i in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "CLUSTER-SIZE $size" >/dev/null; then
			sleep 2
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name did not reach cluster size $size after 60s"
}

start_cluster() {
	local network="$1"
	shift
	local spec container host_port conf

	docker network create "$network" >/dev/null
	STARTED_NETWORKS+=("$network")

	for spec in "$@"; do
		IFS='|' read -r container host_port conf <<< "$spec"
		STARTED+=("$container")
		docker run -d --name "$container" --network "$network" -p "127.0.0.1:$host_port:3000" --ulimit nofile=15000 \
			-v "$WORK_DIR:/etc/aerospike/conf:ro" \
			-v "$FEATURES:/etc/aerospike/features.conf:ro" \
			--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
	done
}

container_a="$PREFIX-a"
container_b="$PREFIX-b"
container_c="$PREFIX-c"
container_d="$PREFIX-d"
write_conf aerospike-a.conf "$NODE_A" "$PORT_A" "$container_b"
write_conf aerospike-b.conf "$NODE_B" "" "$container_a"
write_conf aerospike-c.conf "$NODE_C" "$PORT_C" "$container_d"
write_conf aerospike-d.conf "$NODE_D" "$PORT_D" "$container_c"

echo "Starting missing cluster: $NODE_A on 127.0.0.1:$PORT_A (alternate), $NODE_B on 127.0.0.1:$PORT_B (no alternate) ($AEROSPIKE_IMAGE)"
start_cluster "$PREFIX-missing" "$container_a|$PORT_A|aerospike-a.conf" "$container_b|$PORT_B|aerospike-b.conf"
echo "Starting control cluster: $NODE_C on 127.0.0.1:$PORT_C, $NODE_D on 127.0.0.1:$PORT_D (both alternate)"
start_cluster "$PREFIX-control" "$container_c|$PORT_C|aerospike-c.conf" "$container_d|$PORT_D|aerospike-d.conf"

for c in "$container_a" "$container_b" "$container_c" "$container_d"; do
	wait_cluster_size "$c" 2
done

for c in peers-clear-alt peers-clear-std service-clear-alt; do
	echo "SERVER-INFO $NODE_A $c: [$(docker exec "$container_a" asinfo -v "$c")]"
done
echo "SERVER-INFO $NODE_B service-clear-alt: [$(docker exec "$container_b" asinfo -v service-clear-alt)]"

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteAlternateMissing.class' \
	-Dalt.host=127.0.0.1 -Dalt.nodeA="$NODE_A" -Dalt.portA="$PORT_A" -Dalt.nodeB="$NODE_B" -Dalt.portB="$PORT_B" \
	-Dalt.nodeC="$NODE_C" -Dalt.portC="$PORT_C" -Dalt.nodeD="$NODE_D") > "$log" 2>&1
rc=$?
set -e

grep -E "ALT-MISSING-" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
