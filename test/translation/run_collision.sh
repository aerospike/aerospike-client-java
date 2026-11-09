#!/usr/bin/env bash
#
# Translated address collision integration cases.
#
# Starts two independent two node Aerospike Enterprise clusters in Docker, runs
# SuiteTranslationCollision once against them and removes both clusters on exit, including on
# failure. Every node advertises alternate-access-address 127.0.0.1. In the control cluster each
# node advertises its own published host port as alternate-access-port. In the collision cluster
# node B advertises node A's published host port, so with useServicesAlternate both nodes translate
# to the same host:port. Every case seeds node A only.
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   TRANSLATION_PORT          first of four host ports published: control A, control B, collision A,
#                             collision B (default 3620)
#   TRANSLATION_SKIP_BUILD    set to 1 to skip installing the client module first
#   TRANSLATION_MAVEN_OFFLINE set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/translation/run_collision.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/translation-collision"
CONTROL_PORT_A="${TRANSLATION_PORT:-3620}"
CONTROL_PORT_B=$((CONTROL_PORT_A + 1))
COLLISION_PORT_A=$((CONTROL_PORT_A + 2))
COLLISION_PORT_B=$((CONTROL_PORT_A + 3))
CONTROL_A=A1
CONTROL_B=B2
COLLISION_A=C3
COLLISION_B=D4
PREFIX="translation-collision-$$"

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

write_conf() {
	local conf="$1" node_id="$2" alt_port="$3" mesh_seed="$4" cluster_name="$5"
	cat > "$WORK_DIR/$conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name $cluster_name
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
        alternate-access-address 127.0.0.1
        alternate-access-port $alt_port
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
        data-size 1G
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
	local name="$1" node_a="$2" port_a="$3" node_b="$4" port_b="$5" alt_port_b="$6"
	local network="$PREFIX-$name"
	local container_a="$PREFIX-$name-a"
	local container_b="$PREFIX-$name-b"
	local spec container host_port conf

	write_conf "$name-a.conf" "$node_a" "$port_a" "$container_b" "$name"
	write_conf "$name-b.conf" "$node_b" "$alt_port_b" "$container_a" "$name"

	echo "Starting $name cluster: $node_a on 127.0.0.1:$port_a advertising $port_a," \
		"$node_b on 127.0.0.1:$port_b advertising $alt_port_b ($AEROSPIKE_IMAGE)"
	docker network create "$network" >/dev/null
	STARTED_NETWORKS+=("$network")

	for spec in "$container_a|$port_a|$name-a.conf" "$container_b|$port_b|$name-b.conf"; do
		IFS='|' read -r container host_port conf <<< "$spec"
		STARTED+=("$container")
		docker run -d --name "$container" --network "$network" -p "127.0.0.1:$host_port:3000" --ulimit nofile=15000 \
			-v "$WORK_DIR:/etc/aerospike/conf:ro" \
			-v "$FEATURES:/etc/aerospike/features.conf:ro" \
			--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
	done
	wait_cluster_size "$container_a" 2
	wait_cluster_size "$container_b" 2
}

start_cluster control "$CONTROL_A" "$CONTROL_PORT_A" "$CONTROL_B" "$CONTROL_PORT_B" "$CONTROL_PORT_B"
start_cluster collision "$COLLISION_A" "$COLLISION_PORT_A" "$COLLISION_B" "$COLLISION_PORT_B" "$COLLISION_PORT_A"

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteTranslationCollision.class' \
	-Dtranslation.host=127.0.0.1 -Dtranslation.namespace=test \
	-Dcontrol.nodeA="$CONTROL_A" -Dcontrol.portA="$CONTROL_PORT_A" \
	-Dcontrol.nodeB="$CONTROL_B" -Dcontrol.portB="$CONTROL_PORT_B" \
	-Dcollision.nodeA="$COLLISION_A" -Dcollision.portA="$COLLISION_PORT_A" \
	-Dcollision.nodeB="$COLLISION_B") > "$log" 2>&1
rc=$?
set -e

grep -E "COLLISION-" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
