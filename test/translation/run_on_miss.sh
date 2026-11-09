#!/usr/bin/env bash
#
# Address translation miss (translation.on_miss) integration cases.
#
# Starts a two node Aerospike Enterprise cluster in Docker, runs SuiteTranslationMiss once against
# it and removes the cluster on exit, including on failure. Each node advertises
# alternate-access-address 127.0.0.1 and its own published host port as alternate-access-port, so
# with useServicesAlternate the peer each node advertises is reachable from the host only as
# advertised. Every case seeds node A only.
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   TRANSLATION_PORT          host port published for node A; node B uses the next port (default 3610)
#   TRANSLATION_SKIP_BUILD    set to 1 to skip installing the client module first
#   TRANSLATION_MAVEN_OFFLINE set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/translation/run_on_miss.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/translation-on-miss"
PORT_A="${TRANSLATION_PORT:-3610}"
PORT_B=$((PORT_A + 1))
NODE_A=A1
NODE_B=B2
PREFIX="translation-on-miss-$$"

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
	local conf="$1" node_id="$2" alt_port="$3" mesh_seed="$4"
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

network="$PREFIX"
container_a="$PREFIX-a"
container_b="$PREFIX-b"
write_conf aerospike-a.conf "$NODE_A" "$PORT_A" "$container_b"
write_conf aerospike-b.conf "$NODE_B" "$PORT_B" "$container_a"

echo "Starting cluster: $NODE_A on 127.0.0.1:$PORT_A, $NODE_B on 127.0.0.1:$PORT_B ($AEROSPIKE_IMAGE)"
docker network create "$network" >/dev/null
STARTED_NETWORKS+=("$network")

for spec in "$container_a|$PORT_A|aerospike-a.conf" "$container_b|$PORT_B|aerospike-b.conf"; do
	IFS='|' read -r container host_port conf <<< "$spec"
	STARTED+=("$container")
	docker run -d --name "$container" --network "$network" -p "127.0.0.1:$host_port:3000" --ulimit nofile=15000 \
		-v "$WORK_DIR:/etc/aerospike/conf:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
done
wait_cluster_size "$container_a" 2
wait_cluster_size "$container_b" 2

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteTranslationMiss.class' \
	-Dtranslation.host=127.0.0.1 -Dtranslation.nodeA="$NODE_A" -Dtranslation.portA="$PORT_A" \
	-Dtranslation.nodeB="$NODE_B" -Dtranslation.portB="$PORT_B" -Dtranslation.namespace=test) > "$log" 2>&1
rc=$?
set -e

grep -E "ON-MISS-" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
