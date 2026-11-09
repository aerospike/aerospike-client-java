#!/usr/bin/env bash
#
# Discovery and translation configured from the YAML config file only.
#
# Starts two two-node Aerospike Enterprise clusters in Docker, runs SuiteYamlTranslation once
# against them and removes both on exit, including on failure. The client is configured only
# through a YAML file per case; every case seeds node A only.
#
# - pass-through: each node advertises alternate-access-address 127.0.0.1 and its own published
#   host port as alternate-access-port. The YAML sets use_services_alternate: true and
#   translation.provider: pass-through.
# - port-map: each node advertises only its container address on port 3000, which the host cannot
#   reach. The YAML sets translation.provider: port-map and on_miss: strict, mapping node A by its
#   advertised host:port and node B by node name to their published host ports.
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   YAML_PORT                 first of four host ports: pass-through A, B, port-map A, B (default 3630)
#   YAML_SUBNET               first three octets of the port-map /24 Docker network subnet (default 10.213.75)
#   TRANSLATION_SKIP_BUILD    set to 1 to skip installing the client module first
#   TRANSLATION_MAVEN_OFFLINE set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/translation/run_yaml.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/translation-yaml"
PT_PORT_A="${YAML_PORT:-3630}"
PT_PORT_B=$((PT_PORT_A + 1))
PM_PORT_A=$((PT_PORT_A + 2))
PM_PORT_B=$((PT_PORT_A + 3))
SUBNET="${YAML_SUBNET:-10.213.75}"
PM_IP_A="$SUBNET.11"
PM_IP_B="$SUBNET.12"
NODE_A=A1
NODE_B=B2
PREFIX="translation-yaml-$$"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
[ -n "${AEROSPIKE_FEATURES_FILE:-}" ] || die "AEROSPIKE_FEATURES_FILE is not set"
[ -r "$AEROSPIKE_FEATURES_FILE" ] || die "AEROSPIKE_FEATURES_FILE '$AEROSPIKE_FEATURES_FILE' is not readable"

unset AEROSPIKE_CLIENT_CONFIG_URL

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

# write_conf <file> <node-id> <mesh-seed> [alternate-access-port]
write_conf() {
	local conf="$1" node_id="$2" mesh_seed="$3" alt_port="${4:-}"
	local alternate=""

	if [ -n "$alt_port" ]; then
		alternate="
        alternate-access-address 127.0.0.1
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
        port 3000$alternate
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

# start <container> <network> <host-port> <conf> [ip]
start() {
	local container="$1" network="$2" host_port="$3" conf="$4" ip="${5:-}"
	local ip_args=()

	[ -n "$ip" ] && ip_args=(--ip "$ip")
	STARTED+=("$container")
	docker run -d --name "$container" --network "$network" ${ip_args[@]+"${ip_args[@]}"} \
		-p "127.0.0.1:$host_port:3000" --ulimit nofile=15000 \
		-v "$WORK_DIR:/etc/aerospike/conf:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
}

pt_network="$PREFIX-pt"
pt_a="$PREFIX-pt-a"
pt_b="$PREFIX-pt-b"
pm_network="$PREFIX-pm"
pm_a="$PREFIX-pm-a"
pm_b="$PREFIX-pm-b"

write_conf pt-a.conf "$NODE_A" "$pt_b" "$PT_PORT_A"
write_conf pt-b.conf "$NODE_B" "$pt_a" "$PT_PORT_B"
write_conf pm-a.conf "$NODE_A" "$PM_IP_B"
write_conf pm-b.conf "$NODE_B" "$PM_IP_A"

cat > "$WORK_DIR/pass-through.yaml" <<EOF
version: 1.2.0
static:
  client:
    use_services_alternate: true
    translation:
      provider: pass-through
dynamic:
  client:
    tend_interval: 250
EOF

cat > "$WORK_DIR/port-map.yaml" <<EOF
version: 1.2.0
static:
  client:
    translation:
      provider: port-map
      on_miss: strict
      port_map:
        "$PM_IP_A:3000": "127.0.0.1:$PM_PORT_A"
        "$NODE_B": "127.0.0.1:$PM_PORT_B"
dynamic:
  client:
    tend_interval: 250
EOF

echo "Starting pass-through cluster: $NODE_A on 127.0.0.1:$PT_PORT_A, $NODE_B on 127.0.0.1:$PT_PORT_B ($AEROSPIKE_IMAGE)"
docker network create "$pt_network" >/dev/null
STARTED_NETWORKS+=("$pt_network")
start "$pt_a" "$pt_network" "$PT_PORT_A" pt-a.conf
start "$pt_b" "$pt_network" "$PT_PORT_B" pt-b.conf

echo "Starting port-map cluster: $NODE_A ($PM_IP_A:3000) on 127.0.0.1:$PM_PORT_A, $NODE_B ($PM_IP_B:3000) on 127.0.0.1:$PM_PORT_B"
docker network create --subnet "$SUBNET.0/24" "$pm_network" >/dev/null || die "cannot create network $pm_network on $SUBNET.0/24"
STARTED_NETWORKS+=("$pm_network")
start "$pm_a" "$pm_network" "$PM_PORT_A" pm-a.conf "$PM_IP_A"
start "$pm_b" "$pm_network" "$PM_PORT_B" pm-b.conf "$PM_IP_B"

for c in "$pt_a" "$pt_b" "$pm_a" "$pm_b"; do
	wait_cluster_size "$c" 2
done

echo "pass-through YAML:"
cat "$WORK_DIR/pass-through.yaml"
echo "port-map YAML:"
cat "$WORK_DIR/port-map.yaml"

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteYamlTranslation.class' \
	-Dyaml.host=127.0.0.1 -Dyaml.namespace=test \
	-Dyaml.passThrough.config="$WORK_DIR/pass-through.yaml" \
	-Dyaml.passThrough.nodeA="$NODE_A" -Dyaml.passThrough.portA="$PT_PORT_A" \
	-Dyaml.passThrough.nodeB="$NODE_B" -Dyaml.passThrough.portB="$PT_PORT_B" \
	-Dyaml.portMap.config="$WORK_DIR/port-map.yaml" \
	-Dyaml.portMap.nodeA="$NODE_A" -Dyaml.portMap.portA="$PM_PORT_A" \
	-Dyaml.portMap.nodeB="$NODE_B" -Dyaml.portMap.portB="$PM_PORT_B") > "$log" 2>&1
rc=$?
set -e

grep -E "YAML-|Set ClientPolicy\.(useServicesAlternate|addressTranslator|translationOnMiss)" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
