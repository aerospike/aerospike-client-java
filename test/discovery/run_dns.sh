#!/usr/bin/env bash
#
# DNS seed discovery (DnsSeedCandidateProvider) integration cases.
#
# Starts a two node Aerospike Enterprise cluster, a standalone third node and a CoreDNS server in
# Docker, runs SuiteDnsDiscovery once against them and removes everything it started on exit,
# including on failure. Node A is reachable on 127.0.0.1 and node B on the host's outbound IPv4
# address (DNS_HOST_B), both on DNS_PORT_BASE, and each advertises that address as
# alternate-access-address, so with useServicesAlternate both are reachable from the host only as
# advertised. Node B is published on 127.0.0.1:DNS_PORT_BASE+1 and reached on
# DNS_HOST_B:DNS_PORT_BASE through a python3 TCP forwarder this script runs, because rootless Podman
# cannot publish one port on two host addresses. Node C is published on
# 127.0.0.1:DNS_PORT_BASE+2. CoreDNS serves zone dns.test from a zone file this script writes, on
# 127.0.0.1:DNS_PORT_BASE+5, and reloads it every second. The client is given no static seed.
#
# Zone dns.test:
#   seeds            A 127.0.0.1, A DNS_HOST_B    (node A and node B, A/AAAA case)
#   _aerospike._tcp  SRV to node-a and node-b     (SRV case)
#   _change._tcp     SRV to node-a and node-b, rewritten by the test to node-c
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   DNS_PORT_BASE             first host port; the script uses DNS_PORT_BASE to DNS_PORT_BASE+5 (default 3640)
#   DNS_HOST_B                non-loopback IPv4 address of this host for node B (default: the address
#                             of the interface holding the default route)
#   COREDNS_IMAGE             DNS server image (default coredns/coredns:1.11.3)
#   DNS_SKIP_BUILD            set to 1 to skip installing the client module first
#   DNS_MAVEN_OFFLINE         set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/discovery/run_dns.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/discovery-dns"
PORT="${DNS_PORT_BASE:-3640}"
PORT_B=$((PORT + 1))
PORT_C=$((PORT + 2))
DNS_PORT=$((PORT + 5))
HOST_A=127.0.0.1
HOST_C=127.0.0.1
NODE_A=A1
NODE_B=B2
NODE_C=C3
COREDNS_IMAGE="${COREDNS_IMAGE:-coredns/coredns:1.11.3}"
PREFIX="discovery-dns-$$"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
[ -n "${AEROSPIKE_FEATURES_FILE:-}" ] || die "AEROSPIKE_FEATURES_FILE is not set"
[ -r "$AEROSPIKE_FEATURES_FILE" ] || die "AEROSPIKE_FEATURES_FILE '$AEROSPIKE_FEATURES_FILE' is not readable"
command -v python3 >/dev/null 2>&1 || die "python3 not found (TCP forwarder for node B)"

HOST_B="${DNS_HOST_B:-$(python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("192.0.2.1", 9))
print(s.getsockname()[0])' 2>/dev/null || true)}"
case "$HOST_B" in
	""|127.*) die "no non-loopback IPv4 address found for node B; set DNS_HOST_B" ;;
esac

MVN=(mvn)
[ "${DNS_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

STARTED=()
STARTED_NETWORKS=()
FORWARDER=

cleanup() {
	if [ -n "$FORWARDER" ]; then
		kill "$FORWARDER" >/dev/null 2>&1 || true
	fi
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for n in "${STARTED_NETWORKS[@]+"${STARTED_NETWORKS[@]}"}"; do
		docker network rm "$n" >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ "${DNS_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/coredns"
FEATURES="$(cd "$(dirname "$AEROSPIKE_FEATURES_FILE")" && pwd)/$(basename "$AEROSPIKE_FEATURES_FILE")"

write_conf() {
	local conf="$1" cluster="$2" node_id="$3" alt_address="$4" alt_port="$5" mesh_seed="$6"
	cat > "$WORK_DIR/$conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name $cluster
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
        alternate-access-address $alt_address
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

wait_dns() {
	local name="$1"
	local i

	for i in $(seq 1 30); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "CoreDNS-" >/dev/null; then
			sleep 1
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name did not start after 30s"
}

network="$PREFIX"
container_a="$PREFIX-a"
container_b="$PREFIX-b"
container_c="$PREFIX-c"
container_dns="$PREFIX-coredns"
write_conf aerospike-a.conf docker "$NODE_A" "$HOST_A" "$PORT" "$container_b"
write_conf aerospike-b.conf docker "$NODE_B" "$HOST_B" "$PORT" "$container_a"
write_conf aerospike-c.conf docker-c "$NODE_C" "$HOST_C" "$PORT_C" 127.0.0.1

cat > "$WORK_DIR/coredns/Corefile" <<EOF
dns.test:53 {
    file /etc/coredns/dns.test.db {
        reload 1s
    }
    errors
}
EOF

ZONE="$WORK_DIR/coredns/dns.test.db"
cat > "$ZONE" <<EOF
\$ORIGIN dns.test.
@ 3600 IN SOA ns.dns.test. admin.dns.test. 1 60 60 60 1
@ 3600 IN NS ns.dns.test.
ns 1 IN A 127.0.0.1
seeds 1 IN A $HOST_A
seeds 1 IN A $HOST_B
node-a 1 IN A $HOST_A
node-b 1 IN A $HOST_B
node-c 1 IN A $HOST_C
_aerospike._tcp 1 IN SRV 10 10 $PORT node-a
_aerospike._tcp 1 IN SRV 10 10 $PORT node-b
_change._tcp 1 IN SRV 10 10 $PORT node-a
_change._tcp 1 IN SRV 10 10 $PORT node-b
EOF

cat > "$WORK_DIR/forward.py" <<'EOF'
import socket, sys, threading

listen_host, listen_port, target_host, target_port = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
server = socket.socket(socket.AF_INET6 if ":" in listen_host else socket.AF_INET)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((listen_host, listen_port))
server.listen(64)
print("forwarding %s:%d to %s:%d" % (listen_host, listen_port, target_host, target_port), flush=True)

def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

while True:
    client, _ = server.accept()
    try:
        upstream = socket.create_connection((target_host, target_port))
    except OSError:
        client.close()
        continue
    threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, client), daemon=True).start()
EOF

YAML="$WORK_DIR/client-dns.yaml"
cat > "$YAML" <<EOF
version: 1.2.0
static:
  client:
    use_services_alternate: true
    discovery:
      provider: dns
      refresh_interval_ms: 1000
      dns:
        names:
          - seeds.dns.test
        record_type: a
        port: $PORT
        timeout_ms: 80
        servers:
          - 127.0.0.1:$DNS_PORT
EOF

echo "Starting cluster: $NODE_A on $HOST_A:$PORT, $NODE_B on $HOST_B:$PORT via 127.0.0.1:$PORT_B ($AEROSPIKE_IMAGE)"
echo "Starting standalone node: $NODE_C on $HOST_C:$PORT_C"
echo "Starting DNS server on 127.0.0.1:$DNS_PORT ($COREDNS_IMAGE)"
docker network create "$network" >/dev/null
STARTED_NETWORKS+=("$network")

for spec in "$container_a|127.0.0.1:$PORT:3000|aerospike-a.conf|$network" \
	"$container_b|127.0.0.1:$PORT_B:3000|aerospike-b.conf|$network" \
	"$container_c|127.0.0.1:$PORT_C:3000|aerospike-c.conf|bridge"; do
	IFS='|' read -r container publish conf net <<< "$spec"
	STARTED+=("$container")
	docker run -d --name "$container" --network "$net" -p "$publish" --ulimit nofile=15000 \
		-v "$WORK_DIR:/etc/aerospike/conf:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
done

STARTED+=("$container_dns")
docker run -d --name "$container_dns" -p "127.0.0.1:$DNS_PORT:53/udp" -p "127.0.0.1:$DNS_PORT:53/tcp" \
	-v "$WORK_DIR/coredns:/etc/coredns:ro" "$COREDNS_IMAGE" -conf /etc/coredns/Corefile >/dev/null

wait_cluster_size "$container_a" 2
wait_cluster_size "$container_b" 2
wait_cluster_size "$container_c" 1
wait_dns "$container_dns"

python3 "$WORK_DIR/forward.py" "$HOST_B" "$PORT" 127.0.0.1 "$PORT_B" > "$WORK_DIR/forward.log" 2>&1 &
FORWARDER=$!
sleep 1
kill -0 "$FORWARDER" 2>/dev/null || { cat "$WORK_DIR/forward.log" >&2; die "TCP forwarder for node B did not start"; }
cat "$WORK_DIR/forward.log"

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteDnsDiscovery.class' \
	-Ddns.server="127.0.0.1:$DNS_PORT" -Ddns.zoneFile="$ZONE" -Ddns.container="$container_dns" \
	-Ddns.namespace=test -Ddns.port="$PORT" \
	-Ddns.nodeA="$NODE_A" -Ddns.hostA="$HOST_A" -Ddns.nodeB="$NODE_B" -Ddns.hostB="$HOST_B" \
	-Ddns.hostC="$HOST_C" -Ddns.portC="$PORT_C" -Ddns.config="$YAML") > "$log" 2>&1
rc=$?
set -e

grep -E "DNS-DISCOVERY-|CLIENT-LOG (WARN|ERROR)" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
