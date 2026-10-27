#!/usr/bin/env bash
#
# AWS EC2 tag seed discovery (AwsEc2TagSeedCandidateProvider) integration cases against a stub EC2
# endpoint. There is no AWS emulator: the stub EC2 Query API endpoint is served from the test JVM.
#
# Starts a two node Aerospike Enterprise cluster in Docker, runs SuiteAwsTags once against it and
# removes everything it started on exit, including on failure. Node A is reachable on 127.0.0.1 and
# node B on the host's outbound IPv4 address (AWS_TAGS_HOST_B), both on AWS_TAGS_PORT_BASE, because
# the provider sets one port on every seed. Node B is published on 127.0.0.1:AWS_TAGS_PORT_BASE+1 and
# reached on AWS_TAGS_HOST_B:AWS_TAGS_PORT_BASE through a python3 TCP forwarder this script runs,
# because rootless Podman cannot publish one port on two host addresses. Each node advertises a
# private address (10.0.0.1, 10.0.0.2) as alternate-access-address, which the client maps to the
# node's reachable address with ipMap. The stub EC2 endpoint listens on
# 127.0.0.1:AWS_TAGS_PORT_BASE+2 and returns both nodes as running instances whose private IP is the
# node's reachable address. The client is given no static seed.
#
# Credentials: the script exports AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN with
# fixed fake values, so the environment source of the provider's credential chain runs end to end.
# The stub checks only that each request carries a well-formed SigV4 Authorization header for that
# access key, the region and service ec2.
#
# Required environment:
#   AEROSPIKE_IMAGE           Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE   feature-key file
# Optional environment:
#   AWS_TAGS_PORT_BASE        first host port; the script uses AWS_TAGS_PORT_BASE to AWS_TAGS_PORT_BASE+2 (default 3650)
#   AWS_TAGS_HOST_B           non-loopback IPv4 address of this host for node B (default: the address
#                             of the interface holding the default route)
#   AWS_TAGS_SKIP_BUILD       set to 1 to skip installing the client module first
#   AWS_TAGS_MAVEN_OFFLINE    set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/discovery/run_aws_tags.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/discovery-aws-tags"
PORT="${AWS_TAGS_PORT_BASE:-3650}"
PORT_B=$((PORT + 1))
STUB_PORT=$((PORT + 2))
HOST_A=127.0.0.1
PRIVATE_A=10.0.0.1
PRIVATE_B=10.0.0.2
NODE_A=A1
NODE_B=B2
REGION=us-east-1
PREFIX="discovery-aws-tags-$$"

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

HOST_B="${AWS_TAGS_HOST_B:-$(python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("192.0.2.1", 9))
print(s.getsockname()[0])' 2>/dev/null || true)}"
case "$HOST_B" in
	""|127.*) die "no non-loopback IPv4 address found for node B; set AWS_TAGS_HOST_B" ;;
esac

export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_SESSION_TOKEN=FwoGZXIvYXdzEXAMPLEFAKESESSIONTOKENFORSTUBTESTS

MVN=(mvn)
[ "${AWS_TAGS_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

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

if [ "${AWS_TAGS_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
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

network="$PREFIX"
container_a="$PREFIX-a"
container_b="$PREFIX-b"
write_conf aerospike-a.conf docker "$NODE_A" "$PRIVATE_A" "$PORT" "$container_b"
write_conf aerospike-b.conf docker "$NODE_B" "$PRIVATE_B" "$PORT" "$container_a"

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

YAML="$WORK_DIR/client-aws-tags.yaml"
cat > "$YAML" <<EOF
version: 1.2.0
static:
  client:
    use_services_alternate: true
    ip_map:
      "$PRIVATE_A": "$HOST_A"
      "$PRIVATE_B": "$HOST_B"
    discovery:
      provider: aws-tags
      refresh_interval_ms: 1000
      tend_refresh_deadline_ms: 50
      aws:
        region: $REGION
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
        address_field: private_ip
        port: $PORT
        timeout_ms: 50
        init_timeout_ms: 5000
        endpoint: http://127.0.0.1:$STUB_PORT
EOF

echo "Starting cluster: $NODE_A on $HOST_A:$PORT, $NODE_B on $HOST_B:$PORT via 127.0.0.1:$PORT_B ($AEROSPIKE_IMAGE)"
echo "Stub EC2 endpoint: http://127.0.0.1:$STUB_PORT (in the test JVM), region $REGION, access key $AWS_ACCESS_KEY_ID"
docker network create "$network" >/dev/null
STARTED_NETWORKS+=("$network")

for spec in "$container_a|127.0.0.1:$PORT:3000|aerospike-a.conf" \
	"$container_b|127.0.0.1:$PORT_B:3000|aerospike-b.conf"; do
	IFS='|' read -r container publish conf <<< "$spec"
	STARTED+=("$container")
	docker run -d --name "$container" --network "$network" -p "$publish" --ulimit nofile=15000 \
		-v "$WORK_DIR:/etc/aerospike/conf:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
done

wait_cluster_size "$container_a" 2
wait_cluster_size "$container_b" 2

python3 "$WORK_DIR/forward.py" "$HOST_B" "$PORT" 127.0.0.1 "$PORT_B" > "$WORK_DIR/forward.log" 2>&1 &
FORWARDER=$!
sleep 1
kill -0 "$FORWARDER" 2>/dev/null || { cat "$WORK_DIR/forward.log" >&2; die "TCP forwarder for node B did not start"; }
cat "$WORK_DIR/forward.log"

log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteAwsTags.class' \
	-Daws.stubPort="$STUB_PORT" -Daws.region="$REGION" -Daws.namespace=test -Daws.port="$PORT" \
	-Daws.nodeA="$NODE_A" -Daws.hostA="$HOST_A" -Daws.privateA="$PRIVATE_A" \
	-Daws.nodeB="$NODE_B" -Daws.hostB="$HOST_B" -Daws.privateB="$PRIVATE_B" \
	-Daws.config="$YAML") > "$log" 2>&1
rc=$?
set -e

grep -E "AWS-TAGS-|CLIENT-LOG (WARN|ERROR)" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
