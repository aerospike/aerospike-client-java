#!/usr/bin/env bash
#
# L2 discovery topology (TestL2Topology) built locally.
#
# Builds the topology .github/workflows/discovery-l2.yaml builds: three Aerospike nodes on a private
# Docker network with no published ports, a Toxiproxy container on that network and on a client
# network with one listener per node behind one endpoint hostname, and the client in its own
# container on the client network only, so the client has no route to the node addresses. Toxiproxy
# is set up by shared-workflows setup-discovery-topology at the commit discovery-l2.yaml pins. The
# script sets the variables the workflow sets, runs SuiteDiscovery as
# .github/discovery/run-l2-suite.sh does, and removes every container and network it created on
# exit, including on failure.
#
# The action calls Toxiproxy's control API with curl on the node network address. This script puts
# a curl on PATH that runs in a container on the node network, so the action also works where the
# host cannot route to container addresses (Docker Desktop, Podman machine).
#
# Optional environment:
#   DISCOVERY_SUBSET          ce (default) or ee. ee enables server TLS and TLS listeners (N-6) and
#                             needs FEATURES_CONF_PATH
#   FEATURES_CONF_PATH        Enterprise feature-key file
#   AEROSPIKE_IMAGE           server image (default aerospike/aerospike-server:latest on ce,
#                             aerospike/aerospike-server-enterprise:latest on ee)
#   SHARED_WORKFLOWS_DIR      local aerospike/shared-workflows clone to read the action from instead
#                             of downloading it
#   L2_DISABLE_LISTENERS      set to 1 to disable every Toxiproxy listener before the suite starts and
#                             skip the TCP check, to prove N-1's guard. Every L2 test then fails
#   L2_NODE_CONTROL           set to 1 to let N-11 start a node. The script sets DISCOVERY_CONTROL_DIR
#                             to a directory under the work directory, which the client container
#                             shares, and serves each <id>.request written there with <id>.ack: "start"
#                             starts one more node on the server network with the same config and mesh
#                             seeds and acknowledges its container and address, "stop <container>"
#                             removes that node
#   L2_PROFILE                default or reachable (default: default). reachable builds the same topology,
#                             then recreates the nodes with one rendered config per node: the last node sets
#                             access-address as-reachable.test and access-port to its listener port,
#                             the others keep their private addresses and set alternate-access-address
#                             as-reachable.test and alternate-access-port to their own listener port.
#                             The hostname resolves to Toxiproxy in the server and client containers
#                             (extra hosts). Sets DISCOVERY_REACHABLE_NODE to the last node's container
#                             and runs TestL2ReachableNode (N-10) only
#   L2_SKIP_BUILD             set to 1 to skip installing the client module first
#   L2_MAVEN_OFFLINE          set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/discovery/run_l2.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

# Inside the client container: the command of .github/discovery/run-l2-suite.sh.
run_client() {
	local host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
	local ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
	local port_base="${DISCOVERY_ENDPOINT_PORT_BASE:?DISCOVERY_ENDPOINT_PORT_BASE is required}"
	local subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"
	local suite='**/SuiteDiscovery.class'
	local args check_ports port

	[ "${L2_PROFILE:-default}" = "reachable" ] && suite='**/TestL2ReachableNode.class'

	if [ "$subset" = "ee" ]; then
		local tls_ports="${DISCOVERY_TLS_ENDPOINT_PORTS:?DISCOVERY_TLS_ENDPOINT_PORTS is required}"
		check_ports="$tls_ports"
		args="-h ${host}:aerospike-tls -p ${tls_ports%%,*} --tlsEnable"
	else
		check_ports="$ports"
		args="-h ${host} -p ${port_base}"
	fi

	if [ "${L2_DISABLE_LISTENERS:-0}" != "1" ]; then
		IFS=',' read -r -a port_list <<< "$check_ports"
		for port in "${port_list[@]}"; do
			echo "Checking TCP ${host}:${port}"
			timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
		done
	fi

	if [ -n "${AEROSPIKE_CA_CERT:-}" ]; then
		keytool -import -trustcacerts \
			-keystore "${JAVA_HOME}/lib/security/cacerts" \
			-storepass changeit -noprompt \
			-alias aerospike-ca \
			-file "${AEROSPIKE_CA_CERT}"
	fi

	local mvn=(mvn -B)
	[ "${L2_MAVEN_OFFLINE:-1}" = "1" ] && mvn+=(-o)
	exec "${mvn[@]}" -pl test -DskipTests=false \
		"-DrunSuite=${suite}" \
		"-Dargs=${args}" \
		test
}

if [ "${1:-}" = "--client" ]; then
	run_client
fi

# setup-discovery-topology and setup-aerospike-server commit pinned in discovery-l2.yaml.
SHARED_WORKFLOWS_COMMIT=9c1d4e91489995aa95187152b670b32cca9e890b
ACTION_PATH=.github/actions/setup-discovery-topology
CLIENT_IMAGE=maven:3-ibm-semeru-21-noble@sha256:0ad9f1918f4a30f89713a03e39659ef3135a2a08b257a90168a6033335a48bcb
CURL_IMAGE=curlimages/curl:8.11.1
SUBSET="${DISCOVERY_SUBSET:-ce}"
PROFILE="${L2_PROFILE:-default}"
REACHABLE_HOSTNAME=as-reachable.test
PREFIX="discovery-l2-$$"
WORK_DIR="$TEST_DIR/target/discovery-l2"
AS_NETWORK="$PREFIX-aerospike-net"
CLIENT_NETWORK="$PREFIX-client-net"
PROXY="$PREFIX-toxiproxy"
CLIENT="$PREFIX-client"
CONTROL_DIR="$WORK_DIR/control"
NODES=3

case "$SUBSET" in
	ce)
		IMAGE="${AEROSPIKE_IMAGE:-aerospike/aerospike-server:latest}"
		TLS_LISTEN_PORT_BASE=""
		;;
	ee)
		IMAGE="${AEROSPIKE_IMAGE:-aerospike/aerospike-server-enterprise:latest}"
		TLS_LISTEN_PORT_BASE=43331
		[ -n "${FEATURES_CONF_PATH:-}" ] || die "DISCOVERY_SUBSET=ee needs FEATURES_CONF_PATH"
		[ -r "$FEATURES_CONF_PATH" ] || die "FEATURES_CONF_PATH '$FEATURES_CONF_PATH' is not readable"
		;;
	*)
		die "DISCOVERY_SUBSET must be ce or ee (got: $SUBSET)"
		;;
esac

case "$PROFILE" in
	default|reachable) ;;
	*) die "L2_PROFILE must be default or reachable (got: $PROFILE)" ;;
esac

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
((BASH_VERSINFO[0] >= 4)) || die "bash 4 or later is required by setup-discovery-topology"

MVN=(mvn)
[ "${L2_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

STARTED=()
STARTED_NETWORKS=()
CONTROL_PID=""

# Nodes started on request carry the prefix but are not in STARTED.
cleanup() {
	if [ -n "$CONTROL_PID" ]; then
		kill "$CONTROL_PID" >/dev/null 2>&1 || true
		wait "$CONTROL_PID" 2>/dev/null || true
	fi
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for c in $(docker ps -aq --filter "name=^$PREFIX-" 2>/dev/null); do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for n in "${STARTED_NETWORKS[@]+"${STARTED_NETWORKS[@]}"}"; do
		docker network rm "$n" >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ "${L2_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/conf" "$WORK_DIR/action" "$WORK_DIR/bin" "$CONTROL_DIR"

for f in entrypoint.sh proxy_map.sh; do
	if [ -n "${SHARED_WORKFLOWS_DIR:-}" ]; then
		git -C "$SHARED_WORKFLOWS_DIR" show "$SHARED_WORKFLOWS_COMMIT:$ACTION_PATH/$f" > "$WORK_DIR/action/$f" ||
			die "$f not found at $SHARED_WORKFLOWS_COMMIT in $SHARED_WORKFLOWS_DIR"
	else
		curl -sfL "https://raw.githubusercontent.com/aerospike/shared-workflows/$SHARED_WORKFLOWS_COMMIT/$ACTION_PATH/$f" \
			-o "$WORK_DIR/action/$f" || die "failed to download $ACTION_PATH/$f at $SHARED_WORKFLOWS_COMMIT"
	fi
done

cat > "$WORK_DIR/bin/curl" <<EOF
#!/usr/bin/env bash
exec docker run --rm --network "$AS_NETWORK" "$CURL_IMAGE" "\$@"
EOF
chmod +x "$WORK_DIR/bin/curl"

container_names=""
mesh_seeds=""
for i in $(seq 1 "$NODES"); do
	container_names+="${container_names:+,}$PREFIX-$i"
	mesh_seeds+="        mesh-seed-address-port $PREFIX-$i 3002"$'\n'
done

feature_key=""
tls_config=""
tls_service=""
run_args=()

if [ "$SUBSET" = "ee" ]; then
	FEATURES="$(cd "$(dirname "$FEATURES_CONF_PATH")" && pwd)/$(basename "$FEATURES_CONF_PATH")"
	feature_key="feature-key-file /etc/aerospike/features.conf"
	run_args+=(-v "$FEATURES:/etc/aerospike/features.conf:ro")

	# Certificates as setup-aerospike-server generates them.
	TLS_DIR="$WORK_DIR/tls"
	mkdir -p "$TLS_DIR"
	{
		echo "[v3_req]"
		echo "subjectAltName = @alt_names"
		echo "[alt_names]"
		for i in $(seq 1 "$NODES"); do
			echo "DNS.$i = $PREFIX-$i"
		done
		echo "DNS.$((NODES + 1)) = localhost"
		echo "DNS.$((NODES + 2)) = docker"
		echo "IP.1 = 127.0.0.1"
	} > "$TLS_DIR/san.ext"
	(
		cd "$TLS_DIR"
		openssl req -x509 -newkey rsa:4096 -nodes -keyout ca.key -out ca.crt -days 365 \
			-subj "/CN=Aerospike-Server-Test-CA"
		openssl req -newkey rsa:4096 -nodes -keyout server.key -out server.csr -subj "/CN=aerospike-tls"
		openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 \
			-extensions v3_req -extfile san.ext
		rm -f ./*.csr ./*.srl san.ext
	) > "$WORK_DIR/openssl.log" 2>&1 || { cat "$WORK_DIR/openssl.log" >&2; die "certificate generation failed"; }
	chmod 644 "$TLS_DIR"/*
	run_args+=(-v "$TLS_DIR:/etc/aerospike/tls:ro")

	tls_config="    tls aerospike-tls {
        cert-file /etc/aerospike/tls/server.crt
        key-file  /etc/aerospike/tls/server.key
        ca-file   /etc/aerospike/tls/ca.crt
    }"
	tls_service="        tls-port 4333
        tls-authenticate-client false
        tls-name aerospike-tls"
fi

# setup-aerospike-server templates/multi-node.conf with namespace-memory.conf.
cat > "$WORK_DIR/conf/aerospike.conf" <<EOF
service {
    $feature_key
    cluster-name docker
}

logging {
    console {
        context any info
    }
}

network {
$tls_config
    service {
        address any
        port 3000
$tls_service
    }
    heartbeat {
        mode mesh
        address any
        port 3002
        interval 150
        timeout 10
$mesh_seeds    }
    fabric {
        address any
        port 3001
    }
}

namespace test {
    replication-factor 2
    storage-engine memory {
        data-size 1G
    }
}
EOF

wait_cluster_size() {
	local name="$1" size="$2"
	local i

	for i in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "CLUSTER-SIZE $size" >/dev/null; then
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name did not reach cluster size $size after 60s"
}

# start_node <container> <config file in conf/> [docker run options]
start_node() {
	local name="$1" conf="$2"
	shift 2
	docker run -d --name "$name" --network "$AS_NETWORK" --ulimit nofile=15000 "$@" \
		-v "$WORK_DIR/conf:/etc/aerospike/conf:ro" "${run_args[@]+"${run_args[@]}"}" \
		--entrypoint asd "$IMAGE" --foreground --config-file "/etc/aerospike/conf/$conf" >/dev/null
}

container_address() {
	docker inspect -f "{{(index .NetworkSettings.Networks \"$2\").IPAddress}}" "$1"
}

# Serve the requests N-11 writes to CONTROL_DIR until killed.
control_loop() {
	local extra="$NODES" request id action arg name ack

	while true; do
		for request in "$CONTROL_DIR"/*.request; do
			[ -e "$request" ] || continue
			id="$(basename "$request" .request)"
			action=""
			arg=""
			read -r action arg < "$request" || true
			rm -f "$request"
			ack="$CONTROL_DIR/$id.tmp"

			case "$action" in
				start)
					extra=$((extra + 1))
					name="$PREFIX-$extra"
					if start_node "$name" aerospike.conf 2> "$WORK_DIR/control.err"; then
						printf 'container=%s\naddress=%s\n' "$name" "$(container_address "$name" "$AS_NETWORK")" > "$ack"
						echo "Control: started $name"
					else
						printf 'error=%s\n' "$(tr '\n' ' ' < "$WORK_DIR/control.err")" > "$ack"
					fi
					;;
				stop)
					if [[ "$arg" == "$PREFIX"-* ]] && docker rm -f "$arg" > /dev/null 2> "$WORK_DIR/control.err"; then
						printf 'removed=%s\n' "$arg" > "$ack"
						echo "Control: removed $arg"
					else
						printf 'error=cannot remove %s %s\n' "$arg" "$(tr '\n' ' ' < "$WORK_DIR/control.err" 2>/dev/null)" > "$ack"
					fi
					;;
				*)
					printf 'error=unknown request: %s %s\n' "$action" "$arg" > "$ack"
					;;
			esac
			mv "$ack" "$CONTROL_DIR/$id.ack"
		done
		sleep 0.1
	done
}

echo "Starting $NODES node $SUBSET cluster on $AS_NETWORK, no published ports ($IMAGE)"
STARTED_NETWORKS+=("$CLIENT_NETWORK" "$AS_NETWORK")
docker network create "$AS_NETWORK" >/dev/null

IFS=',' read -r -a containers <<< "$container_names"
for container in "${containers[@]}"; do
	STARTED+=("$container")
	start_node "$container" aerospike.conf
done
for container in "${containers[@]}"; do
	wait_cluster_size "$container" "$NODES"
done

echo "Setting up discovery topology (shared-workflows $SHARED_WORKFLOWS_COMMIT)"
STARTED=("$CLIENT" "$PROXY" "${STARTED[@]}")
: > "$WORK_DIR/topology.out"
PATH="$WORK_DIR/bin:$PATH" \
	CONTAINER_NAMES="$container_names" \
	AEROSPIKE_NETWORK_NAME="$AS_NETWORK" \
	CLIENT_NETWORK_NAME="$CLIENT_NETWORK" \
	PROXY_CONTAINER_NAME="$PROXY" \
	TLS_LISTEN_PORT_BASE="$TLS_LISTEN_PORT_BASE" \
	GITHUB_OUTPUT="$WORK_DIR/topology.out" \
	bash "$WORK_DIR/action/entrypoint.sh" || die "setup-discovery-topology failed"

output() {
	grep -E "^$1=" "$WORK_DIR/topology.out" | head -1 | cut -d= -f2-
}

export DISCOVERY_SUBSET="$SUBSET"
export DISCOVERY_ENDPOINT_HOSTNAME="$(output endpoint-hostname)"
export DISCOVERY_ENDPOINT_PORT_BASE="$(output endpoint-port-base)"
export DISCOVERY_ENDPOINT_PORTS="$(output endpoint-ports)"
export DISCOVERY_TLS_ENDPOINT_PORTS="$(output tls-endpoint-ports)"
export DISCOVERY_PROXY_MAP="$(output proxy-name-port-map)"
export TOXIPROXY_URL="$(output toxiproxy-url)"
export TOXIPROXY_HOST="$(output toxiproxy-container-name)"
export TOXIPROXY_CONTROL_PORT=8474
export AEROSPIKE_CONTAINER_NAMES="$container_names"
export L2_DISABLE_LISTENERS="${L2_DISABLE_LISTENERS:-0}"
export L2_MAVEN_OFFLINE="${L2_MAVEN_OFFLINE:-1}"
export L2_PROFILE="$PROFILE"
export DISCOVERY_CONTROL_DIR=""
export DISCOVERY_REACHABLE_NODE=""

client_args=(--rm --name "$CLIENT" --network "$CLIENT_NETWORK" --workdir "$ROOT_DIR"
	-v "$ROOT_DIR:$ROOT_DIR" -v "$HOME/.m2:/root/.m2")

if [ "$PROFILE" = "reachable" ]; then
	proxy_server_address="$(container_address "$PROXY" "$AS_NETWORK")"
	proxy_client_address="$(container_address "$PROXY" "$CLIENT_NETWORK")"
	[ -n "$proxy_server_address" ] && [ -n "$proxy_client_address" ] || die "no Toxiproxy address"
	DISCOVERY_REACHABLE_NODE="${containers[$((NODES - 1))]}"

	# One rendered config per node, as setup-aerospike-server renders them.
	for container in "${containers[@]}"; do
		port=""
		IFS=',' read -r -a proxies <<< "$DISCOVERY_PROXY_MAP"
		for entry in "${proxies[@]}"; do
			[ "${entry%:*}" = "$container" ] && port="${entry##*:}"
		done
		[ -n "$port" ] || die "no listener for $container in $DISCOVERY_PROXY_MAP"

		if [ "$container" = "$DISCOVERY_REACHABLE_NODE" ]; then
			access="        access-address $REACHABLE_HOSTNAME"$'\n'"        access-port $port"$'\n'
		else
			access="        alternate-access-address $REACHABLE_HOSTNAME"$'\n'"        alternate-access-port $port"$'\n'
		fi
		ACCESS="$access" awk '{print} $1 == "port" && $2 == "3000" {printf "%s", ENVIRON["ACCESS"]}' \
			"$WORK_DIR/conf/aerospike.conf" > "$WORK_DIR/conf/$container.conf"
		echo "Node $container listener $port:"
		grep -E "access-(address|port)" "$WORK_DIR/conf/$container.conf"
	done

	echo "Recreating the nodes with $REACHABLE_HOSTNAME -> $proxy_server_address"
	for container in "${containers[@]}"; do
		docker rm -f "$container" >/dev/null
	done
	for container in "${containers[@]}"; do
		start_node "$container" "$container.conf" --add-host "$REACHABLE_HOSTNAME:$proxy_server_address"
	done
	for container in "${containers[@]}"; do
		wait_cluster_size "$container" "$NODES"
	done
	client_args+=(--add-host "$REACHABLE_HOSTNAME:$proxy_client_address")
fi

if [ "${L2_NODE_CONTROL:-0}" = "1" ]; then
	DISCOVERY_CONTROL_DIR="$CONTROL_DIR"
	control_loop &
	CONTROL_PID=$!
	echo "Serving node control requests in $CONTROL_DIR"
fi

if [ "$L2_DISABLE_LISTENERS" = "1" ]; then
	IFS=',' read -r -a proxies <<< "$DISCOVERY_PROXY_MAP"
	for entry in "${proxies[@]}"; do
		echo "Disabling Toxiproxy listener ${entry%:*}"
		"$WORK_DIR/bin/curl" -sf -X POST "$TOXIPROXY_URL/proxies/${entry%:*}" \
			-H "Content-Type: application/json" -d '{"enabled":false}' || die "failed to disable ${entry%:*}"
		echo
	done
fi

if [ "$SUBSET" = "ee" ]; then
	client_args+=(-v "$TLS_DIR:/aerospike-tls:ro" -e "AEROSPIKE_CA_CERT=/aerospike-tls/ca.crt")
fi

log="$WORK_DIR/mvn.log"
echo "Running the $PROFILE profile in $CLIENT on $CLIENT_NETWORK ($CLIENT_IMAGE)"
set +e
docker run "${client_args[@]}" \
	-e DISCOVERY_SUBSET \
	-e DISCOVERY_ENDPOINT_HOSTNAME \
	-e DISCOVERY_ENDPOINT_PORT_BASE \
	-e DISCOVERY_ENDPOINT_PORTS \
	-e DISCOVERY_TLS_ENDPOINT_PORTS \
	-e DISCOVERY_PROXY_MAP \
	-e TOXIPROXY_URL \
	-e TOXIPROXY_HOST \
	-e TOXIPROXY_CONTROL_PORT \
	-e AEROSPIKE_CONTAINER_NAMES \
	-e L2_DISABLE_LISTENERS \
	-e L2_MAVEN_OFFLINE \
	-e L2_PROFILE \
	-e DISCOVERY_CONTROL_DIR \
	-e DISCOVERY_REACHABLE_NODE \
	"$CLIENT_IMAGE" \
	bash test/discovery/run_l2.sh --client > "$log" 2>&1
rc=$?
set -e

grep -E "L2-|CLIENT-LOG (WARN|ERROR)" "$log" || true
grep -E "^(\[[A-Z]+\] )?Tests run:.*in com\.aerospike" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
