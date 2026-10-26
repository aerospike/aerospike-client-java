#!/usr/bin/env bash
#
# Kubernetes discovery suite (SuiteDiscoveryK8s) against a kind cluster running the Aerospike
# Kubernetes Operator, built locally.
#
# The default profile builds the cluster .github/workflows/discovery-k8s.yaml builds, with
# shared-workflows setup-kind-ako at the commit that workflow pins: one kind node, the operator, an
# AerospikeCluster with multiPodPerHost: true and alternateAccess: configuredIP, the node labelled
# with the endpoint hostname, a CoreDNS entry for it, and ingress-nginx tcp-services. It then sets
# the variables .github/discovery/run-l3-suite.sh sets and runs SuiteDiscoveryK8s, as that script
# does.
#
# The per-node profile builds what setup-kind-ako cannot: one Aerospike pod per kind worker
# (multiPodPerHost: false), each worker labelled with its own configured-alternate-access-address
# (node-1.<L3_PER_NODE_DOMAIN>, node-2..., one per worker), and CoreDNS resolving each name to its
# worker. The operator, the AerospikeCluster and the secrets come from the same setup-kind-ako
# functions.
#
# The client dials each kind node's address, as the workflow's client does on a Linux runner. That
# address is not reachable from a macOS host, so the client runs in a container on the kind network,
# where each labelled hostname resolves to its node. The action writes the endpoint hostname into
# /etc/hosts; this script never does.
#
# The script refuses to start when a kind cluster with the same name exists, and deletes the kind
# cluster, the client container and the kind network (when it created that network) on exit,
# including on failure.
#
# Required:
#   kind, kubectl and helm on PATH, and docker (podman when KIND_EXPERIMENTAL_PROVIDER=podman)
#   FEATURES_CONF_PATH        Enterprise feature-key file
#
# Optional environment:
#   L3_PROFILE                default or per-node (default: default)
#   L3_KIND_CLUSTER           kind cluster name (default: discovery-l3)
#   L3_CLUSTER_SIZE           initial AerospikeCluster size on the default profile, 1-8 (default: the
#                             action's default, 1)
#   L3_PER_NODE_WORKERS       kind workers, and Aerospike pods, on the per-node profile, 2-8 (default: 2)
#   L3_PER_NODE_DOMAIN        domain of the per-node hostnames (default: as-endpoint.test)
#   KIND_EXPERIMENTAL_PROVIDER passed to kind; podman also makes this script use the podman CLI
#   SHARED_WORKFLOWS_DIR      local aerospike/shared-workflows clone to read the action from instead
#                             of downloading it
#   L3_SKIP_BUILD             set to 1 to skip installing the client module first
#   L3_MAVEN_OFFLINE          set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: FEATURES_CONF_PATH=/path/to/features.conf test/discovery/run_l3.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

run_suite() {
	local seed="${L3_SEED:?L3_SEED is required}"
	local password="${AEROSPIKE_ADMIN_PASSWORD:?AEROSPIKE_ADMIN_PASSWORD is required}"
	local mvn=(mvn -B)

	export KUBECONFIG="${DISCOVERY_KUBECONFIG:?DISCOVERY_KUBECONFIG is required}"
	[ "${L3_MAVEN_OFFLINE:-1}" = "1" ] && mvn+=(-o)
	"${mvn[@]}" -pl test -DskipTests=false \
		'-DrunSuite=**/SuiteDiscoveryK8s.class' \
		"-Dargs=-h ${seed} -p ${seed##*:} -U admin -P ${password}" \
		test
}

if [ "${1:-}" = "--client" ]; then
	cd "$ROOT_DIR"
	run_suite
	exit $?
fi

# setup-kind-ako at shared-workflows #336 head, the commit discovery-k8s.yaml pins.
SHARED_WORKFLOWS_COMMIT=9ab82802cc324d8407bcb42e728d4e03aa180565
ACTION_PATH=.github/actions/setup-kind-ako
CLIENT_IMAGE=maven:3-ibm-semeru-21-noble@sha256:0ad9f1918f4a30f89713a03e39659ef3135a2a08b257a90168a6033335a48bcb
PROFILE="${L3_PROFILE:-default}"
CLUSTER="${L3_KIND_CLUSTER:-discovery-l3}"
WORKERS="${L3_PER_NODE_WORKERS:-2}"
PER_NODE_DOMAIN="${L3_PER_NODE_DOMAIN:-as-endpoint.test}"
WORK_DIR="$TEST_DIR/target/discovery-l3"
ACTION_DIR="$WORK_DIR/action"
KIND_NETWORK=kind
CLIENT="$CLUSTER-client"

case "$PROFILE" in
	default|per-node) ;;
	*) die "L3_PROFILE must be default or per-node (got: $PROFILE)" ;;
esac
[[ "$CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "L3_KIND_CLUSTER must be lower-case letters, digits and dashes (got: $CLUSTER)"
[[ "$WORKERS" =~ ^[2-8]$ ]] || die "L3_PER_NODE_WORKERS must be 2-8 (got: $WORKERS)"
[[ "$PER_NODE_DOMAIN" =~ ^[a-z0-9.-]+$ ]] || die "L3_PER_NODE_DOMAIN must be a DNS name (got: $PER_NODE_DOMAIN)"

for tool in kind kubectl helm; do
	command -v "$tool" >/dev/null 2>&1 || die "$tool not found on PATH"
done
if [ "${KIND_EXPERIMENTAL_PROVIDER:-}" = "podman" ]; then
	DOCKER=podman
else
	DOCKER=docker
fi
command -v "$DOCKER" >/dev/null 2>&1 || die "$DOCKER not found on PATH"
"$DOCKER" info >/dev/null 2>&1 || die "$DOCKER is not reachable"
command -v openssl >/dev/null 2>&1 || die "openssl not found on PATH"
((BASH_VERSINFO[0] >= 4)) || die "bash 4 or later is required by setup-kind-ako"

[ -n "${FEATURES_CONF_PATH:-}" ] || die "FEATURES_CONF_PATH is required: the AerospikeCluster runs Enterprise"
[ -r "$FEATURES_CONF_PATH" ] || die "FEATURES_CONF_PATH '$FEATURES_CONF_PATH' is not readable"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
	die "kind cluster $CLUSTER already exists; delete it or set L3_KIND_CLUSTER"
fi

MVN=(mvn)
[ "${L3_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

rm -rf "$WORK_DIR"
mkdir -p "$ACTION_DIR" "$WORK_DIR/bin" "$WORK_DIR/helm"

for f in lib.sh entrypoint.sh action.yaml; do
	if [ -n "${SHARED_WORKFLOWS_DIR:-}" ]; then
		git -C "$SHARED_WORKFLOWS_DIR" show "$SHARED_WORKFLOWS_COMMIT:$ACTION_PATH/$f" > "$ACTION_DIR/$f" ||
			die "$f not found at $SHARED_WORKFLOWS_COMMIT in $SHARED_WORKFLOWS_DIR"
	else
		curl -sfL "https://raw.githubusercontent.com/aerospike/shared-workflows/$SHARED_WORKFLOWS_COMMIT/$ACTION_PATH/$f" \
			-o "$ACTION_DIR/$f" || die "failed to download $ACTION_PATH/$f at $SHARED_WORKFLOWS_COMMIT"
	fi
done

awk '
	/render_hosts_file "\$\(cat \/etc\/hosts\)"/ { skip = 1 }
	skip && /^    fi$/ { skip = 0; next }
	! skip
' "$ACTION_DIR/entrypoint.sh" > "$ACTION_DIR/entrypoint-local.sh"
if grep -nE 'sudo|/etc/hosts' "$ACTION_DIR/entrypoint-local.sh"; then
	die "entrypoint.sh at $SHARED_WORKFLOWS_COMMIT still writes /etc/hosts after removing its hosts step"
fi
sed '/^case \${1-} in$/,$d' "$ACTION_DIR/entrypoint-local.sh" > "$ACTION_DIR/functions.sh"
grep -q '^cmd_setup()' "$ACTION_DIR/functions.sh" || die "entrypoint.sh at $SHARED_WORKFLOWS_COMMIT has no cmd_setup"

KIND_IMAGE="$(grep -oE 'kindest/node:[^ "]+' "$ACTION_DIR/action.yaml" | head -1)"
SERVER_IMAGE="$(sed -n '/^  server-image:/,/default:/s/^ *default: *//p' "$ACTION_DIR/action.yaml")"
[ -n "$KIND_IMAGE" ] || die "no kind node image in action.yaml"
[ -n "$SERVER_IMAGE" ] || die "no server-image default in action.yaml"
DEFAULT_SIZE="$(sed -n '/^  cluster-size:/,/default:/s/^ *default: *"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p' "$ACTION_DIR/action.yaml")"
CLUSTER_SIZE="${L3_CLUSTER_SIZE:-$DEFAULT_SIZE}"

per_node_names=()
if [ "$PROFILE" = "per-node" ]; then
	CLUSTER_SIZE="$WORKERS"
	for i in $(seq 1 "$WORKERS"); do
		per_node_names+=("node-$i.$PER_NODE_DOMAIN")
	done
fi

KIND_NETWORK_EXISTED=0
"$DOCKER" network inspect "$KIND_NETWORK" >/dev/null 2>&1 && KIND_NETWORK_EXISTED=1
CREATED=0

cleanup() {
	"$DOCKER" rm -f "$CLIENT" >/dev/null 2>&1 || true
	if [ "$CREATED" = "1" ]; then
		echo "Deleting kind cluster $CLUSTER"
		kind delete cluster --name "$CLUSTER" --kubeconfig "$WORK_DIR/kubeconfig" || true
	fi
	if [ "$KIND_NETWORK_EXISTED" = "0" ] && "$DOCKER" network inspect "$KIND_NETWORK" >/dev/null 2>&1; then
		"$DOCKER" network rm "$KIND_NETWORK" >/dev/null || true
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ "${L3_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

export KUBECONFIG="$WORK_DIR/kubeconfig"
export RUNNER_TEMP="$WORK_DIR"
export HELM_CONFIG_HOME="$WORK_DIR/helm/config"
export HELM_CACHE_HOME="$WORK_DIR/helm/cache"
export HELM_DATA_HOME="$WORK_DIR/helm/data"
FEATURES_CONTENT="$(cat "$FEATURES_CONF_PATH")"

output() {
	grep -E "^$1=" "$2" | head -1 | cut -d= -f2-
}

echo "Preparing setup-kind-ako (shared-workflows $SHARED_WORKFLOWS_COMMIT), $PROFILE profile"
ADMIN_PASSWORD="" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" SERVER_IMAGE="$SERVER_IMAGE" \
	GITHUB_OUTPUT="$WORK_DIR/prepare.out" bash "$ACTION_DIR/entrypoint-local.sh" prepare >/dev/null ||
	die "setup-kind-ako prepare failed"
PASSWORD="$(output admin-password "$WORK_DIR/prepare.out")"
KIND_CONFIG="$(output kind-config "$WORK_DIR/prepare.out")"

if [ "$PROFILE" = "per-node" ]; then
	KIND_CONFIG="$WORK_DIR/kind-config-per-node.yaml"
	{
		printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n'
		for i in $(seq 1 "$WORKERS"); do
			printf '  - role: worker\n'
		done
	} > "$KIND_CONFIG"
fi

echo "Creating kind cluster $CLUSTER ($KIND_IMAGE)"
CREATED=1
kind create cluster --name "$CLUSTER" --image "$KIND_IMAGE" --config "$KIND_CONFIG" --wait 180s \
	--kubeconfig "$KUBECONFIG" || die "kind create cluster failed"

if [ "$PROFILE" = "default" ]; then
	ADMIN_PASSWORD="$PASSWORD" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" \
		SERVER_IMAGE="$SERVER_IMAGE" GITHUB_OUTPUT="$WORK_DIR/setup.out" \
		bash "$ACTION_DIR/entrypoint-local.sh" setup 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log" ||
		die "setup-kind-ako setup failed (log: $WORK_DIR/setup.log)"

	export DISCOVERY_ENDPOINT_HOSTNAME="$(output endpoint-hostname "$WORK_DIR/setup.out")"
	export DISCOVERY_INGRESS_PORTS="$(output ingress-ports "$WORK_DIR/setup.out")"
	export DISCOVERY_SERVICE_NODE_PORTS="$(output service-node-ports "$WORK_DIR/setup.out")"
	export DISCOVERY_NODE_PORT_RANGE="$(output node-port-range "$WORK_DIR/setup.out")"
	export DISCOVERY_KUBECONFIG=/kubeconfig
	export AEROSPIKE_ADMIN_PASSWORD="$PASSWORD"
	[ "$DISCOVERY_NODE_PORT_RANGE" = "30000-32767" ] ||
		die "NodePort range must stay 30000-32767 (got: $DISCOVERY_NODE_PORT_RANGE)"
	L3_SEED="$DISCOVERY_ENDPOINT_HOSTNAME:${DISCOVERY_INGRESS_PORTS%%,*}"
else
	# Not in an && or || list, so set -e holds inside the subshell.
	set +e
	(
		# shellcheck source=/dev/null
		source "$ACTION_DIR/functions.sh"
		ADMIN_PASSWORD="$PASSWORD"
		CLUSTER_SIZE="$WORKERS"
		SERVER_IMAGE="$SERVER_IMAGE"
		FEATURES_CONTENT="$FEATURES_CONTENT"
		mkdir -p "$WORK_DIR"

		eval "action_$(declare -f render_cluster_manifest)"
		render_cluster_manifest() {
			action_render_cluster_manifest "$@" | sed 's/^    multiPodPerHost: true$/    multiPodPerHost: false/'
		}
		render_cluster_manifest "$SERVER_IMAGE" "$WORKERS" | grep -q '^    multiPodPerHost: false$' ||
			error "AerospikeCluster manifest has no multiPodPerHost to set to false"

		mapfile -t workers < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
			-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
		((${#workers[@]} == WORKERS)) || error "expected $WORKERS kind workers, found ${workers[*]}"

		hosts_block="    ${CORE_DNS_MARKER}"$'\n'"    hosts {"$'\n'
		for i in "${!workers[@]}"; do
			name="${per_node_names[$i]}"
			ip="$(kubectl get node "${workers[$i]}" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
			kubectl label node "${workers[$i]}" "${ALTERNATE_ACCESS_LABEL}=${name}" --overwrite
			hosts_block+="        $ip $name"$'\n'
			echo "Worker ${workers[$i]} ($ip) labelled ${ALTERNATE_ACCESS_LABEL}=${name}"
		done
		hosts_block+="        fallthrough"$'\n'"    }"

		corefile="$(kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}')"
		BLOCK="$hosts_block" awk '
			{ print }
			! done && /^\.:53[[:space:]]*\{[[:space:]]*$/ { print ENVIRON["BLOCK"]; done = 1 }
			END { exit done ? 0 : 1 }
		' <<< "$corefile" > "$WORK_DIR/Corefile" || error "Corefile has no '.:53 {' server block"
		kubectl -n kube-system create configmap coredns --from-file="Corefile=$WORK_DIR/Corefile" \
			--dry-run=client -o yaml | kubectl apply -f -
		kubectl -n kube-system rollout restart deploy/coredns
		kubectl -n kube-system rollout status deploy/coredns --timeout=180s

		run_group "Install cert-manager and AKO" install_ako
		kubectl -n "$AKO_NAMESPACE" rollout status deploy/aerospike-kubernetes-operator --timeout=180s
		webhook=0
		for _ in $(seq 1 60); do
			if render_cluster_manifest "$SERVER_IMAGE" "$WORKERS" |
				kubectl apply --dry-run=server -f - >/dev/null 2>"$WORK_DIR/webhook.err"; then
				webhook=1
				break
			fi
			sleep 3
		done
		((webhook)) || error "operator webhook not serving after 180s: $(<"$WORK_DIR/webhook.err")"
		run_group "Apply AerospikeCluster" deploy_cluster
	) 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log"
	status=("${PIPESTATUS[@]}")
	set -e
	[ "${status[0]}" = "0" ] && [ "${status[2]}" = "0" ] || die "per-node setup failed (log: $WORK_DIR/setup.log)"

	export DISCOVERY_ENDPOINT_HOSTNAME="${per_node_names[0]}"
	export DISCOVERY_INGRESS_PORTS=""
	export DISCOVERY_SERVICE_NODE_PORTS=""
	export DISCOVERY_NODE_PORT_RANGE="$(bash -c 'source "$1"; printf %s "$NODEPORT_RANGE"' _ "$ACTION_DIR/lib.sh")"
	export DISCOVERY_KUBECONFIG=/kubeconfig
	export AEROSPIKE_ADMIN_PASSWORD="$PASSWORD"
	L3_SEED="${per_node_names[0]}:$(kubectl get aerospikecluster -A -o jsonpath='{.items[0].spec.aerospikeConfig.network.service.port}')"
fi

echo "Cluster:"
kubectl get nodes -o wide
kubectl get nodes -L "aerospike.com/configured-alternate-access-address"
kubectl -n aerospike get aerospikecluster,pods,svc -o wide
for pod in $(kubectl -n aerospike get pods -l aerospike.com/cr -o jsonpath='{.items[*].metadata.name}'); do
	echo "Rendered by the operator for $pod:"
	kubectl --request-timeout=30s -n aerospike exec "$pod" -c aerospike-server -- \
		grep -E 'access-(address|port)' /etc/aerospike/aerospike.conf || true
done

log="$WORK_DIR/mvn.log"
echo "Running SuiteDiscoveryK8s, $PROFILE profile, seed $L3_SEED"
export L3_SEED
export L3_MAVEN_OFFLINE="${L3_MAVEN_OFFLINE:-1}"
set +e
"$DOCKER" cp "$CLUSTER-control-plane:/usr/bin/kubectl" "$WORK_DIR/bin/kubectl" &&
	kind get kubeconfig --internal --name "$CLUSTER" > "$WORK_DIR/kubeconfig-internal"
rc=$?
client_args=(--rm --name "$CLIENT" --network "$KIND_NETWORK" --workdir "$ROOT_DIR"
	-v "$ROOT_DIR:$ROOT_DIR" -v "$HOME/.m2:/root/.m2"
	-v "$WORK_DIR/bin/kubectl:/usr/local/bin/kubectl:ro" -v "$WORK_DIR/kubeconfig-internal:/kubeconfig:ro"
	--add-host "$CLUSTER-control-plane:$("$DOCKER" inspect -f "{{(index .NetworkSettings.Networks \"$KIND_NETWORK\").IPAddress}}" "$CLUSTER-control-plane")")
while read -r name address; do
	[ -n "$name" ] || continue
	client_args+=(--add-host "$name:$address")
	echo "Client resolves $name to $address"
done < <(kubectl get nodes -l "aerospike.com/configured-alternate-access-address" -o \
	jsonpath='{range .items[*]}{.metadata.labels.aerospike\.com/configured-alternate-access-address} {.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}')
if [ $rc -eq 0 ]; then
	"$DOCKER" run "${client_args[@]}" \
		-e DISCOVERY_ENDPOINT_HOSTNAME \
		-e DISCOVERY_INGRESS_PORTS \
		-e DISCOVERY_SERVICE_NODE_PORTS \
		-e DISCOVERY_NODE_PORT_RANGE \
		-e DISCOVERY_KUBECONFIG \
		-e AEROSPIKE_ADMIN_PASSWORD \
		-e L3_SEED \
		-e L3_MAVEN_OFFLINE \
		"$CLIENT_IMAGE" \
		bash test/discovery/run_l3.sh --client > "$log" 2>&1
	rc=$?
fi
set -e

grep -E "L3-|CLIENT-LOG (WARN|ERROR)" "$log" || true
grep -E "^(\[[A-Z]+\] )?Tests run:.*in com\.aerospike" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
grep -E "BUILD (SUCCESS|FAILURE)" "$log" || true
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
