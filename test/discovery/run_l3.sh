#!/usr/bin/env bash
# shellcheck disable=SC2155 # export VAR="$(output ...)" keeps a missing output from stopping the script
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
# The tls profile builds the default profile's cluster at two pods, then runs
# .github/discovery/enable-l3-tls.sh, as the l3-tls job of discovery-k8s.yaml does: the
# AerospikeCluster serves TLS from a secret holding a server certificate whose only dNSName SAN is
# the cluster tls-name. The suite reads the CA file and the tls-name from DISCOVERY_TLS_CA_FILE and
# DISCOVERY_TLS_NAME.
#
# The nlb, nlb-per-pod and nlb-tls profiles build the cluster the l3-nlb, l3-nlb-per-pod and l3-nlb-tls
# jobs of discovery-k8s.yaml build: setup-kind-ako with alternate-access networkLoadBalancer, so no node
# is labelled, every pod advertises the AerospikeCluster's loadBalancerAlternateAccessAddress (the endpoint
# hostname) with its own Service NodePort, and no container publishes a hostPort. nlb keeps every pod on
# one kind node; nlb-per-pod sets multi-pod-per-host false, which runs one pod per kind worker with a spare
# worker, at two pods; nlb-tls runs enable-l3-tls.sh on nlb at two pods with as-endpoint-tls.test as
# loadBalancerTLSAlternateAccessAddress. They need an AKO that offers networkLoadBalancer: set L3_AKO_VERSION
# and L3_CLUSTER_CHART_VERSION to the values the workflow sets.
#
# While the suite runs, .github/discovery/reregister-kind-nodes.sh restarts kubelet in any kind node whose
# Node object has been deleted for 60 seconds, which kind does not register again on its own.
#
# The client dials each kind node's address, as the workflow's client does on a Linux runner. That
# address is not reachable from a macOS host, so the client runs in a container on the kind network,
# where each labelled hostname resolves to its node. The action writes the endpoint hostname into
# /etc/hosts; this script never does. On a cluster with no labelled node, the endpoint hostname, and
# the TLS endpoint hostname on nlb-tls, resolve to the node address CoreDNS has for them.
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
#   L3_PROFILE                default, per-node, tls, nlb, nlb-per-pod or nlb-tls (default: default)
#   L3_AKO_VERSION            setup-kind-ako ako-version (default: latest)
#   L3_CLUSTER_CHART_VERSION  setup-kind-ako cluster-chart-version (default: empty, the resolved ako-version)
#   L3_KIND_CLUSTER           kind cluster name (default: discovery-l3)
#   L3_CLUSTER_SIZE           initial AerospikeCluster size on every profile but per-node, 1-8
#                             (default: the action's default, 1, on default and nlb; 2 on tls, nlb-per-pod
#                             and nlb-tls)
#   L3_PER_NODE_WORKERS       kind workers, and Aerospike pods, on the per-node profile, 2-8 (default: 2)
#   L3_PER_NODE_DOMAIN        domain of the per-node hostnames (default: as-endpoint.test)
#   L3_SERVER_IMAGE           server image of the AerospikeCluster, as repository:tag (default: the action's
#                             server-image default). It must already be in the local image store; the script loads
#                             it into the kind cluster, so a private image needs no pull secret. The operator's
#                             webhook rejects a tag whose version it has no config schema for (AKO 4.5.0: 8.1.2 at most)
#   L3_UNRESOLVED_ENDPOINT    set to 1 on the default profile to restore the CoreDNS configuration the action
#                             found, without its entry for the endpoint hostname, before any Aerospike pod starts.
#                             Before the tests run, a pod in the cluster looks the hostname up and the run fails
#                             unless the answer is NXDOMAIN. Runs TestK8sPassThrough only. A server that resolves
#                             access addresses at startup does not start
#   KIND_EXPERIMENTAL_PROVIDER passed to kind; podman also makes this script use the podman CLI
#   SHARED_WORKFLOWS_DIR      local aerospike/shared-workflows clone to read the action from instead
#                             of downloading it
#   L3_SHARED_WORKFLOWS_REF   commit to read from SHARED_WORKFLOWS_DIR, or worktree to read its working
#                             tree (default: SHARED_WORKFLOWS_COMMIT)
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
		"-DrunSuite=${L3_SUITE:?L3_SUITE is required}" \
		"-Dargs=-h ${seed} -p ${seed##*:} -U admin -P ${password}" \
		test
}

if [ "${1:-}" = "--client" ]; then
	cd "$ROOT_DIR"
	run_suite
	exit $?
fi

SHARED_WORKFLOWS_COMMIT=a2c89aad687949ed5e1936476fa5ec7bc833159d
SHARED_WORKFLOWS_REF="${L3_SHARED_WORKFLOWS_REF:-$SHARED_WORKFLOWS_COMMIT}"
ACTION_PATH=.github/actions/setup-kind-ako
CLIENT_IMAGE=maven:3-ibm-semeru-21-noble@sha256:0ad9f1918f4a30f89713a03e39659ef3135a2a08b257a90168a6033335a48bcb
PROFILE="${L3_PROFILE:-default}"
CLUSTER="${L3_KIND_CLUSTER:-discovery-l3}"
WORKERS="${L3_PER_NODE_WORKERS:-2}"
UNRESOLVED="${L3_UNRESOLVED_ENDPOINT:-0}"
LOOKUP_IMAGE=busybox:1.36.1
PER_NODE_DOMAIN="${L3_PER_NODE_DOMAIN:-as-endpoint.test}"
TLS_ENDPOINT_HOSTNAME=as-endpoint-tls.test
WORK_DIR="$TEST_DIR/target/discovery-l3"
ACTION_DIR="$WORK_DIR/action"
KIND_NETWORK=kind
CLIENT="$CLUSTER-client"

ALTERNATE_ACCESS=configuredIP
MULTI_POD_PER_HOST=true
TLS=0
case "$PROFILE" in
	default|per-node) ;;
	tls) TLS=1 ;;
	nlb) ALTERNATE_ACCESS=networkLoadBalancer ;;
	nlb-per-pod) ALTERNATE_ACCESS=networkLoadBalancer MULTI_POD_PER_HOST=false ;;
	nlb-tls) ALTERNATE_ACCESS=networkLoadBalancer TLS=1 ;;
	*) die "L3_PROFILE must be default, per-node, tls, nlb, nlb-per-pod or nlb-tls (got: $PROFILE)" ;;
esac
AKO_VERSION="${L3_AKO_VERSION:-latest}"
CLUSTER_CHART_VERSION="${L3_CLUSTER_CHART_VERSION:-}"
[[ "$CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "L3_KIND_CLUSTER must be lower-case letters, digits and dashes (got: $CLUSTER)"
[[ "$WORKERS" =~ ^[2-8]$ ]] || die "L3_PER_NODE_WORKERS must be 2-8 (got: $WORKERS)"
[[ "$PER_NODE_DOMAIN" =~ ^[a-z0-9.-]+$ ]] || die "L3_PER_NODE_DOMAIN must be a DNS name (got: $PER_NODE_DOMAIN)"
[[ "$UNRESOLVED" =~ ^[01]$ ]] || die "L3_UNRESOLVED_ENDPOINT must be 0 or 1 (got: $UNRESOLVED)"
[ "$UNRESOLVED" = "0" ] || [ "$PROFILE" = "default" ] || die "L3_UNRESOLVED_ENDPOINT=1 needs the default profile"

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
mkdir -p "$ACTION_DIR/templates" "$WORK_DIR/bin" "$WORK_DIR/helm"

for f in entrypoint.sh action.yaml templates/aerospike-cluster-values.yaml templates/aerospike-cluster-tls-values.yaml \
	templates/ingress-values.yaml templates/rbac.yaml; do
	if [ -n "${SHARED_WORKFLOWS_DIR:-}" ] && [ "$SHARED_WORKFLOWS_REF" = "worktree" ]; then
		cp "$SHARED_WORKFLOWS_DIR/$ACTION_PATH/$f" "$ACTION_DIR/$f" || die "$f not found in $SHARED_WORKFLOWS_DIR"
	elif [ -n "${SHARED_WORKFLOWS_DIR:-}" ]; then
		git -C "$SHARED_WORKFLOWS_DIR" show "$SHARED_WORKFLOWS_REF:$ACTION_PATH/$f" > "$ACTION_DIR/$f" ||
			die "$f not found at $SHARED_WORKFLOWS_REF in $SHARED_WORKFLOWS_DIR"
	else
		curl -sfL "https://raw.githubusercontent.com/aerospike/shared-workflows/$SHARED_WORKFLOWS_COMMIT/$ACTION_PATH/$f" \
			-o "$ACTION_DIR/$f" || die "failed to download $ACTION_PATH/$f at $SHARED_WORKFLOWS_COMMIT"
	fi
done

sed '/| sudo tee -a \/etc\/hosts/d' "$ACTION_DIR/entrypoint.sh" > "$ACTION_DIR/entrypoint-local.sh"
if grep -nE 'sudo|/etc/hosts' "$ACTION_DIR/entrypoint-local.sh"; then
	die "entrypoint.sh at $SHARED_WORKFLOWS_REF still writes /etc/hosts after removing its hosts step"
fi
sed '/^case \${1-} in$/,$d' "$ACTION_DIR/entrypoint-local.sh" > "$ACTION_DIR/functions.sh"
grep -q '^cmd_setup()' "$ACTION_DIR/functions.sh" || die "entrypoint.sh at $SHARED_WORKFLOWS_REF has no cmd_setup"

input_default() {
	sed -n "/^  $1:/,/default:/s/^ *default: *//p" "$ACTION_DIR/action.yaml"
}

KIND_IMAGE="$(input_default kind-node-image)"
SERVER_IMAGE="$(input_default server-image)"
AKO_HELM_REPO="$(input_default ako-helm-repo)"
[ -n "$KIND_IMAGE" ] || die "no kind-node-image default in action.yaml"
[ -n "$SERVER_IMAGE" ] || die "no server-image default in action.yaml"
[ -n "$AKO_HELM_REPO" ] || die "no ako-helm-repo default in action.yaml at $SHARED_WORKFLOWS_REF"
if [ -n "${L3_SERVER_IMAGE:-}" ]; then
	SERVER_IMAGE="$L3_SERVER_IMAGE"
	"$DOCKER" image inspect "$SERVER_IMAGE" >/dev/null 2>&1 || die "L3_SERVER_IMAGE $SERVER_IMAGE is not in the local image store; pull it first"
fi
DEFAULT_SIZE="$(sed -n '/^  cluster-size:/,/default:/s/^ *default: *"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p' "$ACTION_DIR/action.yaml")"
[ "$TLS" = "1" ] || [ "$MULTI_POD_PER_HOST" = "false" ] && DEFAULT_SIZE=2
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

WATCHER=""
cleanup() {
	[ -z "$WATCHER" ] || kill "$WATCHER" 2>/dev/null || true
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
export AKO_HELM_REPO AKO_VERSION ALTERNATE_ACCESS CLUSTER_CHART_VERSION MULTI_POD_PER_HOST
export ENABLE_TLS=false KIND_NODE_IMAGE="$KIND_IMAGE"
FEATURES_CONTENT="$(cat "$FEATURES_CONF_PATH")"

output() {
	grep -E "^$1=" "$2" | head -1 | cut -d= -f2-
}

echo "Preparing setup-kind-ako (shared-workflows $SHARED_WORKFLOWS_REF), $PROFILE profile, ako-version $AKO_VERSION," \
	"cluster-chart-version ${CLUSTER_CHART_VERSION:-(ako-version)}, alternate-access $ALTERNATE_ACCESS," \
	"multi-pod-per-host $MULTI_POD_PER_HOST"
ADMIN_PASSWORD="" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" SERVER_IMAGE="$SERVER_IMAGE" \
	GITHUB_OUTPUT="$WORK_DIR/prepare.out" bash "$ACTION_DIR/entrypoint-local.sh" prepare >/dev/null ||
	die "setup-kind-ako prepare failed"
PASSWORD="$(output admin-password "$WORK_DIR/prepare.out")"

kind_args=()
if [ "$PROFILE" = "per-node" ]; then
	{
		printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n'
		for i in $(seq 1 "$WORKERS"); do
			printf '  - role: worker\n'
		done
	} > "$WORK_DIR/kind-config-per-node.yaml"
	kind_args=(--config "$WORK_DIR/kind-config-per-node.yaml")
elif [ -n "$(output kind-config "$WORK_DIR/prepare.out")" ]; then
	kind_args=(--config "$(output kind-config "$WORK_DIR/prepare.out")")
fi

echo "Creating kind cluster $CLUSTER ($KIND_IMAGE)"
CREATED=1
kind create cluster --name "$CLUSTER" --image "$KIND_IMAGE" "${kind_args[@]}" --wait 180s \
	--kubeconfig "$KUBECONFIG" || die "kind create cluster failed"

if [ -n "${L3_SERVER_IMAGE:-}" ]; then
	echo "Loading $SERVER_IMAGE into kind cluster $CLUSTER"
	"$DOCKER" save -o "$WORK_DIR/server-image.tar" "$SERVER_IMAGE" || die "cannot save $SERVER_IMAGE"
	kind load image-archive "$WORK_DIR/server-image.tar" --name "$CLUSTER" || die "cannot load $SERVER_IMAGE into $CLUSTER"
	rm -f "$WORK_DIR/server-image.tar"
fi

if [ "$PROFILE" != "per-node" ] && [ "$UNRESOLVED" = "1" ]; then
	# Not in an && or || list, so set -e holds inside the subshell.
	set +e
	(
		export GITHUB_OUTPUT="$WORK_DIR/setup.out"
		# shellcheck source=/dev/null
		source "$ACTION_DIR/functions.sh"
		# shellcheck disable=SC2034 # read by the action's functions
		ADMIN_PASSWORD="$PASSWORD"

		eval "action_$(declare -f prepare_node)"
		# shellcheck disable=SC2329 # called by the action's cmd_setup
		prepare_node() {
			kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}' > "$WORK_DIR/Corefile.found"
			action_prepare_node
			kubectl -n kube-system create configmap coredns --from-file="Corefile=$WORK_DIR/Corefile.found" \
				--dry-run=client -o yaml | kubectl apply -f -
			kubectl -n kube-system rollout restart deploy/coredns
			kubectl -n kube-system rollout status deploy/coredns --timeout=180s
			if kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}' | grep -F "$ENDPOINT_HOSTNAME"; then
				error "CoreDNS still names $ENDPOINT_HOSTNAME"
			fi
			echo "CoreDNS has no entry for $ENDPOINT_HOSTNAME; no Aerospike pod has started"
		}
		cmd_setup
	) 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log"
	status=("${PIPESTATUS[@]}")
	set -e
	[ "${status[0]}" = "0" ] && [ "${status[2]}" = "0" ] || die "setup-kind-ako setup failed (log: $WORK_DIR/setup.log)"
fi

if [ "$PROFILE" != "per-node" ]; then
	if [ "$UNRESOLVED" = "0" ]; then
		ADMIN_PASSWORD="$PASSWORD" CLUSTER_SIZE="$CLUSTER_SIZE" FEATURES_CONTENT="$FEATURES_CONTENT" \
			SERVER_IMAGE="$SERVER_IMAGE" GITHUB_OUTPUT="$WORK_DIR/setup.out" \
			bash "$ACTION_DIR/entrypoint-local.sh" setup 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log" ||
			die "setup-kind-ako setup failed (log: $WORK_DIR/setup.log)"
	fi

	export DISCOVERY_ENDPOINT_HOSTNAME="$(output endpoint-hostname "$WORK_DIR/setup.out")"
	export DISCOVERY_INGRESS_PORTS="$(output ingress-ports "$WORK_DIR/setup.out")"
	export DISCOVERY_SERVICE_NODE_PORTS="$(output service-node-ports "$WORK_DIR/setup.out")"
	export DISCOVERY_KUBECONFIG=/kubeconfig
	export AEROSPIKE_ADMIN_PASSWORD="$PASSWORD"
	L3_SEED="$DISCOVERY_ENDPOINT_HOSTNAME:${DISCOVERY_INGRESS_PORTS%%,*}"
else
	# Not in an && or || list, so set -e holds inside the subshell.
	set +e
	(
		# shellcheck source=/dev/null
		source "$ACTION_DIR/functions.sh"
		# shellcheck disable=SC2034 # read by the action's functions
		ADMIN_PASSWORD="$PASSWORD"
		CLUSTER_SIZE="$WORKERS"
		mkdir -p "$WORK_DIR"

		eval "action_$(declare -f render_cluster_manifest)"
		# shellcheck disable=SC2329 # called by the action's deploy_cluster
		render_cluster_manifest() {
			action_render_cluster_manifest
			sed 's/^    multiPodPerHost: true$/    multiPodPerHost: false/' "$WORK_DIR/aerospike-cluster.yaml" \
				> "$WORK_DIR/aerospike-cluster.per-node.yaml"
			mv "$WORK_DIR/aerospike-cluster.per-node.yaml" "$WORK_DIR/aerospike-cluster.yaml"
			grep -q '^    multiPodPerHost: false$' "$WORK_DIR/aerospike-cluster.yaml" ||
				error "AerospikeCluster manifest has no multiPodPerHost to set to false"
		}

		mapfile -t workers < <(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
			-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
		((${#workers[@]} == WORKERS)) || error "expected $WORKERS kind workers, found ${workers[*]}"

		hosts_block="    hosts {"$'\n'
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
		run_group "Apply AerospikeCluster" deploy_cluster
	) 2>&1 | sed '/::add-mask::/d' | tee "$WORK_DIR/setup.log"
	status=("${PIPESTATUS[@]}")
	set -e
	[ "${status[0]}" = "0" ] && [ "${status[2]}" = "0" ] || die "per-node setup failed (log: $WORK_DIR/setup.log)"

	export DISCOVERY_ENDPOINT_HOSTNAME="${per_node_names[0]}"
	export DISCOVERY_INGRESS_PORTS=""
	export DISCOVERY_SERVICE_NODE_PORTS=""
	export DISCOVERY_KUBECONFIG=/kubeconfig
	export AEROSPIKE_ADMIN_PASSWORD="$PASSWORD"
	L3_SEED="${per_node_names[0]}:$(kubectl get aerospikecluster -A -o jsonpath='{.items[0].spec.aerospikeConfig.network.service.port}')"
fi

if [ "$TLS" = "1" ]; then
	if [ "$PROFILE" = "nlb-tls" ]; then
		export DISCOVERY_TLS_ENDPOINT_HOSTNAME="$TLS_ENDPOINT_HOSTNAME"
	fi
	L3_TLS_DIR="$WORK_DIR/tls" GITHUB_OUTPUT="$WORK_DIR/tls.out" \
		bash "$ROOT_DIR/.github/discovery/enable-l3-tls.sh" 2>&1 | tee "$WORK_DIR/tls.log" ||
		die "enable-l3-tls.sh failed (log: $WORK_DIR/tls.log)"
	export DISCOVERY_TLS_CA_FILE="$(output ca-file "$WORK_DIR/tls.out")"
	export DISCOVERY_TLS_NAME="$(output tls-name "$WORK_DIR/tls.out")"
	export DISCOVERY_TLS_ENDPOINT_HOSTNAME="$(output tls-endpoint-hostname "$WORK_DIR/tls.out")"
fi

echo "Cluster:"
kubectl get nodes -o wide
kubectl get nodes -L "aerospike.com/configured-alternate-access-address"
kubectl -n aerospike get aerospikecluster,pods,svc -o wide
kubectl -n aerospike get aerospikecluster -o jsonpath='{range .items[*]}aerospikeNetworkPolicy {.spec.aerospikeNetworkPolicy}{"\n"}{end}'
for pod in $(kubectl -n aerospike get pods -l aerospike.com/cr -o jsonpath='{.items[*].metadata.name}'); do
	echo "Rendered by the operator for $pod:"
	kubectl --request-timeout=30s -n aerospike exec "$pod" -c aerospike-server -- \
		grep -E 'access-(address|port)' /etc/aerospike/aerospike.conf || true
done

if [ "$UNRESOLVED" = "1" ]; then
	lookup=l3-endpoint-lookup
	kubectl -n aerospike run "$lookup" --image="$LOOKUP_IMAGE" --restart=Never --command -- \
		nslookup "$DISCOVERY_ENDPOINT_HOSTNAME." >/dev/null || die "cannot start the lookup pod"
	phase=""
	for _ in $(seq 1 90); do
		phase="$(kubectl -n aerospike get pod "$lookup" -o jsonpath='{.status.phase}')"
		[ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] && break
		sleep 2
	done
	kubectl -n aerospike logs "$lookup" > "$WORK_DIR/lookup.log" 2>&1 || true
	echo "nslookup $DISCOVERY_ENDPOINT_HOSTNAME. from pod $lookup on $(kubectl -n aerospike get pod "$lookup" \
		-o jsonpath='{.spec.nodeName}') ($phase):"
	kubectl -n aerospike delete pod "$lookup" --wait=false >/dev/null || true
	cat "$WORK_DIR/lookup.log"
	grep -q "NXDOMAIN" "$WORK_DIR/lookup.log" || die "$DISCOVERY_ENDPOINT_HOSTNAME is not NXDOMAIN in the cluster"
	for pod in $(kubectl -n aerospike get pods -l aerospike.com/cr -o jsonpath='{.items[*].metadata.name}'); do
		rc=0
		kubectl --request-timeout=30s -n aerospike exec "$pod" -c aerospike-server -- \
			getent hosts "$DISCOVERY_ENDPOINT_HOSTNAME" || rc=$?
		echo "getent hosts $DISCOVERY_ENDPOINT_HOSTNAME in $pod: exit $rc"
		[ "$rc" = "2" ] || die "$DISCOVERY_ENDPOINT_HOSTNAME resolves in $pod or the lookup failed"
	done
fi

log="$WORK_DIR/mvn.log"
export L3_SEED
case "$PROFILE" in
	default|nlb|nlb-per-pod)
		if [ "$UNRESOLVED" = "1" ]; then
			L3_SUITE='**/TestK8sPassThrough.class'
		else
			L3_SUITE='**/SuiteDiscoveryK8s.class'
		fi
		;;
	per-node) L3_SUITE='**/TestK8sPassThrough.class' ;;
	tls|nlb-tls) L3_SUITE='**/TestK8sTlsName.class' ;;
esac
export L3_SUITE
echo "Running $L3_SUITE, $PROFILE profile, seed $L3_SEED"
export L3_MAVEN_OFFLINE="${L3_MAVEN_OFFLINE:-1}"
set +e
"$DOCKER" cp "$CLUSTER-control-plane:/usr/bin/kubectl" "$WORK_DIR/bin/kubectl" &&
	kind get kubeconfig --internal --name "$CLUSTER" > "$WORK_DIR/kubeconfig-internal"
rc=$?
client_args=(--rm --name "$CLIENT" --network "$KIND_NETWORK" --workdir "$ROOT_DIR"
	-v "$ROOT_DIR:$ROOT_DIR" -v "$HOME/.m2:/root/.m2"
	-v "$WORK_DIR/bin/kubectl:/usr/local/bin/kubectl:ro" -v "$WORK_DIR/kubeconfig-internal:/kubeconfig:ro"
	--add-host "$CLUSTER-control-plane:$("$DOCKER" inspect -f "{{(index .NetworkSettings.Networks \"$KIND_NETWORK\").IPAddress}}" "$CLUSTER-control-plane")")
mapfile -t endpoints < <(kubectl get nodes -l "aerospike.com/configured-alternate-access-address" -o \
	jsonpath='{range .items[*]}{.metadata.labels.aerospike\.com/configured-alternate-access-address} {.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}')
if [ ${#endpoints[@]} -eq 0 ]; then
	corefile="$(kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}')"
	for name in "$DISCOVERY_ENDPOINT_HOSTNAME" ${DISCOVERY_TLS_ENDPOINT_HOSTNAME:+"$DISCOVERY_TLS_ENDPOINT_HOSTNAME"}; do
		address="$(awk -v name="$name" 'NF == 2 && $2 == name { print $1; exit }' <<< "$corefile")"
		[ -n "$address" ] || die "no node labelled and no CoreDNS hosts entry for $name"
		endpoints+=("$name $address")
	done
fi
for endpoint in "${endpoints[@]}"; do
	read -r name address <<< "$endpoint"
	[ -n "$name" ] || continue
	client_args+=(--add-host "$name:$address")
	echo "Client resolves $name to $address"
done
if [ $rc -eq 0 ]; then
	KIND_EXPERIMENTAL_PROVIDER="${KIND_EXPERIMENTAL_PROVIDER:-}" \
		bash "$ROOT_DIR/.github/discovery/reregister-kind-nodes.sh" "$CLUSTER" > "$WORK_DIR/nodes.log" 2>&1 &
	WATCHER=$!
	"$DOCKER" run "${client_args[@]}" \
		-e DISCOVERY_ENDPOINT_HOSTNAME \
		-e DISCOVERY_INGRESS_PORTS \
		-e DISCOVERY_SERVICE_NODE_PORTS \
		-e DISCOVERY_KUBECONFIG \
		-e AEROSPIKE_ADMIN_PASSWORD \
		-e DISCOVERY_TLS_CA_FILE \
		-e DISCOVERY_TLS_NAME \
		-e DISCOVERY_TLS_ENDPOINT_HOSTNAME \
		-e L3_SEED \
		-e L3_SUITE \
		-e L3_MAVEN_OFFLINE \
		"$CLIENT_IMAGE" \
		bash test/discovery/run_l3.sh --client > "$log" 2>&1
	rc=$?
	kill "$WATCHER" 2>/dev/null
	WATCHER=""
	cat "$WORK_DIR/nodes.log"
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
