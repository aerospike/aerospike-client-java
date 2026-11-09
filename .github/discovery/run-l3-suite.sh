#!/usr/bin/env bash
# Run on the runner after setup-kind-ako and, in the TLS job, enable-l3-tls.sh.
# discovery-k8s.yaml injects DISCOVERY_* and AEROSPIKE_ADMIN_PASSWORD, which
# SuiteDiscoveryK8s reads. L3_SUITE selects the suite (default SuiteDiscoveryK8s).
# test/discovery/run_l3.sh sets the same variables for a local run.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ingress_ports="${DISCOVERY_INGRESS_PORTS:?DISCOVERY_INGRESS_PORTS is required}"
node_ports="${DISCOVERY_SERVICE_NODE_PORTS:?DISCOVERY_SERVICE_NODE_PORTS is required}"
password="${AEROSPIKE_ADMIN_PASSWORD:?AEROSPIKE_ADMIN_PASSWORD is required}"
kubeconfig="${DISCOVERY_KUBECONFIG:?DISCOVERY_KUBECONFIG is required}"
suite="${L3_SUITE:-**/SuiteDiscoveryK8s.class}"

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}
[[ "$suite" =~ ^[A-Za-z0-9*/.,_-]+$ ]] || {
  echo "Refusing unexpected suite: ${suite}" >&2
  exit 1
}
[[ "$password" =~ ^[A-Za-z0-9]+$ ]] || {
  echo "Refusing unexpected admin password characters" >&2
  exit 1
}
[[ -f "$kubeconfig" ]] || {
  echo "kubeconfig does not exist: ${kubeconfig}" >&2
  exit 1
}
if [[ -n "${DISCOVERY_TLS_CA_FILE:-}${DISCOVERY_TLS_NAME:-}" ]]; then
  [[ -f "${DISCOVERY_TLS_CA_FILE:-}" ]] || {
    echo "DISCOVERY_TLS_CA_FILE does not exist: ${DISCOVERY_TLS_CA_FILE:-}" >&2
    exit 1
  }
  [[ "${DISCOVERY_TLS_NAME:-}" =~ ^[A-Za-z0-9.-]+$ ]] || {
    echo "Refusing unexpected tls-name: ${DISCOVERY_TLS_NAME:-}" >&2
    exit 1
  }
fi

check_ports() {
  local label=$1
  local ports=$2
  local port
  local -a port_list
  IFS=',' read -r -a port_list <<< "$ports"
  if ((${#port_list[@]} == 0)); then
    echo "${label} is empty" >&2
    exit 1
  fi
  for port in "${port_list[@]}"; do
    [[ "$port" =~ ^[0-9]+$ ]] || {
      echo "Refusing unexpected ${label} port: ${port}" >&2
      exit 1
    }
    echo "Checking TCP ${host}:${port}"
    timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
  done
}

check_ports "ingress" "$ingress_ports"
check_ports "service node" "$node_ports"

seed_port="${ingress_ports%%,*}"
export KUBECONFIG="$kubeconfig"

mvn -B -DskipTests install -pl test -am
mvn -B -pl test -DskipTests=false \
  "-DrunSuite=${suite}" \
  "-Dargs=-h ${host}:${seed_port} -p ${seed_port} -U admin -P ${password}" \
  test
