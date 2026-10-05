#!/usr/bin/env bash
# Run on the runner after setup-kind-ako. discovery-k8s.yaml injects
# DISCOVERY_* and AEROSPIKE_ADMIN_PASSWORD.
#
# SuiteDiscoveryK8s (CLIENT-5554, CLIENT-5555) is the K-* suite. Until that
# class is in this revision, write a skipped Surefire report after the
# published ports accept a TCP connection.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ingress_ports="${DISCOVERY_INGRESS_PORTS:?DISCOVERY_INGRESS_PORTS is required}"
node_ports="${DISCOVERY_SERVICE_NODE_PORTS:?DISCOVERY_SERVICE_NODE_PORTS is required}"
node_port_range="${DISCOVERY_NODE_PORT_RANGE:?DISCOVERY_NODE_PORT_RANGE is required}"
password="${AEROSPIKE_ADMIN_PASSWORD:?AEROSPIKE_ADMIN_PASSWORD is required}"
kubeconfig="${DISCOVERY_KUBECONFIG:?DISCOVERY_KUBECONFIG is required}"

if [[ "$node_port_range" != "30000-32767" ]]; then
  echo "NodePort range must stay 30000-32767 (got: ${node_port_range})" >&2
  exit 1
fi

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
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

suite="test/src/com/aerospike/test/SuiteDiscoveryK8s.java"
reports="test/target/surefire-reports"

if [[ ! -f "$suite" ]]; then
  mkdir -p "$reports"
  skip_message="SuiteDiscoveryK8s is not in this revision (CLIENT-5554, CLIENT-5555). K-1 through K-10 did not run."
  cat > "${reports}/TEST-com.aerospike.test.SuiteDiscoveryK8s.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="com.aerospike.test.SuiteDiscoveryK8s" tests="1" skipped="1" failures="0" errors="0" time="0">
  <testcase classname="com.aerospike.test.SuiteDiscoveryK8s" name="k-suite">
    <skipped message="${skip_message}"/>
  </testcase>
</testsuite>
EOF
  echo "::notice::${skip_message} Ingress and NodePort endpoints were reachable."
  exit 0
fi

seed_port="${ingress_ports%%,*}"
export KUBECONFIG="$kubeconfig"

mvn -B -DskipTests install -pl test -am
mvn -B -pl test -DskipTests=false \
  -DrunSuite='**/SuiteDiscoveryK8s.class' \
  "-Dargs=-h ${host}:${seed_port} -p ${seed_port} -U admin -P ${password}" \
  test
