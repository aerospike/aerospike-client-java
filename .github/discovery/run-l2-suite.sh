#!/usr/bin/env bash
# Run inside the L2 client container. The discovery workflow injects
# DISCOVERY_* and TOXIPROXY_*. DISCOVERY_SUBSET is ce or ee.
#
# SuiteDiscovery is the discovery config suite. Seed it through the proxy.
# Do not pass --endpoint-hostname or --endpoint-port-base: SuiteDiscovery
# asserts that those flags are rejected until a client build applies them.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
port_base="${DISCOVERY_ENDPOINT_PORT_BASE:?DISCOVERY_ENDPOINT_PORT_BASE is required}"
subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"

case "$subset" in
  ce|ee) ;;
  *)
    echo "DISCOVERY_SUBSET must be ce or ee (got: ${subset})" >&2
    exit 1
    ;;
esac

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}

if [[ "$subset" == "ee" ]]; then
  tls_ports="${DISCOVERY_TLS_ENDPOINT_PORTS:?DISCOVERY_TLS_ENDPOINT_PORTS is required}"
  seed_port="${tls_ports%%,*}"
  check_ports="$tls_ports"
  args="-h ${host}:aerospike-tls -p ${seed_port} --tlsEnable"
else
  seed_port="$port_base"
  check_ports="$ports"
  args="-h ${host} -p ${seed_port}"
fi

# A cluster that never came up must fail here, not look like a skipped suite.
IFS=',' read -r -a port_list <<< "$check_ports"
if ((${#port_list[@]} == 0)); then
  echo "No proxy ports to check" >&2
  exit 1
fi
for port in "${port_list[@]}"; do
  [[ "$port" =~ ^[0-9]+$ ]] || {
    echo "Refusing unexpected endpoint port: ${port}" >&2
    exit 1
  }
  echo "Checking TCP ${host}:${port}"
  timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
done

if [[ -n "${AEROSPIKE_CA_CERT:-}" ]]; then
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "JAVA_HOME is required to import ${AEROSPIKE_CA_CERT}" >&2
    exit 1
  fi
  keytool -import -trustcacerts \
    -keystore "${JAVA_HOME}/lib/security/cacerts" \
    -storepass changeit -noprompt \
    -alias aerospike-ca \
    -file "${AEROSPIKE_CA_CERT}"
fi

suite="${L2_SUITE:-**/SuiteDiscovery.class}"
[[ "$suite" =~ ^[A-Za-z0-9*/._-]+$ ]] || {
  echo "Refusing unexpected suite: ${suite}" >&2
  exit 1
}

mvn -B -pl test -DskipTests=false \
  "-DrunSuite=${suite}" \
  "-Dargs=${args}" \
  test
