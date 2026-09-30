#!/usr/bin/env bash
# Run inside the L2 client container. The discovery workflow injects
# DISCOVERY_* and TOXIPROXY_*. DISCOVERY_SUBSET is ce or ee.
#
# SuiteDiscovery (CLIENT-5550) must honor -Ddiscovery.subset:
#   ce - community topology tests, excluding N-6 and N-14
#   ee - N-6 and N-14
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

# A cluster that never came up must fail here, not look like a skipped suite.
IFS=',' read -r -a port_list <<< "$ports"
if ((${#port_list[@]} == 0)); then
  echo "DISCOVERY_ENDPOINT_PORTS is empty" >&2
  exit 1
fi
[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}
for port in "${port_list[@]}"; do
  [[ "$port" =~ ^[0-9]+$ ]] || {
    echo "Refusing unexpected endpoint port: ${port}" >&2
    exit 1
  }
  echo "Checking TCP ${host}:${port}"
  timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
done

suite="test/src/com/aerospike/test/SuiteDiscovery.java"
reports="test/target/surefire-reports"

if [[ ! -f "$suite" ]]; then
  mkdir -p "$reports"
  if [[ "$subset" == "ee" ]]; then
    skip_message="SuiteDiscovery is not in this revision (CLIENT-5550). EE subset did not run: N-6, N-14."
  else
    skip_message="SuiteDiscovery is not in this revision (CLIENT-5550). CE discovery subset did not run."
  fi
  cat > "${reports}/TEST-com.aerospike.test.SuiteDiscovery.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="com.aerospike.test.SuiteDiscovery" tests="1" skipped="1" failures="0" errors="0" time="0">
  <testcase classname="com.aerospike.test.SuiteDiscovery" name="subset-${subset}">
    <skipped message="${skip_message}"/>
  </testcase>
</testsuite>
EOF
  echo "::notice::${skip_message} Proxy ports were reachable."
  exit 0
fi

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

args="--endpoint-hostname ${host} --endpoint-port-base ${port_base}"
if [[ "$subset" == "ee" ]]; then
  args+=" --tlsEnable"
fi

mvn -B -pl test -DskipTests=false \
  -DrunSuite='**/SuiteDiscovery.class' \
  "-Ddiscovery.subset=${subset}" \
  "-Dargs=${args}" \
  test
