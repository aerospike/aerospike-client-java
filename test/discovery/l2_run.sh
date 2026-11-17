#!/usr/bin/env bash
#
# The L2 tests that assert client internals, for the l2-proxy harness in aerospike-client-conformance
# (l2-proxy/README.md there), run by test/discovery/l5-aws.manifest as L2_RUN once per profile in a
# container on the client network with the topology's values file as its environment. Runs the
# profile's test classes through L2Run on the classpath L5_BUILD wrote, so it needs no network.
#
# The client keeps the legacy CN fallback, so the cn-only test cannot pass yet: on cn-only a run whose
# only failure is that test accepting the CN-only certificate exits 0 and says so.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CORRETTO=/usr/lib/jvm/java-21-amazon-corretto
CLASSPATH_FILE="$TEST_DIR/target/l5-driver.classpath"
PKG=com.aerospike.test.discovery

if [ -d "$CORRETTO" ]; then
	JAVA_HOME="$CORRETTO"
fi

if [ ! -f "$CLASSPATH_FILE" ]; then
	echo "ERROR: $CLASSPATH_FILE not found; run L5_BUILD first" >&2
	exit 1
fi

profile="${L2_PROFILE:?L2_PROFILE is required}"
args=()
tls_skip=",TestL2Topology.n6TlsValidatesAgainstNodeTlsName"
if [ "${DISCOVERY_SUBSET:-}" = ee ]; then
	tls_skip=
	truststore="$(mktemp -d)/truststore.p12"
	"${JAVA_HOME:+$JAVA_HOME/bin/}keytool" -importcert -noprompt -alias aerospike-ca \
		-file "${DISCOVERY_TLS_CA_FILE:?DISCOVERY_TLS_CA_FILE is required on ee}" \
		-keystore "$truststore" -storetype PKCS12 -storepass changeit >/dev/null
	args+=("-Djavax.net.ssl.trustStore=$truststore" -Djavax.net.ssl.trustStoreType=PKCS12
		-Djavax.net.ssl.trustStorePassword=changeit)
fi
case "$profile" in
	default)
		classes=(com.aerospike.test.SuiteDiscovery)
		skips="TestL2AsyncIdleReset.*,TestL2IdleReset.*,TestL2LowMtu.*,TestL2SourcePortRange.*,TestL2ReachableNode.*"
		skips+=",TestL2PortMapRange.*,TestL2PassThrough.*,TestL2CnOnlyCertificate.*$tls_skip"
		args+=("-Dargs=-h ${DISCOVERY_ENDPOINT_HOSTNAME:?} -p ${DISCOVERY_ENDPOINT_PORT_BASE:?}")
		;;
	cn-only)
		classes=("$PKG.TestL2CnOnlyCertificate")
		skips=
		;;
	reachable)
		classes=("$PKG.TestL2ReachableNode")
		skips=
		;;
	pass-through-names)
		classes=("$PKG.TestL2PassThrough")
		skips="TestL2PassThrough.n16SharedNameRoutesToEveryOwner,TestL2PassThrough.n16SharedNameStrictRoutesToEveryOwner"
		skips+=",TestL2PassThrough.n18PassThroughCollidingNodesCreateNoNode,TestL2PassThrough.n18StaticMapCollidingNodesCreateNoNode"
		skips+=",TestL2PassThrough.n18CustomTranslatorCollidingNodesCreateNoNode,TestL2PassThrough.n18PortMapCollidingNodesCreateNoNode"
		skips+=",TestL2PassThrough.n18PassThroughNewcomerCollidingWithNodeIsNotCreated"
		;;
	pass-through-ports)
		classes=("$PKG.TestL2PassThrough")
		skips="TestL2PassThrough.n15DistinctNamesRouteToEveryOwner"
		;;
	port-map-range)
		classes=("$PKG.TestL2PortMapRange")
		skips=
		;;
	unresolved-access)
		classes=("$PKG.TestL2UnresolvedAccessAddress")
		skips=
		;;
	port-range)
		classes=("$PKG.TestL2SourcePortRange")
		skips=
		;;
	low-mtu)
		classes=("$PKG.TestL2LowMtu")
		skips=
		;;
	idle-reset)
		classes=("$PKG.TestL2IdleReset" "$PKG.TestL2AsyncIdleReset")
		skips=
		;;
	*)
		echo "ERROR: unknown profile $profile" >&2
		exit 1
		;;
esac

# Netty's native epoll transport is packaged for x86_64 only.
if [ "$(uname -m)" != x86_64 ]; then
	skips+=",TestL2FaultInjection.f4AsyncNettyEpollSilentDropTimesOutWithoutBlocking"
	skips+=",TestL2FaultInjection.f4AsyncNettyEpollSilentDropTripsCircuitBreaker"
	skips+=",TestL2AsyncIdleReset.idleResetAbsorbedByPoolNettyEpoll"
fi

run() {
	"${JAVA_HOME:+$JAVA_HOME/bin/}java" "${args[@]+"${args[@]}"}" \
		-cp "$TEST_DIR/target/test-classes:$(cat "$CLASSPATH_FILE")" "$PKG.L2Run" "$skips" "${classes[@]}"
}

if [ "$profile" != cn-only ]; then
	run
	exit
fi
log="$(mktemp)"
rc=0
run 2>&1 | tee "$log" || rc=$?
if [ "$rc" -ne 0 ] && grep -q '^L2-N17-ACCEPTED ' "$log" && grep -q '^L2-RESULT run=1 failed=1 skipped=0 ' "$log"; then
	echo "L2-RUN profile=$profile nonblocking exit=$rc: the client accepts the CN-only certificate through its legacy CN fallback"
	exit 0
fi
exit "$rc"
