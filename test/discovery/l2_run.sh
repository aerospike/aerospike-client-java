#!/usr/bin/env bash
#
# The L2 tests that assert client internals, for the l2-proxy harness in aerospike-client-conformance
# (l2-proxy/README.md there), run by test/discovery/l5-aws.manifest as L2_RUN once per profile in a
# container on the client network with the topology's values file as its environment. Runs the
# profile's test classes through L2Run on the classpath L5_BUILD wrote, so it needs no network.
#
# The tests that start nodes, use TLS or a DNS server, and the unresolved-access profile run in
# .github/workflows/discovery-l2.yaml instead.

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
case "$profile" in
	default)
		classes=(com.aerospike.test.SuiteDiscovery)
		skips="TestL2AsyncIdleReset.*,TestL2IdleReset.*,TestL2LowMtu.*,TestL2SourcePortRange.*,TestL2ReachableNode.*"
		skips+=",TestL2PortMapRange.*,TestL2PassThrough.*,TestL2CnOnlyCertificate.*"
		skips+=",TestL2Topology.n4DnsProviderSeedsThroughEndpoint,TestL2Topology.n6TlsValidatesAgainstNodeTlsName"
		args=("-Dargs=-h ${DISCOVERY_ENDPOINT_HOSTNAME:?} -p ${DISCOVERY_ENDPOINT_PORT_BASE:?}")
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
		skips+=",TestL2PassThrough.n18PassThroughCollidingNodesCreateNoNode,TestL2PassThrough.n18StaticMapCollidingNodesCreateNoNode"
		skips+=",TestL2PassThrough.n18PassThroughNewcomerCollidingWithNodeIsNotCreated"
		;;
	port-map-range)
		echo "L2-RUN profile=$profile none: TestL2PortMapRange starts a node, so discovery-l2.yaml runs it"
		exit 0
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

exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" "${args[@]+"${args[@]}"}" \
	-cp "$TEST_DIR/target/test-classes:$(cat "$CLASSPATH_FILE")" "$PKG.L2Run" "$skips" "${classes[@]}"
