#!/usr/bin/env bash
#
# Server certificate name matching integration cases.
#
# For each case: generate a CA and the case's server certificate with openssl, start one
# Aerospike Enterprise container serving TLS with that certificate, run SuiteTlsMatching once
# (sync and async NETTY_NIO), then remove the container. Certificates live under
# test/target/tls-matching and are regenerated on every run. Certificate generation, file names
# and server config follow shared-workflows setup-aerospike-server; only the CN and SANs vary.
#
# Required environment:
#   AEROSPIKE_IMAGE             Enterprise server image, e.g. aerospike/aerospike-server-enterprise:8.1
#   AEROSPIKE_FEATURES_FILE     feature-key file, or
#   AEROSPIKE_FEATURES_CONTENT  feature-key file content (e.g. from a CI secret)
# Optional environment:
#   TLS_MATCHING_PORT           host port mapped to the container TLS port 4333 (default 4433)
#   TLS_MATCHING_SKIP_BUILD     set to 1 to skip installing the client module first
#   TLS_MATCHING_MAVEN_OFFLINE  set to 0 to let Maven resolve dependencies online (default 1)
#
# Usage: test/tls/run_tls_matching.sh [case ...]   (no arguments runs every case)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/tls-matching"
PORT="${TLS_MATCHING_PORT:-4433}"
PREFIX="tls-matching-$$"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
command -v openssl >/dev/null 2>&1 || die "openssl not found"
[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
if [ -n "${AEROSPIKE_FEATURES_FILE:-}" ]; then
	[ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ] || die "set only one of AEROSPIKE_FEATURES_FILE and AEROSPIKE_FEATURES_CONTENT"
	[ -r "$AEROSPIKE_FEATURES_FILE" ] || die "AEROSPIKE_FEATURES_FILE '$AEROSPIKE_FEATURES_FILE' is not readable"
elif [ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ]; then
	die "AEROSPIKE_FEATURES_FILE or AEROSPIKE_FEATURES_CONTENT must be set"
fi

MVN=(mvn)
[ "${TLS_MATCHING_MAVEN_OFFLINE:-1}" = "1" ] && MVN+=(-o)

STARTED=()

cleanup() {
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	[ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ] || rm -f "$WORK_DIR/features.conf"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# name|tlsName|subject CN|subjectAltName ("-" for no extension)|expect|warning|bootstrap tlsName|revoke
# Reject certs carry the bootstrap tlsName as a dNSName so netty is reached without the CN fallback (except cn_case_only).
CASES=(
	"dns_exact|node.example.com|server-cn|DNS:node.example.com|accept|false||no"
	"dns_case|node.example.com|server-cn|DNS:Node.Example.COM|accept|false||no"
	"ipv4_ip_san|127.0.0.1|server-cn|IP:127.0.0.1|accept|false||no"
	"ipv6_ip_san|2001:db8::1|server-cn|IP:2001:db8::1|accept|false||no"
	"ipv6_ip_san_equivalent|2001:0db8:0:0:0:0:0:1|server-cn|IP:2001:db8::1|accept|false||no"
	"ipv6_bracketed|[2001:db8::1]|server-cn|IP:2001:db8::1|accept|false||no"
	"ipv6_scoped|fe80::1%en0|server-cn|IP:fe80::1,DNS:server-cn|reject|false|server-cn|no"
	"ipv4_leading_zero|127.000.0.1|server-cn|IP:127.0.0.1,DNS:server-cn|reject|false|server-cn|no"
	"dns_only_as_ip|localhost|server-cn|IP:127.0.0.1,DNS:server-cn|reject|false|server-cn|no"
	"wildcard_one_label|a.example.com|server-cn|DNS:*.example.com|accept|false||no"
	"wildcard_case|A.Example.COM|server-cn|DNS:*.example.com|accept|false||no"
	"wildcard_bare_domain|example.com|server-cn|DNS:*.example.com,DNS:server-cn|reject|false|server-cn|no"
	"wildcard_two_labels|a.b.example.com|server-cn|DNS:*.example.com,DNS:server-cn|reject|false|server-cn|no"
	"wildcard_partial_label|ab.example.com|server-cn|DNS:a*.example.com,DNS:server-cn|reject|false|server-cn|no"
	"wildcard_double|a.b.example.com|server-cn|DNS:*.*.example.com,DNS:server-cn|reject|false|server-cn|no"
	"wildcard_inner_label|foo.a.example.com|server-cn|DNS:foo.*.example.com,DNS:server-cn|reject|false|server-cn|no"
	"ip_vs_wildcard|127.0.0.1|server-cn|DNS:*.0.0.1,DNS:server-cn|reject|false|server-cn|no"
	"cn_case_only|node.example.com|Node.Example.com|-|reject|false|Node.Example.com|no"
	"legacy_cn_only|node.example.com|node.example.com|-|accept|true||no"
	"legacy_cn_nonmatching_dns|node.example.com|node.example.com|DNS:other.example.com|accept|true||no"
	"legacy_ip_as_dns|127.0.0.1|server-cn|DNS:127.0.0.1|accept|true||no"
	"n6_dns_san_nonmatching_cn|node.example.com|server-cn|DNS:node.example.com|accept|false||no"
	"ci_setup_aerospike_server|aerospike-tls|aerospike-tls|DNS:aerospike-1,DNS:localhost,DNS:docker,IP:127.0.0.1|accept|true||no"
	"ci_setup_aerospike_server_fixed|aerospike-tls|aerospike-tls|DNS:aerospike-1,DNS:localhost,DNS:docker,IP:127.0.0.1,DNS:aerospike-tls|accept|false||no"
	"revoked_serial|node.example.com|server-cn|DNS:node.example.com|reject|false||yes"
)

if [ $# -gt 0 ]; then
	SELECTED=()
	for want in "$@"; do
		found=0
		for c in "${CASES[@]}"; do
			if [ "${c%%|*}" = "$want" ]; then
				SELECTED+=("$c")
				found=1
			fi
		done
		[ $found = 1 ] || die "unknown case '$want'"
	done
	CASES=("${SELECTED[@]}")
fi

if [ "${TLS_MATCHING_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

if [ -n "${AEROSPIKE_FEATURES_FILE:-}" ]; then
	FEATURES="$(cd "$(dirname "$AEROSPIKE_FEATURES_FILE")" && pwd)/$(basename "$AEROSPIKE_FEATURES_FILE")"
else
	FEATURES="$WORK_DIR/features.conf"
	printf '%s\n' "$AEROSPIKE_FEATURES_CONTENT" > "$FEATURES"
	chmod 644 "$FEATURES"
fi

gen_certs() {
	local dir="$1" cn="$2" san="$3"
	local ext=()

	openssl req -x509 -newkey rsa:4096 -nodes \
		-keyout "$dir/ca.key" -out "$dir/ca.crt" -days 365 \
		-subj "/CN=Aerospike-Server-Test-CA" 2>/dev/null

	if [ "$san" != "-" ]; then
		printf '[v3_req]\nsubjectAltName = %s\n' "$san" > "$dir/san.ext"
		ext=(-extensions v3_req -extfile "$dir/san.ext")
	fi

	openssl req -newkey rsa:4096 -nodes \
		-keyout "$dir/server.key" -out "$dir/server.csr" \
		-subj "/CN=$cn" 2>/dev/null
	openssl x509 -req -in "$dir/server.csr" \
		-CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
		-out "$dir/server.crt" -days 365 "${ext[@]+"${ext[@]}"}" 2>/dev/null

	rm -f "$dir"/*.csr "$dir"/*.srl "$dir/san.ext"
	chmod 644 "$dir"/*
}

write_conf() {
	local dir="$1" server_tls_name="$2"
	cat > "$dir/aerospike.conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name docker
    proto-fd-max 15000
}

logging {
    console {
        context any info
    }
}

network {
    tls $server_tls_name {
        cert-file /etc/aerospike/tls/server.crt
        key-file  /etc/aerospike/tls/server.key
        ca-file   /etc/aerospike/tls/ca.crt
    }
    service {
        address any
        port 3000
        tls-port 4333
        tls-authenticate-client false
        tls-name $server_tls_name
    }
    heartbeat {
        mode mesh
        port 3002
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

wait_ready() {
	local name="$1"
	local i

	for i in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep -q "rebalanced:"; then
			sleep 1
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name not ready after 60s"
}

SUMMARY=()
FAILED=0

for c in "${CASES[@]}"; do
	IFS='|' read -r name tls_name cn san expect warning bootstrap revoke <<< "$c"
	dir="$WORK_DIR/$name"
	mkdir -p "$dir"

	gen_certs "$dir" "$cn" "$san"
	# The server refuses to start unless its own tls-name is the certificate CN or a dNSName.
	write_conf "$dir" "$cn"

	serial=""
	if [ "$revoke" = "yes" ]; then
		serial="$(openssl x509 -in "$dir/server.crt" -noout -serial | cut -d= -f2)"
	fi

	echo
	echo "=== CASE $name"
	echo "certificate:"
	openssl x509 -in "$dir/server.crt" -noout -text | grep -m1 'Version:' | sed 's/^ */  /'
	openssl x509 -in "$dir/server.crt" -noout -subject -ext subjectAltName 2>&1 | sed 's/^/  /'
	echo "tlsName: $tls_name"
	echo "expected: $expect (sync and async), deprecation warning expected: $warning"
	[ -n "$serial" ] && echo "revokeCertificates: $serial"

	container="$PREFIX-$name"
	docker run -d --name "$container" -p "127.0.0.1:$PORT:4333" --ulimit nofile=15000 \
		-v "$dir:/etc/aerospike/tls:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file /etc/aerospike/tls/aerospike.conf >/dev/null
	STARTED+=("$container")
	wait_ready "$container"

	log="$dir/mvn.log"
	set +e
	(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteTlsMatching.class' \
		-Dtls.case="$name" -Dtls.name="$tls_name" -Dtls.expect="$expect" -Dtls.warning="$warning" \
		-Dtls.bootstrap="$bootstrap" -Dtls.revoke="$serial" -Dtls.ca="$dir/ca.crt" \
		-Dtls.host=127.0.0.1 -Dtls.port="$PORT" -Dtls.namespace=test) > "$log" 2>&1
	rc=$?
	set -e

	grep -E "TLS-RESULT|CLIENT-LOG WARN" "$log" | sed 's/^/  /' || true
	grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 | sed 's/^/  /' || echo "  no 'Tests run' line"
	grep -E "^\[ERROR\]   TestTlsMatching|AssertionError|^java\.lang\." "$log" | head -5 | sed 's/^/  /' || true

	if [ $rc -eq 0 ]; then
		status=PASS
	else
		status=FAIL
		FAILED=$((FAILED + 1))
	fi
	echo "CASE-STATUS $name $status"
	SUMMARY+=("$(printf '%-36s %-6s %s' "$name" "$expect" "$status")")

	docker rm -f "$container" >/dev/null 2>&1 || true
done

echo
echo "=== SUMMARY (case, expected, status)"
for s in "${SUMMARY[@]}"; do
	echo "$s"
done
echo "cases: ${#CASES[@]} failed: $FAILED"

[ $FAILED -eq 0 ]
