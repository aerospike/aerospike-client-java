#!/usr/bin/env bash
#
# The L5 driver for the l5-aws harness in aerospike-client-conformance (l5-aws/DRIVER.md there),
# run by test/discovery/l5-aws.manifest as L5_DRIVER once L5_BUILD has compiled the test module and
# written its classpath. Runs L5Driver with this script's arguments, on the runner's JDK 21 when it
# is installed and on JAVA_HOME or PATH otherwise.
#
# Usage: test/discovery/l5_driver.sh --seed HOST:PORT --shape pass-through|port-map ...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CORRETTO=/usr/lib/jvm/java-21-amazon-corretto
CLASSPATH_FILE="$TEST_DIR/target/l5-driver.classpath"

if [ -d "$CORRETTO" ]; then
	JAVA_HOME="$CORRETTO"
fi

if [ ! -f "$CLASSPATH_FILE" ]; then
	echo "ERROR: $CLASSPATH_FILE not found; run L5_BUILD first" >&2
	exit 1
fi
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -cp "$TEST_DIR/target/test-classes:$(cat "$CLASSPATH_FILE")" \
	com.aerospike.test.discovery.L5Driver "$@"
