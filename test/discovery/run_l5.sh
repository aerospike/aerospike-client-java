#!/usr/bin/env bash
#
# One L5 row of the aerospike-client-conformance l5-aws harness (l5-aws/README.md there), run by
# test/discovery/l5-aws.manifest as L5_RUN. L5_ROW selects the row's test class; the class reads
# the rest of the harness environment and fails, naming each variable, when any is missing.
#
# Required:
#   L5_ROW                    W-1, W-2, W-3 or W-5
#
# Optional environment:
#   L5_MAVEN_OFFLINE          set to 1 to run Maven offline (default 0)
#
# Usage: L5_ROW=W-1 test/discovery/run_l5.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

case "${L5_ROW:-}" in
	W-1) class=TestL5EndpointShapes ;;
	W-2) class=TestL5SpotInterruptAndRollout ;;
	W-3) class=TestL5ScaleOutBeforeListener ;;
	W-5) class=TestL5AzOutage ;;
	*) die "L5_ROW must be W-1, W-2, W-3 or W-5 (got: ${L5_ROW:-})" ;;
esac

mvn=(mvn -B)
[ "${L5_MAVEN_OFFLINE:-0}" = "1" ] && mvn+=(-o)
echo "Running $class for row $L5_ROW"
cd "$ROOT_DIR"
"${mvn[@]}" -pl test -DskipTests=false \
	"-DrunSuite=**/$class.class" \
	"-Dargs=-h ${L5_ENDPOINT_NAME:-127.0.0.1}:${L5_SEED_PORT:-3000} -p ${L5_SEED_PORT:-3000} -n test" \
	test
