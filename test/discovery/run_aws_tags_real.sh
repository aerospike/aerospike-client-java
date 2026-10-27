#!/usr/bin/env bash
#
# AWS EC2 tag seed discovery (AwsEc2TagSeedCandidateProvider) against real AWS.
#
# Run on an EC2 instance in the same region as a small self-managed Aerospike cluster whose nodes
# carry the tag AWS_TAGS_REAL_TAG. The instance's instance profile role must allow only
# ec2:DescribeInstances (the documented minimum policy) and is the only credential source: the script
# fails when any AWS credential environment variable is set. Runs SuiteAwsTagsReal once, which:
#   - runs AWS_TAGS_REAL_RUNS fresh JVMs, each with one cold refresh (credentials through IMDSv2
#     included) and AWS_TAGS_REAL_WARM warm refreshes, and reports min, median, p99 and max of each
#     against the default discoveryTendRefreshDeadline (100ms) and the recommended timeout (80ms);
#   - checks that credentials came from IMDS and that AWS accepted the signature;
#   - checks that the seed list holds AWS_TAGS_REAL_EXPECTED_NODES hosts and none of
#     AWS_TAGS_REAL_EXCLUDED (untagged and stopped instances);
#   - runs one fresh JVM with the temporary credentials of a role with no policy in AWS_ACCESS_KEY_ID,
#     AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN, and checks that EC2 refuses it with
#     UnauthorizedOperation and a request id;
#   - joins every tagged node with a client seeded only by the provider and reports the overrun
#     count after AWS_TAGS_REAL_TEND_REFRESHES tend refreshes.
# The account allows roles and instance profiles, not IAM users. Obtain the no-policy role's
# temporary credentials from another principal allowed to assume it, for example
#   aws sts assume-role --role-arn <arn> --role-session-name aws-tags-denied
#
# Required environment:
#   AWS_TAGS_REAL_REGION                   region of the instances, e.g. us-east-1
#   AWS_TAGS_REAL_TAG                      tag filter key=value, e.g. aerospike-cluster=aws-tags-real
#   AWS_TAGS_REAL_EXPECTED_NODES           number of tagged running cluster nodes
#   AWS_TAGS_REAL_EXCLUDED                 comma-separated private IPs of untagged and stopped instances
#   AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID     temporary credentials of a role with no policy
#   AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY
#   AWS_TAGS_REAL_DENIED_SESSION_TOKEN
# Optional environment:
#   AWS_TAGS_REAL_NAMESPACE                namespace of the cluster (default test)
#   AWS_TAGS_REAL_RUNS                     fresh-JVM latency runs (default 20)
#   AWS_TAGS_REAL_WARM                     warm refreshes per run (default 20)
#   AWS_TAGS_REAL_TEND_REFRESHES           tend refreshes counted by the join case (default 30)
#   AWS_TAGS_REAL_SKIP_BUILD               set to 1 to skip installing the client module first
#   AWS_TAGS_REAL_MAVEN_OFFLINE            set to 1 to run Maven offline (default 0)
#
# Usage: test/discovery/run_aws_tags_real.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/target/discovery-aws-tags-real"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

missing=()
for name in AWS_TAGS_REAL_REGION AWS_TAGS_REAL_TAG AWS_TAGS_REAL_EXPECTED_NODES AWS_TAGS_REAL_EXCLUDED \
	AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY AWS_TAGS_REAL_DENIED_SESSION_TOKEN; do
	[ -n "${!name:-}" ] || missing+=("$name")
done
[ ${#missing[@]} -eq 0 ] || die "required variables not set: ${missing[*]}"

case "$AWS_TAGS_REAL_TAG" in
	?*=?*) ;;
	*) die "AWS_TAGS_REAL_TAG '$AWS_TAGS_REAL_TAG' is not key=value" ;;
esac
TAG_KEY="${AWS_TAGS_REAL_TAG%%=*}"
TAG_VALUE="${AWS_TAGS_REAL_TAG#*=}"

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_SHARED_CREDENTIALS_FILE \
	AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI; do
	[ -z "${!name:-}" ] || die "$name is set; the instance profile must be the only credential source"
done
[ ! -e "$HOME/.aws/credentials" ] || die "$HOME/.aws/credentials exists; the instance profile must be the only credential source"

command -v curl >/dev/null 2>&1 || die "curl not found (IMDSv2 check)"
IMDS_TOKEN="$(curl -sf -m 2 -X PUT http://169.254.169.254/latest/api/token \
	-H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
[ -n "$IMDS_TOKEN" ] || die "IMDSv2 is not reachable: run on an EC2 instance with IMDS enabled"
ROLE="$(curl -sf -m 2 -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
	http://169.254.169.254/latest/meta-data/iam/security-credentials/ || true)"
[ -n "$ROLE" ] || die "no AWS credentials: the instance has no instance profile role"
echo "Instance profile role: $ROLE"

MVN=(mvn)
[ "${AWS_TAGS_REAL_MAVEN_OFFLINE:-0}" = "1" ] && MVN+=(-o)

if [ "${AWS_TAGS_REAL_SKIP_BUILD:-0}" != "1" ]; then
	echo "Installing client module"
	(cd "$ROOT_DIR" && "${MVN[@]}" -q -pl client -am install -DskipTests) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
log="$WORK_DIR/mvn.log"
set +e
(cd "$TEST_DIR" && "${MVN[@]}" test -DskipTests=false -DrunSuite='**/SuiteAwsTagsReal.class' \
	-Dawsreal.region="$AWS_TAGS_REAL_REGION" -Dawsreal.tagKey="$TAG_KEY" -Dawsreal.tagValue="$TAG_VALUE" \
	-Dawsreal.expectedNodes="$AWS_TAGS_REAL_EXPECTED_NODES" -Dawsreal.excluded="$AWS_TAGS_REAL_EXCLUDED" \
	-Dawsreal.namespace="${AWS_TAGS_REAL_NAMESPACE:-test}" -Dawsreal.runs="${AWS_TAGS_REAL_RUNS:-20}" \
	-Dawsreal.warm="${AWS_TAGS_REAL_WARM:-20}" \
	-Dawsreal.tendRefreshes="${AWS_TAGS_REAL_TEND_REFRESHES:-30}") > "$log" 2>&1
rc=$?
set -e

grep -E "AWS-REAL-" "$log" || true
grep -E "^\[ERROR\]|AssertionError|^java\.lang\." "$log" | head -20 || true
grep -E "^(\[[A-Z]+\] )?Tests run:" "$log" | tail -1 || echo "no 'Tests run' line"
echo "maven log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
