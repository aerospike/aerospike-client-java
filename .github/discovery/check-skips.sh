#!/usr/bin/env bash
# Fail when a test in the surefire reports skipped, unless EXPECTED_SKIPS names it. EXPECTED_SKIPS
# is a comma-separated list of <simple class name>.<method>. The result publisher fails only on
# test failures, so a suite whose environment is missing would otherwise pass with every test skipped.
#
# Usage: check-skips.sh <surefire reports directory>
set -euo pipefail

reports="${1:?surefire reports directory is required}"

python3 -I - "$reports" "${EXPECTED_SKIPS:-}" <<'EOF'
import glob
import os
import sys
import xml.etree.ElementTree as ET

reports, expected = sys.argv[1], {name for name in sys.argv[2].split(",") if name}
files = sorted(glob.glob(os.path.join(reports, "TEST-*.xml")))
if not files:
    print(f"::error::No surefire reports in {reports}")
    sys.exit(1)

total = 0
unexpected = []
for path in files:
    for case in ET.parse(path).getroot().iter("testcase"):
        total += 1
        if case.find("skipped") is None:
            continue
        name = f"{case.get('classname', '').rsplit('.', 1)[-1]}.{case.get('name')}"
        if name in expected:
            print(f"Expected skip: {name}")
        else:
            print(f"::error::{name} skipped")
            unexpected.append(name)

if total == 0:
    print(f"::error::No tests in {reports}")
    sys.exit(1)
if unexpected:
    print(f"{len(unexpected)} of {total} tests skipped unexpectedly")
    sys.exit(1)
print(f"{total} tests, no unexpected skips")
EOF
