#!/bin/sh

# POSIX sh does not have 'set -o pipefail'.
# We manage exit codes manually or rely on logic flow.

call_from_tools_container() {
    # Using "$@" is POSIX compliant for passing all arguments
    docker run --rm --network host aerospike/aerospike-tools "$@"
}

while true; do
    echo "Checking if we can reach the server via the service port..."

    # Process substitution >(cat) is replaced by a simple pipe to tee.
    # tee will write to the terminal (stdout) and pass the text to grep.
    if call_from_tools_container asinfo $SECURITY_FLAGS -v status | tee /dev/tty | grep -q "^ok"; then
        echo "Can reach server now."
        break
    fi

    echo "Server didn't return ok via the service port. Polling again..."
    sleep 2
done

while true; do
    echo "Waiting for server to stabilize (i.e return a cluster key)..."

    # Bash's (! grep ...) is replaced by standard logic.
    # We capture the output, check for "ERROR", and use the '!' command prefix.
    # 2>&1 redirects stderr to stdout so grep can catch it.
    if ! call_from_tools_container asinfo $SECURITY_FLAGS -v "cluster-stable:ignore-migrations=true" 2>&1 | grep -q "^ERROR"; then
        echo "Server is in a stable state."
        break
    fi

    echo "Server did not return a cluster key. Polling again..."
    sleep 2
done