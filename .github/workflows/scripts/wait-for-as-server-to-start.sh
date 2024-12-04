#!/bin/bash

set -x
# Makes sure that if the "docker exec" command fails, it is not ignored
set -o pipefail

container_name=$1
is_security_enabled=$2

if [[ $is_security_enabled == true ]]; then
  # We need to pass credentials to asinfo if server requires it
  # TODO: passing in credentials via command line flags since I can't figure out how to use --instance with global astools.conf
  user_credentials="--user=admin --password=admin"
fi

while true; do
  # An unset variable will have a default empty value
  # Intermediate step is to print docker exec command's output in case it fails
  # Sometimes, errors only appear in stdout and not stderr, like if asinfo throws an error because of no credentials
  # (This is a bug in asinfo since all error messages should be sent to stderr)
  # But piping and passing stdin to grep will hide the first command's stdout.
  # grep doesn't have a way to print all lines passed as input.
  # ack does have an option but it doesn't come installed by default
  # shellcheck disable=SC2086 # The flags in user credentials should be separate anyways. Not one string
  echo "Checking if we can reach the server via the service port..."
  if docker exec "$container_name" asinfo $user_credentials -v status | tee >(cat) | grep -qE "^ok"; then
    # Server is ready when asinfo returns ok
    echo "Can reach server now."
    # docker container inspect "$container_name"
    break
  fi

  echo "Server didn't return ok via the service port. Polling again..."
done

# Although the server may be reachable via the service port, the cluster may not be fully initialized yet.
# If we try to connect too soon (e.g right after "status" returns ok), the client may throw error code -1
while true; do
  echo "Waiting for server to stabilize (i.e return a cluster key)..."
  # We assume that when an ERROR is returned, the cluster is not stable yet (i.e not fully initialized)
  if docker exec "$container_name" asinfo $user_credentials -v cluster-stable 2>&1 | (! grep -qE "^ERROR"); then
    echo "Server is in a stable state."
    break
  fi

  echo "Server did not return a cluster key. Polling again..."
done
