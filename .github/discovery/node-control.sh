#!/usr/bin/env bash
# Run on the runner next to the L2 suite. Serves the start and stop requests
# TestL2Topology writes to DISCOVERY_CONTROL_DIR (N-11) by starting the nodes
# setup-aerospike-server created but did not start.
set -uo pipefail

dir="${1:?control directory}"
network="${2:?Aerospike network}"
IFS=',' read -r -a parked <<< "${3:?parked container names}"
next=0

ack() {
  printf '%s\n' "$2" > "$dir/$1.tmp"
  mv "$dir/$1.tmp" "$dir/$1.ack"
}

while true; do
  for request in "$dir"/*.request; do
    [[ -e "$request" ]] || continue
    id="$(basename "$request" .request)"
    action=""
    arg=""
    read -r action arg < "$request" || true
    rm -f "$request"

    case "$action" in
      start)
        if ((next >= ${#parked[@]})); then
          ack "$id" "error=no parked node left"
        else
          name="${parked[$next]}"
          next=$((next + 1))
          if docker start "$name" > /dev/null; then
            address="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$name")"
            ack "$id" "$(printf 'container=%s\naddress=%s' "$name" "$address")"
            echo "Control: started $name ($address)"
          else
            ack "$id" "error=cannot start $name"
          fi
        fi
        ;;
      stop)
        if [[ " ${parked[*]} " == *" $arg "* ]] && docker stop "$arg" > /dev/null; then
          ack "$id" "removed=$arg"
          echo "Control: stopped $arg"
        else
          ack "$id" "error=cannot stop $arg"
        fi
        ;;
      *)
        ack "$id" "error=unknown request: $action $arg"
        ;;
    esac
  done
  sleep 0.1
done
