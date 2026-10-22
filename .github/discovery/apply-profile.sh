#!/usr/bin/env bash
# Run on the runner after setup-discovery-topology. Reproduces the run_l2.sh
# profiles. reachable (N-10), pass-through-names (N-15) and pass-through-ports
# (N-16, N-18) rewrite each running node's access addresses and restart it;
# setup-aerospike-server already maps the advertised hostnames with
# extra-hosts, because the server must resolve them to start. port-map-range
# (N-19) adds one listener per address of the node subnet range.
set -euo pipefail

profile="${L2_PROFILE:?}"
conf_dir=/etc/aerospike-custom
IFS=',' read -r -a containers <<< "${AEROSPIKE_CONTAINER_NAMES:?}"
IFS=',' read -r -a proxies <<< "${DISCOVERY_PROXY_MAP:?}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

container_address() {
  docker inspect -f "{{(index .NetworkSettings.Networks \"$2\").IPAddress}}" "$1"
}

listener_port() {
  local entry
  for entry in "${proxies[@]}"; do
    if [[ "${entry%:*}" == "$1" ]]; then
      echo "${entry##*:}"
      return
    fi
  done
  echo "No listener for $1 in ${DISCOVERY_PROXY_MAP}" >&2
  exit 1
}

access() {
  printf '        %saccess-address %s\n        %saccess-port %s\n' "$1" "$2" "$1" "$3"
}

wait_cluster() {
  local container size=""
  for container in "${containers[@]}"; do
    for _ in $(seq 1 60); do
      size="$(docker exec "$container" asinfo -v statistics 2> /dev/null | tr ';' '\n' | sed -n 's/^cluster_size=//p')"
      [[ "$size" == "${#containers[@]}" ]] && break
      sleep 1
    done
    if [[ "$size" != "${#containers[@]}" ]]; then
      echo "${container} reports cluster_size=${size:-none}, expected ${#containers[@]}" >&2
      exit 1
    fi
    echo "${container} service-clear-std=$(docker exec "$container" asinfo -v service-clear-std)" \
      "service-clear-alt=$(docker exec "$container" asinfo -v service-clear-alt)"
  done
}

case "$profile" in
  reachable | pass-through-names | pass-through-ports)
    proxy_address="$(container_address "${TOXIPROXY_HOST:?}" "${CLIENT_NETWORK:?}")"
    hostnames=()
    index=0
    for container in "${containers[@]}"; do
      index=$((index + 1))
      port="$(listener_port "$container")"
      case "$profile" in
        reachable)
          hostnames=(as-reachable.test)
          if [[ "$container" == "${containers[-1]}" ]]; then
            lines="$(access "" as-reachable.test "$port")"
          else
            lines="$(access alternate- as-reachable.test "$port")"
          fi
          ;;
        pass-through-names)
          hostnames+=("node-${index}.pass-through.test")
          lines="$(access alternate- "node-${index}.pass-through.test" "$port")"
          ;;
        pass-through-ports)
          hostnames=(as-pass-through.test)
          lines="$(access alternate- as-pass-through.test "$port")"
          ;;
      esac
      docker cp "${container}:${conf_dir}/${container}.conf" "${work}/${container}.conf"
      ACCESS="${lines}"$'\n' awk '{print} $1 == "port" && $2 == "3000" {printf "%s", ENVIRON["ACCESS"]}' \
        "${work}/${container}.conf" > "${work}/${container}.new"
      if ! grep -q "access-port ${port}" "${work}/${container}.new"; then
        echo "No service port line in ${container}'s config" >&2
        exit 1
      fi
      docker cp "${work}/${container}.new" "${container}:${conf_dir}/${container}.conf"
    done

    docker restart "${containers[@]}" > /dev/null
    wait_cluster

    client_hosts=""
    for hostname in "${hostnames[@]}"; do
      client_hosts+="${client_hosts:+,}${hostname}:${proxy_address}"
    done
    {
      echo "client-hosts=${client_hosts}"
      [[ "$profile" == reachable ]] && echo "reachable-node=${containers[-1]}"
      [[ "$profile" == pass-through-names ]] && echo "pass-through-branch=A"
      [[ "$profile" == pass-through-ports ]] && echo "pass-through-branch=B"
    } >> "${GITHUB_OUTPUT}"
    ;;
  port-map-range)
    first="$(container_address "${containers[0]}" "${AEROSPIKE_NETWORK:?}")"
    prefix="${first%.*}"
    range_first=2
    range_last=17
    port_base=31000
    # Keep Toxiproxy off the range, so a node the test starts on a free address can take it.
    docker network disconnect "${AEROSPIKE_NETWORK}" "${TOXIPROXY_HOST:?}"
    docker network connect --ip "${prefix}.250" "${AEROSPIKE_NETWORK}" "${TOXIPROXY_HOST}"
    docker run --rm --network "${CLIENT_NETWORK:?}" --entrypoint sh \
      -e PREFIX="$prefix" -e FIRST="$range_first" -e LAST="$range_last" -e BASE="$port_base" -e URL="${TOXIPROXY_URL:?}" \
      curlimages/curl:8.11.1 -c 'set -e
        for octet in $(seq "$FIRST" "$LAST"); do
          curl -sf -o /dev/null -X POST "$URL/proxies" -H "Content-Type: application/json" \
            -d "{\"name\":\"range-$PREFIX.$octet\",\"listen\":\"0.0.0.0:$((BASE + octet))\",\"upstream\":\"$PREFIX.$octet:3000\",\"enabled\":true}"
        done'
    echo "Listeners on ${port_base} + last octet for ${prefix}.${range_first}-${prefix}.${range_last}"
    {
      echo "port-map-range=${prefix}.${range_first}-${prefix}.${range_last}"
      echo "port-map-port-base=${port_base}"
    } >> "${GITHUB_OUTPUT}"
    ;;
  *)
    echo "No profile step for ${profile}" >&2
    exit 1
    ;;
esac
