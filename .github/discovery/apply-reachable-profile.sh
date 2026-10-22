#!/usr/bin/env bash
# Run on the runner after setup-discovery-topology, for TestL2ReachableNode
# (N-10). The last node advertises access-address as-reachable.test and the
# others alternate-access-address as-reachable.test, each with its own listener
# port, as run_l2.sh L2_PROFILE=reachable does. setup-aerospike-server already
# maps the name with extra-hosts, because the server must resolve it to start.
set -euo pipefail

hostname=as-reachable.test
conf_dir=/etc/aerospike-custom
IFS=',' read -r -a containers <<< "${AEROSPIKE_CONTAINER_NAMES:?}"
IFS=',' read -r -a proxies <<< "${DISCOVERY_PROXY_MAP:?}"
reachable="${containers[-1]}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

for container in "${containers[@]}"; do
  port=""
  for entry in "${proxies[@]}"; do
    [[ "${entry%:*}" == "$container" ]] && port="${entry##*:}"
  done
  if [[ -z "$port" ]]; then
    echo "No listener for ${container} in ${DISCOVERY_PROXY_MAP}" >&2
    exit 1
  fi

  if [[ "$container" == "$reachable" ]]; then
    access="        access-address ${hostname}"$'\n'"        access-port ${port}"$'\n'
  else
    access="        alternate-access-address ${hostname}"$'\n'"        alternate-access-port ${port}"$'\n'
  fi
  docker cp "${container}:${conf_dir}/${container}.conf" "${work}/${container}.conf"
  ACCESS="$access" awk '{print} $1 == "port" && $2 == "3000" {printf "%s", ENVIRON["ACCESS"]}' \
    "${work}/${container}.conf" > "${work}/${container}.new"
  if ! grep -q "access-port ${port}" "${work}/${container}.new"; then
    echo "No service port line in ${container}'s config" >&2
    exit 1
  fi
  docker cp "${work}/${container}.new" "${container}:${conf_dir}/${container}.conf"
  echo "${container} listener ${port}:"
  grep -E "access-(address|port)" "${work}/${container}.new"
done

docker restart "${containers[@]}" > /dev/null

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
  echo "${container} service-clear-std=$(docker exec "$container" asinfo -v service-clear-std)"
done

proxy_address="$(docker inspect -f "{{(index .NetworkSettings.Networks \"${CLIENT_NETWORK:?}\").IPAddress}}" "${TOXIPROXY_HOST:?}")"
{
  echo "reachable-node=${reachable}"
  echo "client-host=${hostname}:${proxy_address}"
} >> "${GITHUB_OUTPUT}"
