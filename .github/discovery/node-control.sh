#!/usr/bin/env bash
# Run on the runner next to the L2 suite. Serves the start and stop requests
# the L2 tests write to DISCOVERY_CONTROL_DIR (N-11, N-16, N-18, N-19). Each
# start runs a new node cloned from the node setup-aerospike-server created but
# did not start, as run_l2.sh starts one locally. Start options:
# alternate-port=<port> adds the alternate hostname and that port to the new
# node's config, address=<ip> starts it on that address.
set -uo pipefail

dir="${1:?control directory}"
network="${2:?Aerospike network}"
template="${3:?parked container name}"
writer="${4:?running container that mounts the config volume}"
alternate_host="${5:-}"
conf_dir=/etc/aerospike-custom
prefix="${template%-*}"
next="${template##*-}"
started=" "
work="$(mktemp -d)"

inspect() {
  docker inspect "$template" | jq -r "$1"
}

image="$(inspect '.[0].Config.Image')"
entrypoint="$(inspect '.[0].Config.Entrypoint[0]')"
mapfile -t cmd < <(inspect '.[0].Config.Cmd[]')
mapfile -t mounts < <(inspect '.[0].Mounts[] | "\(if .Type == "volume" then .Name else .Source end):\(.Destination)\(if .RW then "" else ":ro" end)"')
mapfile -t hosts < <(inspect '.[0].HostConfig.ExtraHosts // [] | .[]')
docker cp "$template:$conf_dir/$template.conf" "$work/template.conf"

run_args=(--network "$network" --ulimit nofile=15000 --entrypoint "$entrypoint")
for mount in "${mounts[@]}"; do
  run_args+=(-v "$mount")
done
for host in "${hosts[@]}"; do
  run_args+=(--add-host "$host")
done

ack() {
  printf '%s\n' "$2" > "$dir/$1.tmp"
  mv "$dir/$1.tmp" "$dir/$1.ack"
}

start() {
  local id="$1" name conf option error="" options=() args=()
  next=$((next + 1))
  name="$prefix-$next"
  conf="$work/$name.conf"
  cp "$work/template.conf" "$conf"

  for option in $2; do
    case "$option" in
      alternate-port=*)
        if [[ -n "$alternate_host" && "${option#*=}" =~ ^[0-9]+$ ]]; then
          ACCESS="        alternate-access-address $alternate_host"$'\n'"        alternate-access-port ${option#*=}"$'\n' \
            awk '{print} $1 == "port" && $2 == "3000" {printf "%s", ENVIRON["ACCESS"]}' "$work/template.conf" > "$conf"
        else
          error="$option needs a port and a profile with an alternate hostname"
        fi
        ;;
      address=*)
        if [[ "${option#*=}" =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then
          options+=(--ip "${option#*=}")
        else
          error="$option is not an IPv4 address"
        fi
        ;;
      *)
        error="unknown start option $option"
        ;;
    esac
  done

  if [[ -n "$error" ]]; then
    ack "$id" "error=$error"
    return
  fi

  for option in "${cmd[@]}"; do
    args+=("${option/$conf_dir\/$template.conf/$conf_dir/$name.conf}")
  done

  if docker cp "$conf" "$writer:$conf_dir/$name.conf" 2> "$work/err" &&
    docker run -d --name "$name" "${options[@]}" "${run_args[@]}" "$image" "${args[@]}" > /dev/null 2>> "$work/err"; then
    started+="$name "
    local address
    address="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$name")"
    ack "$id" "$(printf 'container=%s\naddress=%s' "$name" "$address")"
    echo "Control: started $name ($address) $2"
  else
    ack "$id" "error=$(tr '\n' ' ' < "$work/err")"
  fi
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
        start "$id" "$arg"
        ;;
      stop)
        if [[ "$started" == *" $arg "* ]] && docker rm -f "$arg" > /dev/null; then
          started="${started/ $arg / }"
          ack "$id" "removed=$arg"
          echo "Control: removed $arg"
        else
          ack "$id" "error=cannot remove $arg"
        fi
        ;;
      *)
        ack "$id" "error=unknown request: $action $arg"
        ;;
    esac
  done
  sleep 0.1
done
