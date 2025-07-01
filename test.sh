#!/usr/bin/env bash

JSON=$(cat ./.github/test_servers.json)

MATRIX=$(echo "$JSON" | jq -c '
  to_entries
  | map({
      server:  .key,
      version: .value.version,
      type: .value.type,
    })
')

echo input-matrix="$MATRIX"
