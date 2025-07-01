#!/usr/bin/env bash

FILE=test_servers.json

if [[ -s "$FILE" ]]; then
  echo "⚙️  Using $FILE from repository"
  JSON=$(cat "$FILE")
else
  echo "⚠️  $FILE missing or empty – using DEFAULT_SERVER_VERSIONS"
  JSON="$DEFAULT_JSON"
fi

MATRIX=$(echo $JSON | jq -c '
    to_entries
    | map({server: .key, version: .value.version, "crypt-version": .value.crypto  })
') # → [{"server":"rc","version":"8.1.0"}, …]
echo input-matrix="$MATRIX"
