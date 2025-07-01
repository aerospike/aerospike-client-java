# CI pipeline

## Test server configuration on PR
Test server release version can be set by updating the `./.github/test_servers.json` file. 

**Format:**
```json
{
    "server_alias": {
        "type": "aerospike-server | aerospike-server-enterprise",
        "version": ...,
    },
    .
    .
    .
    .
}
```

**Concrete example:**
```json
{
  "rc-opensource": {
    "type": "aerospike-server",
    "version": "8.1.0.0-rc2_1"
  },
  "rc": {
    "type": "aerospike-server-enterprise",
    "version": "8.1.0.0-rc2_1"
  },
  "stable": {
    "type": "aerospike-server-enterprise",
    "version": "latest"
  },
  "stable-opensource": {
    "type": "aerospike-server",
    "version": "latest"
  }
}
```