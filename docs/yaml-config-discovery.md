# YAML configuration: discovery and translation

These keys sit under `static.client` in the YAML config file named by the
`AEROSPIKE_CLIENT_CONFIG_URL` environment variable. Each one sets a `ClientPolicy`
field when the client is constructed and overrides a value set in code. With
logging at info level, each override is logged as `Set ClientPolicy.<field> = <value>`.
A key that is absent leaves the field as it is: the value set in code, otherwise the
`ClientPolicy` default.

Schema version `1.2.0` adds these keys. Files declaring `1.0.0` or `1.1.0` load them too.

```yaml
version: 1.2.0
static:
  client:
    use_services_alternate: true
    discovery:
      provider: static
      merge_policy: merge
      refresh_interval_ms: 30000
      tend_refresh_deadline_ms: 100
    translation:
      provider: port-map
      on_miss: strict
      port_map:
        "10.0.0.1:3000": "lb.example.com:30001"
        "BB9020011AC4202": "lb.example.com:30002"
```

## `static.client`

| Key | Type | Values | Default | `ClientPolicy` field |
| --- | --- | --- | --- | --- |
| `ip_map` | map of string to string | advertised host to the host to connect to; the port is kept | none (no translation) | `ipMap` |
| `use_services_alternate` | bool | `true`, `false` | `false` | `useServicesAlternate` |

`ip_map` and `translation.static_map` cannot both be set.

`dynamic.client.use_service_alternative` sets the same field and is applied after
`static.client.use_services_alternate`, so it wins when both are set.

## `static.client.discovery`

| Key | Type | Values | Default | `ClientPolicy` field |
| --- | --- | --- | --- | --- |
| `provider` | string | `static` | `static` | `seedCandidateProvider` (`static` sets it to null: `StaticSeedCandidateProvider` over the seed hosts passed to the client constructor, the same as setting nothing) |
| `merge_policy` | string | `merge`, `replace`, `discovery_only` | `merge` | `discoveryMergePolicy` |
| `refresh_interval_ms` | int | milliseconds | `30000` | `discoveryRefreshInterval` |
| `tend_refresh_deadline_ms` | int | milliseconds | `100` | `discoveryTendRefreshDeadline` |

Code only for now:

| Key or value | Status |
| --- | --- |
| `provider: dns` | Not yet configurable from YAML. Set `ClientPolicy.seedCandidateProvider` in code. |
| `provider: aws-tags` | Not yet configurable from YAML. Set `ClientPolicy.seedCandidateProvider` in code. |
| `provider_class` | Not read from YAML (ignored as an unknown key). Set `ClientPolicy.seedCandidateProvider` in code. |

Not supported in Java:

| Key | Status |
| --- | --- |
| `execution_mode` | Not supported. The Java client runs discovery on the tend thread only. |
| `refresh_timeout_ms` | Not supported. The Java client runs discovery on the tend thread only. |

`refresh_interval_ms` and `tend_refresh_deadline_ms` are checked against
`dynamic.client.tend_interval` at client construction when the provider needs periodic
refresh, as when they are set in code. A value that fails the check fails client
construction, whether it came from the file or from code.

## `static.client.translation`

| Key | Type | Values | Default | `ClientPolicy` field |
| --- | --- | --- | --- | --- |
| `provider` | string | `static-map`, `port-map`, `pass-through` | `static-map` | `addressTranslator` |
| `on_miss` | string | `pass_through`, `strict` | `pass_through` | `translationOnMiss` |
| `static_map` | map of string to string | advertised host to the host to connect to; the port is kept | none | `addressTranslator` (with `provider: static-map`) |
| `port_map` | map of string to string | key: an advertised `host:port` (IPv6 host in brackets, `[2001:db8::1]:3000`) or a node name; value: the `host:port` to connect to | none | `addressTranslator` (with `provider: port-map`) |

`provider` builds:

| Value | `addressTranslator` |
| --- | --- |
| `static-map` | `StaticMapAddressTranslator` over `static_map`. Without `static_map`, null: the default `StaticMapAddressTranslator` over `ip_map`. |
| `port-map` | `PortMapAddressTranslator` over `port_map`, which is required. Entries are validated by the `PortMapAddressTranslator` constructor. |
| `pass-through` | `PassThroughAddressTranslator`. Use with `use_services_alternate: true` when each node advertises a client-reachable alternate address and port. |

`on_miss` applies when the translator has no mapping for an advertised endpoint:

| Value | Behaviour |
| --- | --- |
| `pass_through` (default) | Connect to the advertised endpoint unchanged. |
| `strict` | Do not connect to the advertised endpoint; the miss fails with `AerospikeException.TranslationMiss`. |

`provider` absent: `addressTranslator` is left as it is unless `static_map` is set,
which implies `static-map`. `static_map` is valid only with `static-map` and
`port_map` only with `port-map`. Quote `port_map` keys and values: an unquoted
`host:port` can be read as a number.

Code only: `provider_class` is not read from YAML (ignored as an unknown key). Set
`ClientPolicy.addressTranslator` in code.

## Invalid settings

A setting below is invalid. An invalid setting makes the whole file invalid, which is
handled like any other file the client cannot parse: a warning naming the key or value
is logged, nothing from the file is applied, and the client starts on the config set
in code.

- an unknown value for `discovery.provider`, `discovery.merge_policy`,
  `translation.provider` or `translation.on_miss`;
- `discovery.execution_mode` or `discovery.refresh_timeout_ms`;
- `discovery.provider: dns` or `discovery.provider: aws-tags`;
- `static_map` with a provider other than `static-map`, `port_map` with one other than
  `port-map`, or `provider: port-map` without `port_map`;
- `static_map` and `ip_map` both set;
- a value of the wrong type, such as a string for `refresh_interval_ms`;
- a `port_map` entry that the `PortMapAddressTranslator` constructor rejects.

Example warning:

```
Error -1,0: Failed to parse file:///etc/aerospike/client.yaml: com.aerospike.client.AerospikeException: Error -1,0: Invalid static.client.translation.on_miss value 'drop': valid values: pass_through, strict
```

Unknown keys are ignored everywhere in the file, including inside `discovery` and
`translation`; the rest of the file applies.

## Reload

The client checks the file for changes every `static.client.config_interval`. As for
the other `static.client` keys, a changed value is copied into the client's merged
`ClientPolicy` and logged, but the running cluster keeps the translator, `ip_map`,
`on_miss`, seed provider and discovery settings it was constructed with. They take
effect for a new client instance. `useServicesAlternate` is the exception: the cluster
re-reads it on every reload because `dynamic.client.use_service_alternative` is dynamic,
so a changed `use_services_alternate` takes effect for the running cluster after the reload.

A reloaded file with an invalid setting is logged at warning
(`Dynamic configuration failed: ...` naming the key or value) and the last good config
stays in use.
