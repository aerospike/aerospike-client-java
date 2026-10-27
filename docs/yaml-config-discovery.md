# YAML configuration: discovery and translation

These keys sit under `static.client` in the YAML config file named by the
`AEROSPIKE_CLIENT_CONFIG_URL` environment variable. Each one sets a `ClientPolicy`
field when the client is constructed and overrides a value set in code. With
logging at info level, each override is logged as `Set ClientPolicy.<field> = <value>`.
A key that is absent leaves the field as it is: the value set in code, otherwise the
`ClientPolicy` default.

To write a custom seed candidate provider or address translator, see the
[plugin author guide](discovery-plugin-guide.md).

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
| `provider` | string | `static`, `dns`, `aws-tags`, or a name registered with `ClientPolicy.registerSeedCandidateProvider` | `static` | `seedCandidateProvider` (`static` sets it to null: `StaticSeedCandidateProvider` over the seed hosts passed to the client constructor, the same as setting nothing; `dns` sets it to a `DnsSeedCandidateProvider` built from the `dns` block, see [`static.client.discovery.dns`](#staticclientdiscoverydns); `aws-tags` sets it to an `AwsEc2TagSeedCandidateProvider` built from the `aws` block, see [`static.client.discovery.aws`](#staticclientdiscoveryaws); a registered name sets it to the provider its factory builds, see [Registered names](#registered-names)) |
| `dns` | map | see [`static.client.discovery.dns`](#staticclientdiscoverydns) | none | `seedCandidateProvider` (with `provider: dns`) |
| `aws` | map | see [`static.client.discovery.aws`](#staticclientdiscoveryaws) | none | `seedCandidateProvider` (with `provider: aws-tags`) |
| `merge_policy` | string | `merge`, `replace`, `discovery_only`, see [Merge policy](discovery-plugin-guide.md#merge-policy) | `merge` | `discoveryMergePolicy` |
| `refresh_interval_ms` | int | milliseconds | `30000` | `discoveryRefreshInterval` |
| `tend_refresh_deadline_ms` | int | milliseconds | `100` | `discoveryTendRefreshDeadline` |

Code only for now:

| Key or value | Status |
| --- | --- |
| `provider_class` | Not read from YAML (ignored as an unknown key). Register a name with `ClientPolicy.registerSeedCandidateProvider`, or set `ClientPolicy.seedCandidateProvider` in code. |

Not supported in Java:

| Key | Status |
| --- | --- |
| `execution_mode` | Not supported. The Java client runs discovery on the tend thread only. |
| `refresh_timeout_ms` | Not supported. The Java client runs discovery on the tend thread only. |

`refresh_interval_ms` and `tend_refresh_deadline_ms` are checked against
`dynamic.client.tend_interval` at client construction when the provider needs periodic
refresh, as when they are set in code. A value that fails the check fails client
construction, whether it came from the file or from code.

## `static.client.discovery.dns`

`provider: dns` builds a `DnsSeedCandidateProvider` from the `dns` block, which is
required with `provider: dns` and invalid with any other provider. On every refresh the
provider resolves each name in `names` and returns the results as seed hosts.

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: dns
      merge_policy: merge
      refresh_interval_ms: 30000
      tend_refresh_deadline_ms: 100
      dns:
        names:
          - seeds.aerospike.internal
        record_type: a
        port: 3000
        timeout_ms: 50
        servers:
          - 10.0.0.2
```

| Key | Type | Values | Default | `DnsSeedCandidateProvider.Builder` method |
| --- | --- | --- | --- | --- |
| `names` | list of string | one or more DNS names, no duplicates (compared case-insensitively, trailing dot optional) | required | `names` |
| `record_type` | string | `a` (A and AAAA records), `srv` (SRV records) | `a` | `recordType` |
| `port` | int | port set on hosts resolved from A and AAAA records; ignored for `srv`, which uses each record's port | `3000` | `port` |
| `tls_name` | string | TLS name set on every returned host | none | `tlsName` |
| `timeout_ms` | int | total milliseconds for one refresh, across every name, record type, SRV target and resolver | `80` | `timeout` |
| `servers` | list of string | resolver addresses, `ip[:port]` (an IPv6 address with a port in brackets, `"[2001:db8::1]:53"`), tried in order | the system resolvers, read once when the provider is built | `servers` |

Unknown keys inside the block are ignored.

Results:

| `record_type` | Returned hosts |
| --- | --- |
| `a` | One host per A and AAAA address, as an IP address, with `port`. |
| `srv` | One host per SRV record, with the record's port, ordered by priority (lowest first), then weight (highest first). Each target is resolved within the same refresh by an A query, then an AAAA query when it has no A record, and returned as an IP address. A target that does not resolve in time is dropped and logged at warning level. Addresses in the additional section of the SRV answer are not used: the JDK DNS provider exposes only the answer section. |

Every returned host is an IP address, never a hostname, so the client does no further name
resolution when it seeds from the result. Hosts are deduplicated and returned in the same
order for the same DNS answers. Queries go through the JDK DNS provider
(`com.sun.jndi.dns.DnsContextFactory`, module `jdk.naming.dns`); the provider starts no thread.

Deadline: the provider needs periodic refresh and runs on the tend thread, so `timeout_ms`
must not exceed `tend_refresh_deadline_ms` (`ClientPolicy.discoveryTendRefreshDeadline`).
Client construction fails otherwise:

```
DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100
```

With the default deadline of 100 ms, only a local resolver (on the client host or its local
network) answers reliably in time.

Failure:

| Case | Behaviour |
| --- | --- |
| Some names resolve | Their hosts are returned. Each failed name is logged at warning level, for example `DNS seed name b.example.com failed: NXDOMAIN`. |
| Every name fails (NXDOMAIN, SERVFAIL, timeout or an empty answer) | The refresh fails with `DNS seed discovery failed for every name: a.example.com: NXDOMAIN; b.example.com: timeout`. On a periodic refresh the warning `Seed candidate refresh failed: ...` is logged and the previous seed list is kept. At client construction the warning `Seed candidate provider dns failed: ...` is logged and the client continues with the seed hosts passed to the constructor; with none, construction fails with `SEED_DISCOVERY_FAILED` (-27). |

Split network: the seed name resolves only on the client's network, to addresses the client
can reach, while the servers advertise addresses of their own network. Point `servers` at
the client network's resolver and pair the provider with a translator for the advertised
addresses:

```yaml
version: 1.2.0
static:
  client:
    use_services_alternate: true
    discovery:
      provider: dns
      merge_policy: discovery_only
      dns:
        names:
          - _aerospike._tcp.client-net.example.com
        record_type: srv
        timeout_ms: 50
        servers:
          - 10.20.0.2
    translation:
      provider: pass-through
```

The same provider in code:

```java
ClientPolicy policy = new ClientPolicy();
policy.seedCandidateProvider = DnsSeedCandidateProvider.builder()
    .names("_aerospike._tcp.client-net.example.com")
    .recordType(DnsSeedCandidateProvider.RecordType.SRV)
    .timeout(50)
    .servers("10.20.0.2")
    .build();
AerospikeClient client = new AerospikeClient(policy);
```

## `static.client.discovery.aws`

`provider: aws-tags` builds an `AwsEc2TagSeedCandidateProvider` from the `aws` block, which
is required with `provider: aws-tags` and invalid with any other provider. On every refresh
the provider lists the running EC2 instances that match `filters`, usually a tag, through
the EC2 `DescribeInstances` API, and returns one seed host per instance. Use it for a
self-managed Aerospike cluster on EC2 whose nodes carry a known tag.

Self-managed EC2 example: the nodes carry the tag `aerospike-cluster=prod-a` and are
reached on their private IPs, port 3000. The seed host passed to the client constructor
is kept as a fallback (`merge`):

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: aws-tags
      merge_policy: merge
      refresh_interval_ms: 30000
      tend_refresh_deadline_ms: 100
      aws:
        region: us-east-1
        filters:
          - name: tag:aerospike-cluster
            values:
              - prod-a
        address_field: private_ip
        port: 3000
        timeout_ms: 80
```

The same provider in code:

```java
ClientPolicy policy = new ClientPolicy();
policy.seedCandidateProvider = AwsEc2TagSeedCandidateProvider.builder()
    .region("us-east-1")
    .tagFilter("aerospike-cluster", "prod-a")
    .addressField(AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP)
    .port(3000)
    .build();
policy.discoveryMergePolicy = SeedMergePolicy.MERGE;
AerospikeClient client = new AerospikeClient(policy, new Host("10.0.1.10", 3000));
```

| Key | Type | Values | Default | `AwsEc2TagSeedCandidateProvider.Builder` method |
| --- | --- | --- | --- | --- |
| `region` | string | AWS region of the instances and of the EC2 and STS endpoints, such as `us-east-1` | required | `region` |
| `filters` | list of maps | one or more `name` (string) and `values` (non-empty list of strings) pairs, passed to `DescribeInstances` as given: `tag:<key>` or any other EC2 filter name except `instance-state-name` | required | `filter`, `tagFilter` |
| `address_field` | string | `private_ip`, `public_ip`, `private_dns`, `public_dns` | `private_ip` | `addressField` |
| `port` | int | port set on every host | `3000` | `port` |
| `tls_name` | string | TLS name set on every host | none | `tlsName` |
| `timeout_ms` | int | total milliseconds for one tend refresh, credentials and every page included | `80` | `timeout` |
| `init_timeout_ms` | int | total milliseconds for the first refresh, which runs in the client constructor and resolves the credentials | `10000` | `initTimeout` |
| `endpoint` | string | EC2 endpoint URL, `http` or `https`, such as a VPC interface endpoint | `https://ec2.<region>.amazonaws.com` | `endpoint` |

Unknown keys inside the block are ignored. Static access keys (`credentials`) and the
credentials hook (`credentialsHook`) are set in code only.

Results: each refresh sends one SigV4-signed EC2 Query API `DescribeInstances` request
(API version `2016-11-15`) with `filters` plus `instance-state-name=running`, and follows
`NextToken` pagination within the same timeout. It returns one host per instance with a
non-empty `address_field`, with `port` and `tls_name`, deduplicated and sorted by address.
An instance without that field is skipped and the count is logged at debug level. The
provider adds no SDK or other dependency to the client, and starts no thread. It does not
use an HTTP proxy.

Minimum IAM policy of the credentials the provider uses:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["ec2:DescribeInstances"], "Resource": "*"}
  ]
}
```

The provider sees only the instances of the credentials' own account, so it serves
self-managed clusters on EC2, not Aerospike Cloud.

Credentials. When `credentialsHook` is set in code, it replaces the chain below. Otherwise
the first source found, in this order, is used:

1. `credentials` access key, secret and session token set in code;
2. environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`;
3. static keys (`aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`) of profile
   `AWS_PROFILE` (default `default`) in the shared credentials file
   `AWS_SHARED_CREDENTIALS_FILE` (default `~/.aws/credentials`);
4. EKS web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` (and the optional
   `AWS_ROLE_SESSION_NAME`), exchanged through STS `AssumeRoleWithWebIdentity` on the
   regional STS endpoint;
5. ECS container credentials: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
   `AWS_CONTAINER_CREDENTIALS_FULL_URI`, with the authorization token from
   `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `AWS_CONTAINER_AUTHORIZATION_TOKEN`;
6. the EC2 instance profile through IMDSv2 (a session token `PUT`, then the role
   credentials), unless `AWS_EC2_METADATA_DISABLED` is `true`.

Not in the chain: SSO, `credential_process`, `role_arn` profiles and cross-account roles.
Use the hook for them; it may wrap the application's own AWS SDK credentials provider. The
hook runs on the thread that refreshes the provider, the tend thread included, so it must
return quickly. Credentials it returns with an expiry are cached until five minutes before
it; without one they are used for the life of the provider.

The chain resolves once, at the first refresh, within `init_timeout_ms`, and logs
`AWS EC2 seed provider credentials from <source>: access key <id>` at info level. Later
refreshes reuse the source that answered. Temporary credentials are cached with their
expiry and reloaded on the refreshing thread when they are within five minutes of expiry,
never on a timer. No access key secret, session token, web identity token or signature is
logged or put in a message.

Deadline: the Java client runs discovery on the tend thread only. The provider needs
periodic refresh, so `timeout_ms` must not exceed `tend_refresh_deadline_ms`
(`ClientPolicy.discoveryTendRefreshDeadline`). Client construction fails otherwise:

```
AWS EC2 seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100
```

Every connect and read of a refresh is bounded by the time left of `timeout_ms`, credentials
included, and the first refresh by `init_timeout_ms`. The endpoint host names are resolved
by the system resolver, which the provider cannot bound. For an in-region endpoint, keep the
defaults, `timeout_ms: 80` and `tend_refresh_deadline_ms: 100`. An out-of-region endpoint, or
a credential reload on tend that is slow (STS, ECS or IMDS), makes refreshes overrun the
deadline: each overrun is logged as
`Seed candidate refresh took 101ms, exceeding tend deadline 100ms. Consecutive overruns: 1`,
and after three consecutive overruns the client disables the provider for the life of the
client and restores the initial seeds
(`Seed candidate provider exceeded tend deadline 3 consecutive times. Provider disabled for this client and initial seeds restored`).

Failure:

| Case | Behaviour |
| --- | --- |
| An EC2 or STS error | The refresh fails with the HTTP status, AWS error code, message and request id, for example `AWS EC2 seed provider: DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not authorized to perform this operation. (request id ea966190-f9aa-478e-9ede-example)`. |
| No credentials | The refresh fails with `AWS EC2 seed provider: no AWS credentials found in config, environment, shared credentials file, web identity or ECS, and IMDS token timed out` (or the IMDS failure). |
| Timeout | The refresh fails with `AWS EC2 seed provider: DescribeInstances timed out after 80ms`. |
| Any failure | On a periodic refresh the warning `Seed candidate refresh failed: ...` is logged and the previous seed list is kept. At client construction the warning `Seed candidate provider aws-tags failed: ...` is logged and the client continues with the seed hosts passed to the constructor; with none, construction fails with `SEED_DISCOVERY_FAILED` (-27). |
| No instance matches | Not a failure: the refresh returns no host, and `merge_policy` decides the seed list. |

## `static.client.translation`

| Key | Type | Values | Default | `ClientPolicy` field |
| --- | --- | --- | --- | --- |
| `provider` | string | `static-map`, `port-map`, `pass-through`, or a name registered with `ClientPolicy.registerAddressTranslator` | `static-map` | `addressTranslator` |
| `on_miss` | string | `pass_through`, `strict` | `pass_through` | `translationOnMiss` |
| `static_map` | map of string to string | advertised host to the host to connect to; the port is kept | none | `addressTranslator` (with `provider: static-map`) |
| `port_map` | map of string to string | key: an advertised `host:port` (IPv6 host in brackets, `[2001:db8::1]:3000`) or a node name; value: the `host:port` to connect to | none | `addressTranslator` (with `provider: port-map`) |

`provider` builds:

| Value | `addressTranslator` |
| --- | --- |
| `static-map` | `StaticMapAddressTranslator` over `static_map`. Without `static_map`, null: the default `StaticMapAddressTranslator` over `ip_map`. |
| `port-map` | `PortMapAddressTranslator` over `port_map`, which is required. Entries are validated by the `PortMapAddressTranslator` constructor. On AWS, see [Deployment limits](discovery-plugin-guide.md#deployment-limits). |
| `pass-through` | `PassThroughAddressTranslator`. Use with `use_services_alternate: true` when each node advertises a client-reachable alternate address and port. On AWS with `alternate-access-port`, see [Deployment limits](discovery-plugin-guide.md#deployment-limits). |
| a registered name | The translator its factory builds, see [Registered names](#registered-names). |

`on_miss` applies when the translator has no mapping for an advertised endpoint:

| Value | Behaviour |
| --- | --- |
| `pass_through` (default) | Connect to the advertised endpoint unchanged. |
| `strict` | Do not connect to the advertised endpoint; the miss fails with `AerospikeException.TranslationMiss`, result code `TRANSLATION_MISS` (-25). |

`provider` absent: `addressTranslator` is left as it is unless `static_map` is set,
which implies `static-map`. `static_map` is valid only with `static-map` and
`port_map` only with `port-map`. Quote `port_map` keys and values: an unquoted
`host:port` can be read as a number.

Code only: `provider_class` is not read from YAML (ignored as an unknown key). Register a
name with `ClientPolicy.registerAddressTranslator`, or set `ClientPolicy.addressTranslator` in code.

## Registered names

A custom seed candidate provider or address translator can be selected from YAML by a name
registered on the `ClientPolicy` passed to the client constructor. For writing one, see the
[plugin author guide](discovery-plugin-guide.md):

```java
ClientPolicy policy = new ClientPolicy();
policy.registerSeedCandidateProvider("consul", options -> new ConsulSeedProvider(options));
policy.registerAddressTranslator("nat-table", options -> new NatTableTranslator(options));
AerospikeClient client = new AerospikeClient(policy, seeds);
```

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: consul
      merge_policy: merge
      service: aerospike
      endpoints:
        - consul-1.example.com:8500
    translation:
      provider: nat-table
      on_miss: strict
      table:
        "10.0.0.1:3000": "lb.example.com:30001"
```

The factory is a `Function<Map<String,Object>, ...>`. Its options map holds every key of the
block except `provider`, with the values as YAML loaded them (string, integer, boolean, list
or map). That includes the framework keys of the block, such as `merge_policy` or `on_miss`,
which still apply as described above. A block that sets only `provider` gives an empty map.
In the example, the `consul` factory receives `{merge_policy=merge, service=aerospike,
endpoints=[consul-1.example.com:8500]}` and the `nat-table` factory receives
`{on_miss=strict, table={10.0.0.1:3000=lb.example.com:30001}}`.

| Rule | Behaviour |
| --- | --- |
| Registering a built-in name | Rejected by the register method with `AerospikeException`. Built-in discovery names: `static`, `dns`, `aws-tags`. Built-in translation names: `static-map`, `port-map`, `pass-through`. |
| Registering a name twice on one policy | Rejected by the register method with `AerospikeException`. |
| Copying the policy | `new ClientPolicy(policy)` copies the registrations. |
| A name that is neither built-in nor registered | An invalid setting, see [Invalid settings](#invalid-settings). |
| The factory throws or returns null | An invalid setting, see [Invalid settings](#invalid-settings). |
| When the factory is called | Once, at client construction, on the thread that constructs the client. A reload never calls it, even when the name or options changed; the client keeps the instance it was constructed with. |

`static_map` is valid only with `static-map` and `port_map` only with `port-map`, so a block
naming a registered translator cannot carry them.

The client closes the provider it was constructed with: after `AerospikeClient.close()`,
`SeedCandidateProvider.close()` is called once on the tend thread as it exits, after the last
refresh has returned. `AerospikeClient.close()` does not wait for it.

## Invalid settings

A setting below is invalid. An invalid setting makes the whole file invalid, which is
handled like any other file the client cannot parse: a warning naming the key or value
is logged, nothing from the file is applied, and the client starts on the config set
in code.

- an unknown value for `discovery.merge_policy` or `translation.on_miss`;
- a `discovery.provider` or `translation.provider` that is neither built-in nor registered
  on the `ClientPolicy` passed to the client constructor;
- a registered factory that throws or returns null;
- `discovery.execution_mode` or `discovery.refresh_timeout_ms`;
- `discovery.provider: aws-tags` without a `discovery.aws` block, or a `discovery.aws` block with
  any other provider or with no provider;
- a `discovery.aws` block without `region` or `filters`, with an empty `filters`, a filter
  without `name` or with an empty `values`, an unknown `address_field`, or with `port` outside
  1 to 65535 or `timeout_ms` or `init_timeout_ms` below 1;
- a `discovery.aws` value that the `AwsEc2TagSeedCandidateProvider` builder rejects, such as a
  `region` that is not a region name, an `endpoint` that is not an `http` or `https` URL, or a
  filter named `instance-state-name`. This is checked when the provider is built at client
  construction (`Invalid static.client.discovery.aws: ...`);
- `discovery.provider: dns` without a `discovery.dns` block, or a `discovery.dns` block with
  any other provider or with no provider;
- a `discovery.dns` block without `names`, with an empty `names`, with an unknown
  `record_type`, or with `port` outside 1 to 65535 or `timeout_ms` below 1;
- a `discovery.dns` value that the `DnsSeedCandidateProvider` builder rejects, such as a
  duplicate name or a `servers` entry that is not an IP address. This is checked when the
  provider is built at client construction (`Invalid static.client.discovery.dns: ...`);
- `static_map` with a provider other than `static-map`, `port_map` with one other than
  `port-map`, or `provider: port-map` without `port_map`;
- `static_map` and `ip_map` both set;
- a value of the wrong type, such as a string for `refresh_interval_ms`, `dns.port` or `aws.port`;
- a `port_map` entry that the `PortMapAddressTranslator` constructor rejects.

Example warning:

```
Error -1,0: Failed to parse file:///etc/aerospike/client.yaml: com.aerospike.client.AerospikeException: Error -1,0: Invalid static.client.translation.on_miss value 'drop': valid values: pass_through, strict
```

A provider or translator name is checked against the policy's registrations when the file is
applied at client construction. The valid values listed are the built-in names that YAML
accepts followed by the registered names:

```
Failed to parse /etc/aerospike/client.yaml: com.aerospike.client.AerospikeException: Error -1,0: Invalid static.client.translation.provider value 'nat': valid values: static-map, port-map, pass-through, nat-table
```

Unknown keys are ignored everywhere in the file, including inside `discovery` and
`translation`; the rest of the file applies.

## Reload

The client checks the file for changes every `static.client.config_interval`. As for
the other `static.client` keys, a changed value is copied into the client's merged
`ClientPolicy` and logged (a registered provider or translator and the `dns` and `aws-tags`
providers are not rebuilt: they are built only at construction, and a reload that changes the
`dns` or `aws` block keeps the provider built from the original block), but the running cluster keeps the translator, `ip_map`,
`on_miss`, seed provider and discovery settings it was constructed with. They take
effect for a new client instance. `useServicesAlternate` is the exception: the cluster
re-reads it on every reload because `dynamic.client.use_service_alternative` is dynamic,
so a changed `use_services_alternate` takes effect for the running cluster after the reload.

A reloaded file with an invalid setting is logged at warning
(`Dynamic configuration failed: ...` naming the key or value) and the last good config
stays in use.
