# Writing a seed candidate provider or address translator

This guide is for writing a custom `SeedCandidateProvider` or `AddressTranslator`, both in
`com.aerospike.client.discovery`. The YAML keys, the built-in names and reload are covered in
[YAML configuration: discovery and translation](yaml-config-discovery.md).

A provider returns the seed hosts the client tries when it initializes the cluster and when it
re-seeds because no node is active. A translator rewrites each address a server advertises into
the address the client connects to.

The Java client runs discovery on the cluster tend thread. It starts no thread for discovery, and
a provider or translator must not start one either.

## Thread context

`SeedCandidateProvider`:

| Method | Thread | When | What it may do |
| --- | --- | --- | --- |
| `refreshSeedCandidates(ctx)` | The application thread that constructs the client | Once, at construction, with `ctx.tendIteration` 0, before the cluster is seeded | Return the seed hosts. No deadline is measured here, but the constructor waits for the call, so bound any I/O. |
| `refreshSeedCandidates(ctx)` | The tend thread | Every `discoveryRefreshInterval / tendInterval` tend iterations (integer division), when `needsPeriodicRefresh()` returned true, until the provider is disabled | Return the seed hosts within `discoveryTendRefreshDeadline`: in-memory work, or I/O bounded by that deadline. |
| `needsPeriodicRefresh()` | The application thread that constructs the client | Once | Return a constant. |
| `supportsDeadline()` | The application thread that constructs the client | At most once, only when `needsPeriodicRefresh()` returned true | Return a constant. |
| `close()` | The tend thread as it exits; the thread that calls `AerospikeClient.close()` when no tend thread runs (`forceSingleNode`) | Once per client, after `AerospikeClient.close()` and after the last refresh has returned. `AerospikeClient.close()` does not wait for it | Release resources. An exception is logged at warning level and not thrown. A provider shared by several clients is closed by each of them. |

`AddressTranslator`:

| Method | Thread | When | What it may do |
| --- | --- | --- | --- |
| `translate(advertised)` | The application thread that constructs the client, while it seeds the cluster; the tend thread afterwards | For each advertised address the client reads: when it validates a seed, and when it reads a node's peers list after the list changed | In-memory lookup only. No I/O of any kind, including DNS resolution. |
| `onClusterViewChanged(viewChange)` | The tend thread | When a peers refresh adds or removes a node or changes the address a node advertises; never for the view established at construction. Called only when the translator overrides it | In-memory only, for example dropping cached state. An exception is logged at warning level and does not stop the tend. |

A registered factory (see [Registered names](yaml-config-discovery.md#registered-names)) is called
once, on the application thread that constructs the client. Build lookup tables there, so that
`translate()` only reads them.

`SeedCandidateContext` carries `clusterName` (`ClientPolicy.clusterName`, may be null),
`configuredSeeds` (the hosts passed to the client constructor, unmodifiable) and `tendIteration`
(0 at construction, then the tend count; informational only).

### Periodic refresh and the tend deadline

A provider whose seeds can change returns true from `needsPeriodicRefresh()`. Such a provider must
also return true from `supportsDeadline()` and bound all of its I/O to
`ClientPolicy.discoveryTendRefreshDeadline` (default 100 ms), because every refresh after
construction runs on the tend thread. Client construction fails when:

| Case | Error |
| --- | --- |
| `needsPeriodicRefresh()` is true and `supportsDeadline()` is false | `A seed candidate provider that needs periodic refresh must support a deadline` |
| `discoveryRefreshInterval` is less than `tendInterval` | `Discovery refresh interval ... must be greater or equal to the tend interval ...` |
| `discoveryTendRefreshDeadline` is 0 or less, or greater than `tendInterval / 2` | `Invalid discovery tend refresh deadline: ...` |

These checks apply only to a provider that needs periodic refresh. A provider that returns false
from `needsPeriodicRefresh()` is called once, at construction, and never on the tend thread.

The deadline is measured, not preempted: the client times the call after it returns and never
interrupts it, so an overrunning call blocks the tend for its full duration.

| Case | Behaviour |
| --- | --- |
| The call returns within the deadline | Its result is merged into the seed list, see [Merge policy](#merge-policy). The overrun count is reset to 0. |
| The call takes longer than the deadline (an overrun) | The result is discarded, even when the call succeeded, and the previous seed list is kept. The warning `Seed candidate refresh took 140ms, exceeding tend deadline 100ms. Consecutive overruns: 1` is logged. |
| Three consecutive overruns | The provider is disabled for that client instance: it is not called from the tend again. The seed list is restored to the one established at construction, and `Seed candidate provider exceeded tend deadline 3 consecutive times. Provider disabled for this client and initial seeds restored` is logged at error level. A new client instance starts with no overruns. |

A refresh that throws:

| When | Behaviour |
| --- | --- |
| At construction | The warning `Seed candidate provider <name> failed: <message>` is logged, the call counts as an empty result and the client continues with the seeds the merge policy leaves. When none are left, construction fails with `SEED_DISCOVERY_FAILED` (-27). `<name>` is `static`, `dns` or the provider's class name. |
| On the tend | The warning `Seed candidate refresh failed: <message>` is logged and the previous seed list is kept. A call that throws after the deadline also counts as an overrun; one that throws within it resets the count. |

Returned hosts are used as seeds, so they must carry the TLS name when TLS is enabled. A null TLS
name is defaulted as for the seeds passed to the constructor: the cluster name when
`ClientPolicy.clusterName` is set, otherwise the host name.

## Built-in providers

| Provider | Periodic refresh | Deadline |
| --- | --- | --- |
| `static` (`StaticSeedCandidateProvider`) | No. `needsPeriodicRefresh()` returns false and the provider does no I/O; it is called once, at construction. | None applies. |
| `dns` (`DnsSeedCandidateProvider`) | Yes. `needsPeriodicRefresh()` and `supportsDeadline()` return true, and all I/O of one refresh is bounded by its `timeout` (`dns.timeout_ms`, default 80 ms). | `timeout` must not exceed `discoveryTendRefreshDeadline`. This is checked at client construction, which fails otherwise: `DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100`. |

When no provider is configured, the client uses `StaticSeedCandidateProvider` over the hosts
passed to its constructor.

## Writing a translator

`translate(Endpoint advertised)` returns the host to connect to for an advertised endpoint:

- Return `Optional.of(host)` for a hit, even when the host equals the advertised one.
- Return `Optional.empty()` for a miss and let the client apply `translation.on_miss`
  (`ClientPolicy.translationOnMiss`): `pass_through` connects to the advertised endpoint
  unchanged; `strict` does not connect, fails the endpoint with `AerospikeException.TranslationMiss`
  (`TRANSLATION_MISS`, -25) and, for a peer, logs a warning and retries it on the next tend. Do not
  implement either behaviour in the translator.
- Never return null. The client treats null as an error: `Address translator <name> returned null for <endpoint>`.

`translate()` and `onClusterViewChanged()` run on the tend thread (`translate()` also on the
constructing thread while the cluster is seeded) and must work in memory only, with no I/O of
any kind, including DNS resolution. A translator may return a host name: the client resolves it
when it connects. Aim for a p99 under 1 ms per `translate()` call.

`Endpoint` fields:

| Field | Meaning |
| --- | --- |
| `nodeName` | Name of the node that advertised the address. |
| `tlsName` | TLS name of the node. May be null. |
| `host`, `port` | The advertised host name or IP address, and port. |
| `sourceList` | `ALTERNATE` when `ClientPolicy.useServicesAlternate` is true: the address comes from the alternate lists (`peers-clear-alt`, `peers-tls-alt`, `service-clear-alt`, `service-tls-alt`), set by the server's `alternate-access-address` and `alternate-access-port`. `STANDARD` otherwise: the standard lists (`peers-clear-std`, `peers-tls-std`, `service-clear-std`, `service-tls-std`). |

Key the translator's table on the list in use: a table written against the standard list misses
on every address when the client reads the alternate list. `use_services_alternate` is re-read on
every config reload, so the list can change during the life of a client.

The client rejects two peers translated to the same `host:port`, within one peers reply or
against a live node with a different name, with `AerospikeException.TranslationCollision`
(`TRANSLATION_COLLISION`, -26): the collision is logged at warning level, no node is created for
the colliding peers and an existing live node is kept. A translator does not check for
collisions.

## TLS

`translate()` never changes the TLS name. The client always uses the advertised endpoint's TLS
name, whatever TLS name the returned host carries; on a `pass_through` miss it uses the advertised
endpoint unchanged. The certificate the server presents through the translated address is
therefore matched against the node's advertised TLS name, not against the host the translator
returned.

The certificate is matched against the TLS name as follows:

- A TLS name that is an IPv4 or IPv6 literal matches only an `iPAddress` subject alternative name
  holding the same address.
- Any other TLS name matches only a `dNSName` subject alternative name, compared
  case-insensitively (ASCII).
- The only wildcard is one left-most label of exactly `*`, matching one non-empty label:
  `*.example.com` matches `a.example.com`, but not `example.com` or `a.b.example.com`.
- The subject CN is not an identifier.

Deprecated fallback: when those rules find no match, the certificate is still accepted if the TLS
name exactly equals (case-sensitive) a subject CN or a `dNSName`, for example a CN-only
certificate or an IP address present only as a `dNSName`. A warning is logged once per TLS name:

```
TLS name 'node1' matched the server certificate only by the legacy rule (subject CN or IP address as dNSName). This is not RFC 9525 conformant and will be rejected in the next major release.
```

The Java client keeps this fallback until its next major release, which will reject such
certificates. The C client (and the Node.js and Python clients, which are built on it) and the C#
client also keep CN acceptance until their next major release.

## Merge policy

`ClientPolicy.discoveryMergePolicy` (YAML `discovery.merge_policy`) combines the static seeds (the
hosts passed to the client constructor) and the discovered seeds (the hosts the provider returns)
into the seed list. Duplicate hosts (same name and port) are removed, keeping the first
occurrence. The policy orders seed attempts only; it does not change node identity.

| Policy | At initialization | On a periodic refresh | On re-seed with zero active nodes | Use when |
| --- | --- | --- | --- | --- |
| `MERGE` (`merge`, default) | Static seeds, then discovered seeds. | The list becomes the static seeds, then the discovered seeds. An empty result leaves only the static seeds. | Tries the static seeds, then the discovered seeds of the last refresh that was applied, or of initialization before the first one. | Moving an existing deployment to discovery: the static seeds stay a known-good endpoint while discovery catches up. |
| `REPLACE` (`replace`) | Static seeds, then discovered seeds, as `MERGE`. | The discovered seeds replace the list, and the static seeds are no longer tried. An empty result keeps the previous list. | Tries the discovered seeds of the last refresh that returned hosts; before the first one, the list from initialization. | A new cloud deployment where the static seeds only bootstrap the client, for example a seed name that is retired once discovery works. |
| `DISCOVERY_ONLY` (`discovery_only`) | Discovered seeds only; the static seeds when discovery returns none. | The list becomes the discovered seeds; the static seeds when the result is empty. | Tries the seeds of the last refresh that was applied: discovered seeds, or the static seeds when that refresh returned none. | The static seeds are an emergency fallback only, for example a fixed address of a standby site that should be tried only when discovery comes back empty. |

For every policy:

- The seed list is rebuilt only when the provider is called. A re-seed does not call the provider:
  it tries the current seed list in order.
- A refresh that throws or overruns the deadline keeps the previous list. A refresh whose merged
  list is empty keeps the previous list.
- A provider that throws at initialization counts as an empty result. When the merged list is
  then empty, construction fails with `SEED_DISCOVERY_FAILED` (-27).
- After construction, the client appends the addresses of the nodes it found to the seed list.
  They are tried on re-seed until a refresh is applied, which replaces the whole list.
- When the provider is disabled after three consecutive overruns, the list from initialization is
  restored and kept for the life of the client.
- Without a configured provider, every policy yields the static seeds.

## Deployment limits

The following applies to AWS only and comes from the load balancer, not from the client: neither
translator limits the number of nodes.

A Network Load Balancer has at most 50 listeners, a quota AWS does not raise, and each listener
serves a single port. A layout that gives every node its own port on one endpoint therefore
reaches at most 50 nodes per endpoint, whether clients reach the load balancer directly or through
an AWS PrivateLink endpoint service. This applies to:

- `pass-through` with `alternate-access-port`, where every node advertises the endpoint's name and
  its own port;
- `port-map`, where every node maps to the endpoint's name and its own port.

Above 50 nodes, spread the nodes over several endpoint services, each with its own load balancer
and endpoint name, or use VPC Lattice resource endpoints.

## Examples

A minimal provider and translator, registered in code and selected from YAML by name. The factory
receives the keys of the YAML block other than `provider` as its options map, see
[Registered names](yaml-config-discovery.md#registered-names).

The provider returns a host list that the application updates through `setHosts()`, for example
from a service registry callback it already runs. `refreshSeedCandidates()` only reads a field, so
the provider returns true from both `needsPeriodicRefresh()` and `supportsDeadline()`, does no I/O
on the tend thread and starts no thread:

```java
static final class HostListSeedProvider implements SeedCandidateProvider {
	private volatile List<Host> hosts;

	HostListSeedProvider(Map<String,Object> options) {
		List<Host> list = new ArrayList<>();

		for (Object host : (List<?>)options.get("hosts")) {
			list.addAll(Host.parseServiceHosts((String)host));
		}
		this.hosts = List.copyOf(list);
	}

	void setHosts(List<Host> hosts) {
		this.hosts = List.copyOf(hosts);
	}

	@Override
	public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
		return hosts;
	}

	@Override
	public boolean needsPeriodicRefresh() {
		return true;
	}

	@Override
	public boolean supportsDeadline() {
		return true;
	}
}
```

The translator maps an advertised `host:port` to the `host:port` to connect to. The table is
built once by the constructor, and `translate()` is a map lookup. `Optional.ofNullable` returns
`Optional.empty()` for an endpoint that is not in the table, so `on_miss` applies:

```java
static final class NatTableTranslator implements AddressTranslator {
	private final Map<String,Host> table = new HashMap<>();

	NatTableTranslator(Map<String,Object> options) {
		Map<?,?> map = (Map<?,?>)options.get("table");

		for (Map.Entry<?,?> entry : map.entrySet()) {
			table.put((String)entry.getKey(), Host.parseServiceHosts((String)entry.getValue()).get(0));
		}
	}

	@Override
	public Optional<Host> translate(Endpoint advertised) {
		return Optional.ofNullable(table.get(advertised.host + ':' + advertised.port));
	}
}
```

Register both names on the `ClientPolicy`, then pass it to the `AerospikeClient` constructor:

```java
ClientPolicy policy = new ClientPolicy();
policy.registerSeedCandidateProvider("host-list", HostListSeedProvider::new);
policy.registerAddressTranslator("nat-table", NatTableTranslator::new);
```

Select them from the YAML file:

```yaml
version: 1.2.0
static:
  client:
    discovery:
      provider: host-list
      merge_policy: merge
      hosts:
        - "192.0.2.1:3000"
        - "192.0.2.2:3000"
    translation:
      provider: nat-table
      on_miss: strict
      table:
        "192.0.2.1:3000": "lb.example.com:30001"
        "192.0.2.2:3000": "lb.example.com:30002"
```

With `on_miss: strict`, a node whose advertised address is not in `table` is not connected.
