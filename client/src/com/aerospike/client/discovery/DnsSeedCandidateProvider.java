/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.discovery;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.ServiceUnavailableException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;

/**
 * Seed candidate provider that resolves one or more DNS names to seed hosts on every refresh.
 * Use it when the seed hosts are published in DNS, including the split-network case where a seed
 * name resolves only on the client's network to addresses the client can reach.
 * <p>
 * Record types:
 * <ul>
 * <li>{@link RecordType#A}: A and AAAA records. One host per address, with {@link Builder#port}.</li>
 * <li>{@link RecordType#SRV}: SRV records. One host per record with the record's port, ordered by
 * priority (lowest first), then weight (highest first). Each record's target is resolved within
 * the same refresh by an A query, then an AAAA query when the target has no A record, and returned
 * as an IP address; a target that does not resolve in time is dropped and logged at warning level.
 * Addresses in the additional section of the SRV answer are not used: the JDK DNS provider exposes
 * only the answer section.</li>
 * </ul>
 * Every returned host is an IP address literal, never a hostname, so the client does no further
 * name resolution when it seeds from the result. Hosts are deduplicated and returned in the same
 * order for the same DNS answers. {@link Builder#tlsName} is set on every host.
 * <p>
 * Queries go to {@link Builder#servers}, or to the system resolvers read once when the provider
 * is built, through the JDK DNS provider ({@code com.sun.jndi.dns.DnsContextFactory}, module
 * {@code jdk.naming.dns}). All I/O of one refresh is bounded by {@link Builder#timeout}, and the
 * provider starts no thread.
 * <p>
 * The provider needs periodic refresh and runs on the cluster tend thread, so {@link Builder#timeout}
 * must not exceed {@link com.aerospike.client.policy.ClientPolicy#discoveryTendRefreshDeadline};
 * client construction fails otherwise. With the default deadline of 100ms, only a local resolver
 * answers reliably in time.
 * <p>
 * Failure: when some names resolve, their hosts are returned and each failed name is logged at
 * warning level. When every name fails (NXDOMAIN, SERVFAIL, timeout or an empty answer), the refresh
 * throws an {@link AerospikeException} naming each name and its failure. The client then keeps the
 * previous seed list, or at initialization continues with the seeds left by
 * {@link com.aerospike.client.policy.ClientPolicy#discoveryMergePolicy}.
 * <p>
 * Split-network example: {@code seeds.aerospike.internal} resolves only on the client's network.
 * <pre>{@code
 * ClientPolicy policy = new ClientPolicy();
 * policy.seedCandidateProvider = DnsSeedCandidateProvider.builder()
 *     .names("seeds.aerospike.internal")
 *     .port(3000)
 *     .timeout(50)
 *     .servers("10.0.0.2")
 *     .build();
 * AerospikeClient client = new AerospikeClient(policy);
 * }</pre>
 */
public final class DnsSeedCandidateProvider implements SeedCandidateProvider {
	/**
	 * DNS record type queried for each name.
	 */
	public enum RecordType {
		/**
		 * A and AAAA records.
		 */
		A,

		/**
		 * SRV records. Targets are resolved with A, then AAAA, queries.
		 */
		SRV
	}

	/**
	 * Default port for A and AAAA results.
	 */
	public static final int DEFAULT_PORT = 3000;

	/**
	 * Default refresh timeout in milliseconds.
	 */
	public static final int DEFAULT_TIMEOUT = 80;

	private static final int DNS_PORT = 53;
	private static final String FACTORY = "com.sun.jndi.dns.DnsContextFactory";
	private static final String TIMEOUT_PROPERTY = "com.sun.jndi.dns.timeout.initial";
	private static final String RETRIES_PROPERTY = "com.sun.jndi.dns.timeout.retries";
	private static final String[] A_TYPES = new String[] {"A", "AAAA"};

	private final List<String> names;
	private final RecordType recordType;
	private final int port;
	private final String tlsName;
	private final int timeout;
	private final List<String> servers;
	private final String providerUrl;
	private final int serverCount;

	/**
	 * Return a builder.
	 */
	public static Builder builder() {
		return new Builder();
	}

	private DnsSeedCandidateProvider(Builder builder) {
		if (builder.names == null || builder.names.isEmpty()) {
			throw new AerospikeException("Invalid DNS seed provider names: at least one name is required");
		}

		LinkedHashSet<String> set = new LinkedHashSet<>();

		for (String name : builder.names) {
			if (name == null || name.isEmpty() || name.equals(".") || name.contains(" ")) {
				throw new AerospikeException("Invalid DNS seed provider name: '" + name + "'");
			}
			if (! set.add(absolute(name).toLowerCase(Locale.ROOT))) {
				throw new AerospikeException("Invalid DNS seed provider names: duplicate name '" + name + "'");
			}
		}

		if (builder.recordType == null) {
			throw new AerospikeException("Invalid DNS seed provider recordType: null");
		}

		if (builder.port <= 0 || builder.port > 65535) {
			throw new AerospikeException("Invalid DNS seed provider port: " + builder.port);
		}

		if (builder.tlsName != null && builder.tlsName.isEmpty()) {
			throw new AerospikeException("Invalid DNS seed provider tlsName: empty");
		}

		if (builder.timeout <= 0) {
			throw new AerospikeException("Invalid DNS seed provider timeout: " + builder.timeout);
		}

		this.names = Collections.unmodifiableList(new ArrayList<>(builder.names));
		this.recordType = builder.recordType;
		this.port = builder.port;
		this.tlsName = builder.tlsName;
		this.timeout = builder.timeout;

		if (builder.servers != null && ! builder.servers.isEmpty()) {
			StringBuilder sb = new StringBuilder();

			for (String server : builder.servers) {
				if (sb.length() > 0) {
					sb.append(' ');
				}
				sb.append(serverUrl(server));
			}
			this.servers = Collections.unmodifiableList(new ArrayList<>(builder.servers));
			this.providerUrl = sb.toString();
		}
		else {
			this.servers = Collections.emptyList();
			this.providerUrl = systemProviderUrl();
		}
		this.serverCount = providerUrl.split(" ").length;
	}

	/**
	 * Resolve every name and return the seed hosts.
	 *
	 * @throws AerospikeException	if every name fails. The message names each name and its failure.
	 */
	@Override
	public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
		LinkedHashSet<Host> hosts = new LinkedHashSet<>();
		Map<String,String> failures = new LinkedHashMap<>();

		for (String name : names) {
			try {
				List<Host> list = (recordType == RecordType.SRV)? resolveSrv(name, deadline) : resolveA(name, deadline);

				if (list.isEmpty()) {
					failures.put(name, "empty answer");
				}
				hosts.addAll(list);
			}
			catch (NamingException e) {
				failures.put(name, reason(e));
			}
		}

		if (hosts.isEmpty()) {
			StringBuilder sb = new StringBuilder("DNS seed discovery failed for every name:");

			for (Map.Entry<String,String> entry : failures.entrySet()) {
				sb.append(' ').append(entry.getKey()).append(": ").append(entry.getValue()).append(';');
			}
			sb.setLength(sb.length() - 1);
			throw new AerospikeException(sb.toString());
		}

		if (Log.warnEnabled()) {
			for (Map.Entry<String,String> entry : failures.entrySet()) {
				Log.warn("DNS seed name " + entry.getKey() + " failed: " + entry.getValue());
			}
		}

		if (Log.debugEnabled()) {
			Log.debug("DNS seed names " + names + " resolved to " + hosts);
		}
		return new ArrayList<>(hosts);
	}

	private List<Host> resolveA(String name, long deadline) throws NamingException {
		List<String> addresses = addresses(name, deadline, false);
		List<Host> list = new ArrayList<>(addresses.size());

		for (String address : addresses) {
			list.add(new Host(address, tlsName, port));
		}
		return list;
	}

	private List<Host> resolveSrv(String name, long deadline) throws NamingException {
		List<Srv> records = new ArrayList<>();

		for (String value : query(name, "SRV", deadline)) {
			Srv srv = Srv.parse(value);

			if (srv == null) {
				if (Log.warnEnabled()) {
					Log.warn("DNS seed name " + name + " returned an invalid SRV record: " + value);
				}
				continue;
			}

			if (! srv.target.equals(".")) {
				records.add(srv);
			}
		}

		if (records.isEmpty()) {
			return Collections.emptyList();
		}

		records.sort(Srv.ORDER);

		Map<String,String> targets = new HashMap<>();
		List<Host> list = new ArrayList<>(records.size());

		for (Srv srv : records) {
			String address = targets.get(srv.target);

			if (address == null && ! targets.containsKey(srv.target)) {
				try {
					List<String> addresses = addresses(srv.target, deadline, true);

					if (addresses.isEmpty()) {
						throw new NamingException("empty answer");
					}
					address = addresses.get(0);
				}
				catch (NamingException e) {
					if (Log.warnEnabled()) {
						Log.warn("DNS seed name " + name + " SRV target " + srv.target + " dropped: " + reason(e));
					}
				}
				targets.put(srv.target, address);
			}

			if (address != null) {
				list.add(new Host(address, tlsName, srv.port));
			}
		}

		if (list.isEmpty()) {
			throw new NamingException("no SRV target resolved");
		}
		return list;
	}

	private List<String> addresses(String name, long deadline, boolean first) throws NamingException {
		List<String> addresses = new ArrayList<>();
		NamingException error = null;

		for (String type : A_TYPES) {
			if (first && ! addresses.isEmpty()) {
				break;
			}

			try {
				List<String> values = query(name, type, deadline);
				Collections.sort(values);
				addresses.addAll(values);
			}
			catch (NameNotFoundException e) {
				throw e;
			}
			catch (NamingException e) {
				error = e;
			}
		}

		if (addresses.isEmpty() && error != null) {
			throw error;
		}
		return addresses;
	}

	private List<String> query(String name, String type, long deadline) throws NamingException {
		long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());

		if (remaining <= 0) {
			throw new DeadlineException();
		}

		Hashtable<String,String> env = new Hashtable<>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, FACTORY);
		env.put(Context.PROVIDER_URL, providerUrl);
		env.put(TIMEOUT_PROPERTY, Long.toString(Math.max(1, remaining / serverCount)));
		env.put(RETRIES_PROPERTY, "1");

		DirContext dir = new InitialDirContext(env);

		try {
			Attribute attr = dir.getAttributes(absolute(name), new String[] {type}).get(type);
			List<String> values = new ArrayList<>();

			if (attr != null) {
				NamingEnumeration<?> e = attr.getAll();

				while (e.hasMore()) {
					values.add(e.next().toString());
				}
			}
			return values;
		}
		finally {
			dir.close();
		}
	}

	private static String absolute(String name) {
		return name.endsWith(".")? name : name + '.';
	}

	private static String reason(NamingException e) {
		if (e instanceof NameNotFoundException) {
			return "NXDOMAIN";
		}
		if (e instanceof ServiceUnavailableException) {
			return "SERVFAIL";
		}
		if (e instanceof DeadlineException ||
			(e instanceof CommunicationException && e.getRootCause() instanceof SocketTimeoutException)) {
			return "timeout";
		}
		String reason = (e.getExplanation() != null)? e.getExplanation() : e.getClass().getName();
		return (e.getRootCause() != null)? reason + ": " + e.getRootCause() : reason;
	}

	private static String serverUrl(String server) {
		if (server == null) {
			throw new AerospikeException("Invalid DNS seed provider server: null");
		}

		String host;
		String portString = null;

		if (server.startsWith("[")) {
			int end = server.indexOf(']');

			if (end < 0) {
				throw new AerospikeException("Invalid DNS seed provider server: '" + server + "'");
			}
			host = server.substring(1, end);

			if (end + 1 < server.length()) {
				if (server.charAt(end + 1) != ':') {
					throw new AerospikeException("Invalid DNS seed provider server: '" + server + "'");
				}
				portString = server.substring(end + 2);
			}
		}
		else if (server.indexOf(':') != server.lastIndexOf(':')) {
			host = server;
		}
		else {
			int index = server.indexOf(':');

			if (index >= 0) {
				host = server.substring(0, index);
				portString = server.substring(index + 1);
			}
			else {
				host = server;
			}
		}

		boolean ipv6 = host.indexOf(':') >= 0;

		if (! (ipv6? isIPv6(host) : isIPv4(host))) {
			throw new AerospikeException("Invalid DNS seed provider server: '" + server + "': expected an IP address");
		}

		int serverPort = DNS_PORT;

		if (portString != null) {
			try {
				serverPort = Integer.parseInt(portString);
			}
			catch (NumberFormatException e) {
				serverPort = -1;
			}

			if (serverPort <= 0 || serverPort > 65535) {
				throw new AerospikeException("Invalid DNS seed provider server: '" + server + "': invalid port");
			}
		}
		return "dns://" + (ipv6? "[" + host + "]" : host) + ':' + serverPort;
	}

	private static boolean isIPv4(String host) {
		String[] parts = host.split("\\.", -1);

		if (parts.length != 4) {
			return false;
		}

		for (String part : parts) {
			if (part.isEmpty() || part.length() > 3) {
				return false;
			}

			for (int i = 0; i < part.length(); i++) {
				if (! Character.isDigit(part.charAt(i))) {
					return false;
				}
			}

			if (Integer.parseInt(part) > 255) {
				return false;
			}
		}
		return true;
	}

	private static boolean isIPv6(String host) {
		int doubleColon = host.indexOf("::");

		if (doubleColon >= 0 && host.indexOf("::", doubleColon + 1) >= 0) {
			return false;
		}

		String[] groups = host.split(":", -1);
		int count = 0;

		for (int i = 0; i < groups.length; i++) {
			String group = groups[i];

			if (group.isEmpty()) {
				continue;
			}

			if (i == groups.length - 1 && group.indexOf('.') >= 0) {
				if (! isIPv4(group)) {
					return false;
				}
				count += 2;
				continue;
			}

			if (group.length() > 4) {
				return false;
			}

			for (int j = 0; j < group.length(); j++) {
				if (Character.digit(group.charAt(j), 16) < 0) {
					return false;
				}
			}
			count++;
		}
		return (doubleColon >= 0)? count < 8 : count == 8 && groups.length >= 8;
	}

	private static String systemProviderUrl() {
		Hashtable<String,String> env = new Hashtable<>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, FACTORY);
		env.put(Context.PROVIDER_URL, "dns:");

		try {
			DirContext dir = new InitialDirContext(env);

			try {
				Object url = dir.getEnvironment().get(Context.PROVIDER_URL);

				if (!(url instanceof String) || ! ((String)url).startsWith("dns://")) {
					throw new AerospikeException("DNS seed provider: no system DNS server found. Set servers");
				}
				return ((String)url).trim();
			}
			finally {
				dir.close();
			}
		}
		catch (NamingException e) {
			throw new AerospikeException("DNS seed provider: failed to read system DNS servers: " + e, e);
		}
	}

	/**
	 * Return the DNS names, in configured order.
	 */
	public List<String> getNames() {
		return names;
	}

	/**
	 * Return the record type queried for each name.
	 */
	public RecordType getRecordType() {
		return recordType;
	}

	/**
	 * Return the port used for A and AAAA results.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Return the TLS name set on every returned host. May be null.
	 */
	public String getTlsName() {
		return tlsName;
	}

	/**
	 * Return the total timeout in milliseconds for one refresh.
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Return the configured resolver addresses. Empty when the system resolvers are used.
	 */
	public List<String> getServers() {
		return servers;
	}

	/**
	 * Return true. DNS answers can change.
	 */
	@Override
	public boolean needsPeriodicRefresh() {
		return true;
	}

	/**
	 * Return true. All I/O of one refresh is bounded by {@link #getTimeout()}.
	 */
	@Override
	public boolean supportsDeadline() {
		return true;
	}

	@Override
	public String toString() {
		return "dns " + names + ' ' + recordType;
	}

	/**
	 * Builder for {@link DnsSeedCandidateProvider}. Values are validated by {@link #build()}.
	 */
	public static final class Builder {
		private List<String> names;
		private RecordType recordType = RecordType.A;
		private int port = DEFAULT_PORT;
		private String tlsName;
		private int timeout = DEFAULT_TIMEOUT;
		private List<String> servers;

		private Builder() {
		}

		/**
		 * DNS names to resolve. Required: one or more names, no duplicates. A name is queried as an
		 * absolute name; a trailing dot is optional.
		 */
		public Builder names(String... names) {
			this.names = (names != null)? Arrays.asList(names.clone()) : null;
			return this;
		}

		/**
		 * DNS names to resolve. See {@link #names(String...)}.
		 */
		public Builder names(List<String> names) {
			this.names = (names != null)? new ArrayList<>(names) : null;
			return this;
		}

		/**
		 * Record type queried for each name.
		 * <p>
		 * Default: {@link RecordType#A} (A and AAAA)
		 */
		public Builder recordType(RecordType recordType) {
			this.recordType = recordType;
			return this;
		}

		/**
		 * Port set on hosts resolved from A and AAAA records. Ignored for {@link RecordType#SRV},
		 * which uses each record's port.
		 * <p>
		 * Default: {@link DnsSeedCandidateProvider#DEFAULT_PORT}
		 */
		public Builder port(int port) {
			this.port = port;
			return this;
		}

		/**
		 * TLS name set on every returned host. Null leaves the TLS name to be defaulted the same way
		 * as for configured seeds.
		 * <p>
		 * Default: null
		 */
		public Builder tlsName(String tlsName) {
			this.tlsName = tlsName;
			return this;
		}

		/**
		 * Total time in milliseconds for one refresh, across every name, record type, SRV target
		 * and resolver. Must be greater than zero, and must not exceed
		 * {@link com.aerospike.client.policy.ClientPolicy#discoveryTendRefreshDeadline}; the
		 * latter is checked at client construction.
		 * <p>
		 * Default: {@link DnsSeedCandidateProvider#DEFAULT_TIMEOUT}
		 */
		public Builder timeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		/**
		 * Resolver addresses, each an IP address with an optional port: {@code ip[:port]}, with an
		 * IPv6 address in brackets when a port is given ({@code [2001:db8::1]:53}). Resolvers are
		 * tried in order. Null or empty uses the system resolvers, read once by {@link #build()}.
		 * <p>
		 * Default: null (system resolvers, port 53)
		 */
		public Builder servers(String... servers) {
			this.servers = (servers != null)? Arrays.asList(servers.clone()) : null;
			return this;
		}

		/**
		 * Resolver addresses. See {@link #servers(String...)}.
		 */
		public Builder servers(List<String> servers) {
			this.servers = (servers != null)? new ArrayList<>(servers) : null;
			return this;
		}

		/**
		 * Validate the settings and build the provider.
		 *
		 * @throws AerospikeException	if a setting is invalid. The message names the setting and value.
		 */
		public DnsSeedCandidateProvider build() {
			return new DnsSeedCandidateProvider(this);
		}
	}

	private static final class Srv {
		static final Comparator<Srv> ORDER = Comparator.comparingInt((Srv s) -> s.priority)
			.thenComparing(Comparator.comparingInt((Srv s) -> s.weight).reversed())
			.thenComparing(s -> s.target)
			.thenComparingInt(s -> s.port);

		final int priority;
		final int weight;
		final int port;
		final String target;

		private Srv(int priority, int weight, int port, String target) {
			this.priority = priority;
			this.weight = weight;
			this.port = port;
			this.target = target;
		}

		static Srv parse(String value) {
			String[] fields = value.trim().split("\\s+");

			if (fields.length != 4) {
				return null;
			}

			try {
				int port = Integer.parseInt(fields[2]);

				if (port <= 0 || port > 65535) {
					return null;
				}
				return new Srv(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), port,
					fields[3].toLowerCase(Locale.ROOT));
			}
			catch (NumberFormatException e) {
				return null;
			}
		}
	}

	private static final class DeadlineException extends NamingException {
		private static final long serialVersionUID = 1L;

		DeadlineException() {
			super("timeout");
		}
	}
}
