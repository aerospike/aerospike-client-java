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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.util.Args;

/**
 * L2 topology built by .github/workflows/discovery-l2.yaml or test/discovery/run_l2.sh: a three node
 * cluster on a private network with no published ports, reachable from the client only through one
 * Toxiproxy listener per node behind one endpoint hostname. The client has no route to the node
 * addresses. The topology arrives as environment variables:
 * <ul>
 * <li>DISCOVERY_ENDPOINT_HOSTNAME: hostname every listener is behind</li>
 * <li>DISCOVERY_ENDPOINT_PORTS: clear listener ports, one per node</li>
 * <li>DISCOVERY_TLS_ENDPOINT_PORTS: TLS listener ports, one per node; empty on ce</li>
 * <li>DISCOVERY_PROXY_MAP: Toxiproxy proxy name to listener port</li>
 * <li>DISCOVERY_SUBSET: ce or ee</li>
 * <li>TOXIPROXY_URL: Toxiproxy control API</li>
 * <li>AEROSPIKE_CONTAINER_NAMES: server containers, one per node</li>
 * <li>DISCOVERY_CONTROL_DIR: optional directory shared with the host, which starts and removes
 * nodes on request; unset when the host offers no node control</li>
 * <li>DISCOVERY_REACHABLE_NODE: optional container of the node that advertises an address the
 * client can reach; set only by the reachable profile of run_l2.sh</li>
 * <li>DISCOVERY_PASS_THROUGH_BRANCH: optional, A when each node advertises its own
 * alternate-access-address hostname and B when every node advertises one hostname, each with its
 * own listener port; set only by the pass-through-names and pass-through-ports profiles</li>
 * <li>DISCOVERY_PORT_MAP_RANGE: optional {@code first-last} range of node addresses on the server
 * network, each with a listener on DISCOVERY_PORT_MAP_PORT_BASE plus its last octet; set only by
 * the port-map-range profile</li>
 * <li>DISCOVERY_TLS_CERT_SHAPE: optional, san-only when the server certificate carries the tls-name
 * only as a dNSName SAN and cn-only when it carries it only as the subject CN; set on ee</li>
 * </ul>
 * Each node's name and advertised addresses are read through its listener at runtime.
 */
final class L2Topology {
	static final int TEND_INTERVAL = 250;
	static final int WAIT_MS = 30000;
	static final String TLS_PROXY_SUFFIX = "-tls";

	private static final Pattern UPSTREAM_PORT = Pattern.compile("\"upstream\":\"[^\"]*:(\\d+)\"");
	private static final Pattern LISTEN_PORT = Pattern.compile("\"listen\":\"[^\"]*:(\\d+)\"");
	private static final Pattern PEER = Pattern.compile("\\[([0-9A-Fa-f]+),([^,\\[\\]]*),\\[((?:[^\\[\\]]|\\[[^\\]]*\\])*)\\]\\]");
	private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
	private static List<L2Node> nodes;

	static final String host = required("DISCOVERY_ENDPOINT_HOSTNAME");
	static final List<Integer> ports = ports(required("DISCOVERY_ENDPOINT_PORTS"));
	static final List<Integer> tlsPorts = ports(optional("DISCOVERY_TLS_ENDPOINT_PORTS"));
	static final Map<String,Integer> proxyPorts = proxyPorts(required("DISCOVERY_PROXY_MAP"));
	static final String subset = required("DISCOVERY_SUBSET");
	static final String toxiproxyUrl = required("TOXIPROXY_URL");
	static final List<String> containers = List.of(required("AEROSPIKE_CONTAINER_NAMES").split(","));
	static final String controlDir = optional("DISCOVERY_CONTROL_DIR");
	static final String reachableNode = optional("DISCOVERY_REACHABLE_NODE");
	static final String passThroughBranch = optional("DISCOVERY_PASS_THROUGH_BRANCH");
	static final String portMapRange = optional("DISCOVERY_PORT_MAP_RANGE");
	static final String portMapPortBase = optional("DISCOVERY_PORT_MAP_PORT_BASE");
	static final String tlsCertShape = optional("DISCOVERY_TLS_CERT_SHAPE");
	static final String namespace = Args.Instance.namespace;

	/**
	 * Node behind one listener, as it advertises itself.
	 */
	static final class L2Node {
		final String proxy;
		final int port;
		final String name;
		final List<String> advertised;
		final int tlsPort;
		final List<String> tlsAdvertised;
		String tlsName;

		L2Node(String proxy, int port, String name, List<String> advertised, int tlsPort, List<String> tlsAdvertised) {
			this.proxy = proxy;
			this.port = port;
			this.name = name;
			this.advertised = advertised;
			this.tlsPort = tlsPort;
			this.tlsAdvertised = tlsAdvertised;
		}

		@Override
		public String toString() {
			return name + " proxy=" + proxy + " listener=" + port + " advertised=" + advertised +
				(tlsPort > 0 ? " tlsListener=" + tlsPort + " tlsAdvertised=" + tlsAdvertised + " tlsName=" + tlsName : "");
		}
	}

	/**
	 * Peer as listed by a node's peers reply.
	 */
	static final class Peer {
		final String name;
		final String tlsName;
		final List<String> endpoints;

		Peer(String name, String tlsName, List<String> endpoints) {
			this.name = name;
			this.tlsName = tlsName;
			this.endpoints = endpoints;
		}
	}

	/**
	 * Query every listener for its node's name and advertised addresses. Read once per run.
	 */
	static List<L2Node> nodes() {
		if (nodes != null) {
			return nodes;
		}

		assertEquals("DISCOVERY_ENDPOINT_PORTS " + ports + " vs AEROSPIKE_CONTAINER_NAMES " + containers,
			containers.size(), ports.size());

		List<L2Node> list = new ArrayList<>();
		Map<String,String> tlsNames = new HashMap<>();

		String[] commands = tlsPorts.isEmpty() ? new String[] {"node", "service-clear-std"} :
			new String[] {"node", "service-clear-std", "service-tls-std", "peers-tls-std"};

		for (int port : ports) {
			String proxy = proxyName(port);
			Map<String,String> info = Info.request(host, port, commands);
			String name = info.get("node");
			assertNotNull("No node name from listener " + port, name);

			Integer tlsPort = proxyPorts.get(proxy + TLS_PROXY_SUFFIX);
			L2Node node = new L2Node(proxy, port, name, endpoints(info.get("service-clear-std")),
				tlsPort == null ? 0 : tlsPort, endpoints(info.get("service-tls-std")));
			assertFalse("Node " + name + " advertises no clear address", node.advertised.isEmpty());
			list.add(node);

			for (Peer peer : parsePeers(info.get("peers-tls-std"))) {
				if (! peer.tlsName.isEmpty()) {
					tlsNames.put(peer.name, peer.tlsName);
				}
			}
		}

		for (L2Node node : list) {
			node.tlsName = tlsNames.get(node.name);
			System.out.println("L2-NODE " + node);
		}
		nodes = Collections.unmodifiableList(list);
		return nodes;
	}

	static List<String> nodeNames() {
		List<String> names = new ArrayList<>();

		for (L2Node node : nodes()) {
			names.add(node.name);
		}
		names.sort(null);
		return names;
	}

	static Host seed() {
		return new Host(host, nodes().get(0).port);
	}

	static String listener(int port) {
		return host + ':' + port;
	}

	static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.tendInterval = TEND_INTERVAL;
		policy.timeout = 1000;
		return policy;
	}

	/**
	 * port-map from each node's advertised clear {@code host:port} to its listener.
	 */
	static Map<String,String> portMapByEndpoint(List<L2Node> list) {
		Map<String,String> map = new LinkedHashMap<>();

		for (L2Node node : list) {
			for (String advertised : node.advertised) {
				map.put(advertised, listener(node.port));
			}
		}
		return map;
	}

	/**
	 * port-map from each node's name to its listener.
	 */
	static Map<String,String> portMapByNodeName(List<L2Node> list) {
		Map<String,String> map = new LinkedHashMap<>();

		for (L2Node node : list) {
			map.put(node.name, listener(node.port));
		}
		return map;
	}

	/**
	 * port-map from each node's advertised TLS {@code host:port} to its TLS listener.
	 */
	static Map<String,String> tlsPortMap(List<L2Node> list) {
		Map<String,String> map = new LinkedHashMap<>();

		for (L2Node node : list) {
			for (String advertised : node.tlsAdvertised) {
				map.put(advertised, listener(node.tlsPort));
			}
		}
		return map;
	}

	static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	static boolean waitFor(BooleanSupplier condition) throws InterruptedException {
		return waitFor(condition, 100);
	}

	static boolean waitFor(BooleanSupplier condition, int pollMs) throws InterruptedException {
		long deadline = System.currentTimeMillis() + WAIT_MS;

		while (! condition.getAsBoolean()) {
			if (System.currentTimeMillis() >= deadline) {
				return false;
			}
			Thread.sleep(pollMs);
		}
		return true;
	}

	static void waitNodes(AerospikeClient client, List<String> names) throws InterruptedException {
		waitFor(() -> nodeNames(client).equals(names));
		assertEquals(names, nodeNames(client));
	}

	/**
	 * Wait until every partition of the namespace has an active master in the client's partition
	 * map and return the master of each partition. Same check as
	 * {@code PortMapCluster.assertAllPartitionsOwned}, which reads run_port_map.sh's system
	 * properties on load.
	 */
	static Node[] assertAllPartitionsOwned(String row, AerospikeClient client) throws InterruptedException {
		Node[] masters = new Node[Node.PARTITIONS];
		waitFor(() -> unowned(client, masters) == 0);
		int unowned = unowned(client, masters);

		System.out.println("L2-PARTITIONS " + row + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
			" owners=" + owners(masters));
		assertEquals(0, unowned);
		return masters;
	}

	/**
	 * Return the number of partitions each node is master of in the client's partition map.
	 */
	static Map<String,Integer> owners(AerospikeClient client) {
		Node[] masters = new Node[Node.PARTITIONS];
		unowned(client, masters);
		return owners(masters);
	}

	static Map<String,Integer> owners(Node[] masters) {
		Map<String,Integer> owners = new TreeMap<>();

		for (Node node : masters) {
			if (node != null) {
				owners.merge(node.getName(), 1, Integer::sum);
			}
		}
		return owners;
	}

	private static int unowned(AerospikeClient client, Node[] masters) {
		Partitions partitions = client.getCluster().partitionMap.get(namespace);
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = null;

			if (partitions != null) {
				AtomicReferenceArray<Node> replicas = partitions.replicas[0];
				node = replicas.get(i);
			}

			if (node == null || ! node.isActive()) {
				masters[i] = null;
				unowned++;
			}
			else {
				masters[i] = node;
			}
		}
		return unowned;
	}

	static void assertUnreachable(String endpoint) {
		int index = endpoint.lastIndexOf(':');
		String h = endpoint.substring(0, index);
		int port = Integer.parseInt(endpoint.substring(index + 1));

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(h, port), 2000);
			fail("Advertised address " + endpoint + " is reachable from the client");
		}
		catch (IOException expected) {
			System.out.println("L2-UNREACHABLE " + endpoint + ' ' + expected);
		}
	}

	/**
	 * Return the namespace statistics of a node, read through its listener.
	 */
	static Map<String,Long> namespaceStats(L2Node node, String... names) {
		String reply = Info.request(host, node.port, "namespace/" + namespace);
		Map<String,Long> stats = new HashMap<>();

		for (String token : reply.split(";")) {
			int index = token.indexOf('=');

			if (index > 0) {
				String name = token.substring(0, index);

				for (String wanted : names) {
					if (wanted.equals(name)) {
						stats.put(name, Long.parseLong(token.substring(index + 1)));
					}
				}
			}
		}

		for (String wanted : names) {
			assertNotNull("Node " + node.name + " has no namespace statistic " + wanted, stats.get(wanted));
		}
		return stats;
	}

	/**
	 * Enable or disable a Toxiproxy listener.
	 */
	static void setListener(String proxy, boolean enabled) throws IOException, InterruptedException {
		HttpRequest request = HttpRequest.newBuilder(URI.create(toxiproxyUrl + "/proxies/" + proxy))
			.timeout(Duration.ofSeconds(10))
			.header("Content-Type", "application/json")
			.POST(HttpRequest.BodyPublishers.ofString("{\"enabled\":" + enabled + "}"))
			.build();
		HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());

		System.out.println("L2-TOXIPROXY " + proxy + " enabled=" + enabled + " status=" + response.statusCode() +
			' ' + response.body());
		assertEquals(proxy + ": " + response.body(), 200, response.statusCode());
	}

	/**
	 * Create a Toxiproxy listener on the first free port above every listener in use and return
	 * that port.
	 */
	static int addListener(String proxy, String upstream) throws IOException, InterruptedException {
		return addListener(proxy, nextListenerPort(), upstream);
	}

	/**
	 * Return the first free port above every Toxiproxy listener in use.
	 */
	static int nextListenerPort() throws IOException, InterruptedException {
		int port = 0;

		for (int used : listenerPorts()) {
			port = Math.max(port, used);
		}
		return port + 1;
	}

	/**
	 * Create a Toxiproxy listener on {@code port} and return that port.
	 */
	static int addListener(String proxy, int port, String upstream) throws IOException, InterruptedException {
		HttpResponse<String> response = toxiproxy("POST", "/proxies", "{\"name\":\"" + proxy + "\",\"listen\":\"0.0.0.0:" +
			port + "\",\"upstream\":\"" + upstream + "\",\"enabled\":true}");

		System.out.println("L2-TOXIPROXY add " + proxy + " status=" + response.statusCode() + ' ' + response.body());
		assertEquals(proxy + ": " + response.body(), 201, response.statusCode());
		return port;
	}

	static void removeListener(String proxy) throws IOException, InterruptedException {
		HttpResponse<String> response = toxiproxy("DELETE", "/proxies/" + proxy, null);

		System.out.println("L2-TOXIPROXY remove " + proxy + " status=" + response.statusCode());
		assertEquals(proxy + ": " + response.body(), 204, response.statusCode());
	}

	/**
	 * Return the port a Toxiproxy listener forwards to.
	 */
	static int upstreamPort(String proxy) throws IOException, InterruptedException {
		HttpResponse<String> response = toxiproxy("GET", "/proxies/" + proxy, null);
		Matcher m = UPSTREAM_PORT.matcher(response.body());

		assertEquals(proxy + ": " + response.body(), 200, response.statusCode());
		assertTrue("No upstream port for " + proxy + ": " + response.body(), m.find());
		return Integer.parseInt(m.group(1));
	}

	static Set<Integer> listenerPorts() throws IOException, InterruptedException {
		HttpResponse<String> response = toxiproxy("GET", "/proxies", null);
		Matcher m = LISTEN_PORT.matcher(response.body());
		Set<Integer> set = new HashSet<>(proxyPorts.values());

		assertEquals(response.body(), 200, response.statusCode());

		while (m.find()) {
			set.add(Integer.parseInt(m.group(1)));
		}
		return set;
	}

	private static HttpResponse<String> toxiproxy(String method, String path, String body)
		throws IOException, InterruptedException {
		HttpRequest request = HttpRequest.newBuilder(URI.create(toxiproxyUrl + path))
			.timeout(Duration.ofSeconds(10))
			.header("Content-Type", "application/json")
			.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
			.build();
		return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
	}

	/**
	 * Ask the host to act on the server cluster through DISCOVERY_CONTROL_DIR and return its
	 * acknowledgement. The request is written as {@code <id>.request} and the host answers with
	 * {@code <id>.ack}, one {@code key=value} per line.
	 */
	static Map<String,String> control(String command) throws IOException, InterruptedException {
		Path dir = Paths.get(controlDir);
		String id = UUID.randomUUID().toString();
		Path tmp = dir.resolve(id + ".tmp");
		Path ack = dir.resolve(id + ".ack");

		Files.write(tmp, (command + '\n').getBytes(StandardCharsets.UTF_8));
		Files.move(tmp, dir.resolve(id + ".request"), StandardCopyOption.ATOMIC_MOVE);
		assertTrue("No acknowledgement for '" + command + "' in " + dir, waitFor(() -> Files.exists(ack)));

		Map<String,String> map = new LinkedHashMap<>();

		for (String line : Files.readAllLines(ack, StandardCharsets.UTF_8)) {
			int index = line.indexOf('=');

			if (index > 0) {
				map.put(line.substring(0, index), line.substring(index + 1));
			}
		}
		Files.delete(ack);
		System.out.println("L2-CONTROL " + command + ' ' + map);
		assertFalse("'" + command + "' failed: " + map.get("error"), map.containsKey("error"));
		return map;
	}

	static long peersGeneration(L2Node node) {
		return Long.parseLong(Info.request(host, node.port, "peers-generation"));
	}

	/**
	 * Node that joined the client and the time from the peers-generation bump that announced it to
	 * the node being active.
	 */
	static final class Joined {
		final Node node;
		final long bumpToActiveMs;
		final long generation;

		Joined(Node node, long bumpToActiveMs, long generation) {
			this.node = node;
			this.bumpToActiveMs = bumpToActiveMs;
			this.generation = generation;
		}
	}

	/**
	 * Wait for the peers-generation of {@code observer} to move from {@code generation} and for an
	 * active node not in {@code names} to appear in the client, as N-11 measures it.
	 */
	static Joined waitJoined(AerospikeClient client, L2Node observer, long generation, List<String> names)
		throws InterruptedException {
		long[] bump = {0};
		long[] active = {0};
		Node[] added = {null};

		assertTrue("No peers-generation bump or no active node after generation " + generation, waitFor(() -> {
			long now = System.nanoTime();

			if (bump[0] == 0 && peersGeneration(observer) != generation) {
				bump[0] = now;
			}

			if (active[0] == 0) {
				for (Node node : client.getNodes()) {
					if (node.isActive() && ! names.contains(node.getName())) {
						active[0] = now;
						added[0] = node;
					}
				}
			}
			return bump[0] != 0 && active[0] != 0;
		}, 5));
		return new Joined(added[0], TimeUnit.NANOSECONDS.toMillis(active[0] - bump[0]), peersGeneration(observer));
	}

	/**
	 * Return the alternate {@code host:port} list a node advertises in service-clear-alt.
	 */
	static List<String> alternates(L2Node node) {
		return endpoints(Info.request(host, node.port, "service-clear-alt"));
	}

	/**
	 * Return the name of the peer {@code observer} lists with {@code endpoint} in a peers reply.
	 */
	static String peerName(L2Node observer, String command, String endpoint) {
		for (Peer peer : parsePeers(Info.request(host, observer.port, command))) {
			if (peer.endpoints.contains(endpoint)) {
				return peer.name;
			}
		}
		return null;
	}

	/**
	 * Wait until every node reports the same cluster key for a cluster of {@code size} nodes with no
	 * migrations left.
	 */
	static void waitClusterStable(int size) throws InterruptedException {
		String command = "cluster-stable:size=" + size + ";ignore-migrations=false";
		List<String> keys = new ArrayList<>();

		boolean stable = waitFor(() -> {
			keys.clear();

			for (L2Node node : nodes()) {
				try {
					keys.add(Info.request(host, node.port, command));
				}
				catch (RuntimeException e) {
					keys.add(e.getMessage());
				}
			}
			return keys.stream().distinct().count() == 1 && ! keys.get(0).startsWith("ERROR");
		});
		System.out.println("L2-CLUSTER-STABLE size=" + size + ' ' + keys);
		assertTrue("Cluster not stable at size " + size + ": " + keys, stable);
	}

	/**
	 * Parse a peers-clear-std or peers-tls-std reply:
	 * {@code gen,port,[[node,tls-name,[address[:port],...]],...]}.
	 */
	static List<Peer> parsePeers(String reply) {
		List<Peer> peers = new ArrayList<>();

		if (reply == null) {
			return peers;
		}

		int first = reply.indexOf(',');
		int second = reply.indexOf(',', first + 1);

		if (first < 0 || second < 0) {
			return peers;
		}

		String portString = reply.substring(first + 1, second);
		int defaultPort = portString.isEmpty() ? 0 : Integer.parseInt(portString);
		Matcher m = PEER.matcher(reply.substring(second + 1));

		while (m.find()) {
			List<String> endpoints = new ArrayList<>();

			for (String address : m.group(3).split(",")) {
				if (address.isEmpty()) {
					continue;
				}

				boolean hasPort = address.startsWith("[") ? address.contains("]:") : address.contains(":");
				endpoints.add(hasPort ? address : address + ':' + defaultPort);
			}
			peers.add(new Peer(m.group(1), m.group(2), endpoints));
		}
		return peers;
	}

	private static List<String> endpoints(String reply) {
		List<String> list = new ArrayList<>();

		if (reply == null || reply.isEmpty()) {
			return list;
		}

		for (Host h : Host.parseServiceHosts(reply)) {
			list.add(h.name.indexOf(':') >= 0 ? "[" + h.name + "]:" + h.port : h.name + ':' + h.port);
		}
		return list;
	}

	private static String proxyName(int port) {
		for (Map.Entry<String,Integer> entry : proxyPorts.entrySet()) {
			if (entry.getValue() == port) {
				return entry.getKey();
			}
		}
		throw new IllegalStateException("DISCOVERY_PROXY_MAP has no proxy for listener " + port + ": " + proxyPorts);
	}

	private static Map<String,Integer> proxyPorts(String csv) {
		Map<String,Integer> map = new LinkedHashMap<>();

		for (String entry : csv.split(",")) {
			int index = entry.lastIndexOf(':');
			map.put(entry.substring(0, index), Integer.parseInt(entry.substring(index + 1)));
		}
		return map;
	}

	private static List<Integer> ports(String csv) {
		List<Integer> list = new ArrayList<>();

		for (String port : csv.split(",")) {
			if (! port.isEmpty()) {
				list.add(Integer.parseInt(port));
			}
		}
		return Collections.unmodifiableList(list);
	}

	private static String optional(String name) {
		String value = System.getenv(name);
		return value == null ? "" : value;
	}

	private static String required(String name) {
		String value = System.getenv(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("Environment variable " + name +
				" is not set. Run test/discovery/run_l2.sh or the discovery-l2 workflow");
		}
		return value;
	}
}
