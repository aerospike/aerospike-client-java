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

import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Driver for the l5-aws harness in aerospike-client-conformance (l5-aws/DRIVER.md there, contract
 * version 3): builds a client from the flags, runs a retrying load, and writes what the client sees
 * to stdout as JSON lines while it answers the commands it reads from stdin.
 */
public final class L5Driver {
	static final int CONTRACT = 3;
	static final long POLL_MS = 10;
	static final long HEARTBEAT_MS = 100;
	static final long REACH_INTERVAL_MS = 500;
	static final int REACH_TIMEOUT_MS = 1000;
	static final long PROBE_INTERVAL_MS = 10;
	static final int PROBE_CONNECT_MS = 50;
	static final int SERVING_ROUNDS = 3;
	static final long WATCH_BOUND_MS = 60000;
	static final long QUIT_MS = 30000;
	static final int KEY_SEARCH_LIMIT = 1000000;
	static final String[] NODE_EVENTS = {"Add node", "Remove node", "Replace node", "Move node"};

	private static final long START = System.nanoTime();
	private static final String USAGE = "usage: L5Driver --seed HOST:PORT --shape pass-through|port-map " +
		"[--port-map NODE=HOST:PORT,...] [--on-miss strict|pass-through] [--user USER] --tend-interval MS " +
		"--timeout MS --namespace NS --set SET [--workers N] [--keys N]";

	public static void main(String[] args) {
		Options options;

		try {
			options = new Options(args, System.getenv("AEROSPIKE_ADMIN_PASSWORD"));
		}
		catch (IllegalArgumentException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.err.println(USAGE);
			System.exit(2);
			return;
		}

		Output out = new Output();
		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> {
			if (forward(level, message)) {
				String line = new Json("log", now()).put("level", level.toString()).put("message", message).toString();

				if (level == Log.Level.INFO) {
					out.writeIfOpen(line);
				}
				else {
					out.write(line);
				}
			}
		});

		AerospikeClient client;

		try {
			client = new AerospikeClient(options.policy(), options.seed);
		}
		catch (RuntimeException e) {
			out.writeNow(new Json("error", now()).put("message", "client not created: " + message(e)).toString());
			System.err.println("ERROR: client not created: " + e);
			System.exit(3);
			return;
		}
		new L5Driver(options, out, client).run();
	}

	static long now() {
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - START);
	}

	static String message(Throwable e) {
		return e.getMessage() != null ? e.getMessage() : e.toString();
	}

	/**
	 * Forward WARN and above, and the INFO lines that add, remove, replace or move a node.
	 */
	static boolean forward(Log.Level level, String message) {
		if (level.compareTo(Log.Level.WARN) <= 0) {
			return true;
		}

		if (level != Log.Level.INFO) {
			return false;
		}

		for (String event : NODE_EVENTS) {
			if (message.startsWith(event)) {
				return true;
			}
		}
		return false;
	}

	static String ready(long t, long epochMs, String clientVersion, String shape) {
		return new Json("ready", t).put("contract", CONTRACT).put("epochMs", epochMs).put("client", "java")
			.put("clientVersion", clientVersion).put("shape", shape).toString();
	}

	static String failure(long t, long startedAt, String message) {
		return new Json("failure", t).put("startedAt", startedAt).put("message", message).toString();
	}

	private final Options options;
	private final Output out;
	private final AerospikeClient client;
	private final Load load;
	private final Routing routing = new Routing();
	private final Map<Node,Boolean> reachable = new ConcurrentHashMap<>();
	private final Set<Node> checking = ConcurrentHashMap.newKeySet();
	private final ScheduledExecutorService routingPoll = Executors.newSingleThreadScheduledExecutor(daemon("l5-routing"));
	private final ScheduledExecutorService reachPoll = Executors.newSingleThreadScheduledExecutor(daemon("l5-reach"));
	private final ExecutorService reachChecks = Executors.newCachedThreadPool(daemon("l5-reach-check"));
	private final AtomicReference<Watch> watch = new AtomicReference<>();

	private L5Driver(Options options, Output out, AerospikeClient client) {
		this.options = options;
		this.out = out;
		this.client = client;
		this.load = new Load(this::ready);
	}

	private void run() {
		reachPoll.scheduleAtFixedRate(this::checkReachable, 0, REACH_INTERVAL_MS, TimeUnit.MILLISECONDS);
		load.start();

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
			String line;

			while ((line = in.readLine()) != null) {
				if (! command(line)) {
					break;
				}
			}
		}
		catch (IOException e) {
			System.err.println("stdin: " + e);
		}
		quit();
	}

	private void ready() {
		out.open(ready(now(), System.currentTimeMillis(), client.getVersion(), options.shape));
		routingPoll.scheduleAtFixedRate(routing::poll, 0, POLL_MS, TimeUnit.MILLISECONDS);
	}

	/**
	 * Run one command line and return false on quit.
	 */
	private boolean command(String line) {
		String[] words = line.trim().split("\\s+");

		switch (words[0]) {
		case "serve":
			if (words.length != 2) {
				error("malformed command: " + line);
			}
			else {
				serve(words[1]);
			}
			return true;

		case "reset":
			if (words.length != 1) {
				error("malformed command: " + line);
			}
			else {
				long t = now();
				load.reset();
				routing.reset(t);
				out.write(new Json("reset", t).toString());
			}
			return true;

		case "watch":
			Host host = words.length == 3 ? hostPort(words[1]) : null;

			if (host == null) {
				error("malformed command: " + line);
			}
			else {
				Watch w = new Watch(words[1], host, words[2]);

				if (watch.compareAndSet(null, w)) {
					w.start();
				}
				else {
					error("a watch is already running: " + line);
				}
			}
			return true;

		case "quit":
			if (words.length != 1) {
				error("malformed command: " + line);
				return true;
			}
			return false;

		default:
			error("unknown command: " + line);
			return true;
		}
	}

	private void error(String message) {
		out.write(new Json("error", now()).put("message", message).toString());
	}

	/**
	 * Stop the load and let a running watch reply, both within {@link #QUIT_MS}, then close the
	 * client and exit.
	 */
	private void quit() {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUIT_MS);
		load.stop(deadline);
		Watch w = watch.get();

		try {
			if (w != null) {
				w.stop(deadline);
			}
			routingPoll.shutdown();
			routingPoll.awaitTermination(1, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		reachPoll.shutdownNow();
		reachChecks.shutdownNow();
		client.close();
		System.exit(0);
	}

	private void serve(String step) {
		Key[] keys = new Key[Node.PARTITIONS];
		int found = 0;

		for (int i = 0; i < KEY_SEARCH_LIMIT && found < Node.PARTITIONS; i++) {
			Key candidate = new Key(options.namespace, options.set, step + '-' + i);
			int partition = Partition.getPartitionId(candidate.digest);

			if (keys[partition] == null) {
				keys[partition] = candidate;
				found++;
			}
		}

		long start = System.nanoTime();
		WritePolicy wp = options.writePolicy();
		Policy rp = options.readPolicy();
		int written = 0;
		int read = 0;
		int mismatched = 0;
		int failures = 0;
		String firstFailure = null;

		for (int partition = 0; partition < Node.PARTITIONS; partition++) {
			if (keys[partition] == null) {
				failures++;
				firstFailure = firstFailure != null ? firstFailure :
					"partition " + partition + ": no key in " + KEY_SEARCH_LIMIT + " candidates";
				continue;
			}
			String value = step + '-' + partition;

			try {
				client.put(wp, keys[partition], new Bin("v", value));
				written++;
			}
			catch (RuntimeException e) {
				failures++;
				firstFailure = firstFailure != null ? firstFailure : "partition " + partition + " put: " + message(e);
				continue;
			}

			try {
				Record record = client.get(rp, keys[partition]);

				if (record != null && value.equals(record.getValue("v"))) {
					read++;
				}
				else {
					mismatched++;
				}
			}
			catch (RuntimeException e) {
				failures++;
				firstFailure = firstFailure != null ? firstFailure : "partition " + partition + " get: " + message(e);
			}
		}
		out.write(new Json("served", now()).put("step", step).put("partitions", Node.PARTITIONS).put("written", written)
			.put("read", read).put("mismatched", mismatched).put("failures", failures).put("firstFailure", firstFailure)
			.put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).toString());
	}

	private void checkReachable() {
		List<Node> nodes = Arrays.asList(client.getNodes());
		reachable.keySet().retainAll(nodes);

		for (Node node : nodes) {
			if (checking.add(node)) {
				reachChecks.execute(() -> {
					try {
						reachable.put(node, reachable(node));
					}
					finally {
						checking.remove(node);
					}
				});
			}
		}
	}

	/**
	 * Return whether the node answers an info request for its name within {@link #REACH_TIMEOUT_MS}.
	 */
	static boolean reachable(Node node) {
		InfoPolicy policy = new InfoPolicy();
		policy.timeout = REACH_TIMEOUT_MS;

		try {
			return node.getName().equals(Info.request(policy, node, "node"));
		}
		catch (AerospikeException e) {
			return false;
		}
	}

	private Map<String,Node> nodesByName(Node[] nodes) {
		Map<String,Node> map = new HashMap<>();

		for (Node node : nodes) {
			map.put(node.getName(), node);
		}
		return map;
	}

	/**
	 * Fill {@code masters} with the master of each partition of the namespace and return the
	 * number of partitions whose master is missing, inactive, or not the node the client holds
	 * under that name.
	 */
	private int unowned(Map<String,Node> nodes, Node[] masters) {
		HashMap<String,Partitions> map = client.getCluster().partitionMap;
		Partitions partitions = map == null ? null : map.get(options.namespace);
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions == null ? null : partitions.replicas[0].get(i);

			if (node == null || ! node.isActive() || nodes.get(node.getName()) != node) {
				masters[i] = null;
				unowned++;
			}
			else {
				masters[i] = node;
			}
		}
		return unowned;
	}

	static String address(Node node) {
		InetSocketAddress address = node.getAddress();
		InetAddress ip = address.getAddress();
		return ip != null ? ip.getHostAddress() : address.getHostString();
	}

	/**
	 * Parse {@code HOST:PORT}, the host optionally in brackets, or return null.
	 */
	static Host hostPort(String value) {
		int colon = value.lastIndexOf(':');

		if (colon <= 0) {
			return null;
		}
		String host = value.substring(0, colon);

		if (host.startsWith("[") && host.endsWith("]")) {
			host = host.substring(1, host.length() - 1);
		}

		try {
			int port = Integer.parseInt(value.substring(colon + 1));
			return ! host.isEmpty() && port > 0 && port <= 65535 ? new Host(host, port) : null;
		}
		catch (NumberFormatException e) {
			return null;
		}
	}

	static ThreadFactory daemon(String name) {
		AtomicLong count = new AtomicLong();
		return r -> {
			Thread thread = new Thread(r, name + '-' + count.getAndIncrement());
			thread.setDaemon(true);
			return thread;
		};
	}

	/**
	 * Polls the client's routing every {@link #POLL_MS}, writes a change snapshot when it differs
	 * and a heartbeat snapshot every {@link #HEARTBEAT_MS}.
	 */
	private final class Routing {
		private String last;
		private long nextHeartbeat;
		private long unownedSince = -1;
		private long longestClosed;

		synchronized void reset(long t) {
			longestClosed = 0;

			if (unownedSince >= 0) {
				unownedSince = t;
			}
		}

		void poll() {
			try {
				long t = now();
				Node[] nodes = client.getNodes();
				Arrays.sort(nodes, Comparator.comparing(Node::getName));
				Map<String,Node> byName = nodesByName(nodes);
				Node[] masters = new Node[Node.PARTITIONS];
				int unowned = unowned(byName, masters);
				long longest = longestUnowned(t, unowned);
				StringBuilder signature = new StringBuilder().append(unowned);

				for (Node node : nodes) {
					Host h = node.getHost();
					signature.append('|').append(node.getName()).append(',').append(h.name).append(',').append(h.port)
						.append(',').append(node.isActive());
				}
				String current = signature.toString();

				if (last == null) {
					nextHeartbeat = t;
				}
				else if (! current.equals(last)) {
					snapshot("change", t, nodes, masters, unowned, longest);
				}
				last = current;

				if (t >= nextHeartbeat) {
					snapshot("heartbeat", t, nodes, masters, unowned, longest);
					nextHeartbeat = Math.max(nextHeartbeat + HEARTBEAT_MS, t + 1);
				}
			}
			catch (RuntimeException e) {
				System.err.println("routing poll: " + e);
			}
		}

		private synchronized long longestUnowned(long t, int unowned) {
			if (unowned > 0 && unownedSince < 0) {
				unownedSince = t;
			}
			else if (unowned == 0 && unownedSince >= 0) {
				longestClosed = Math.max(longestClosed, t - unownedSince);
				unownedSince = -1;
			}
			return unownedSince >= 0 ? Math.max(longestClosed, t - unownedSince) : longestClosed;
		}

		private void snapshot(String reason, long t, Node[] nodes, Node[] masters, int unowned, long longest) {
			StringBuilder list = new StringBuilder("[");
			Map<String,Integer> owners = new TreeMap<>();
			long errors = 0;
			long timeouts = 0;

			for (Node node : nodes) {
				Host h = node.getHost();

				if (list.length() > 1) {
					list.append(',');
				}
				list.append(new Json().put("name", node.getName()).put("host", h.name).put("port", h.port)
					.put("address", address(node)).put("active", node.isActive()).put("reachable", reachable.get(node)));
				errors += node.getErrorCount();
				timeouts += node.getTimeoutCount();
			}
			list.append(']');

			for (Node node : masters) {
				if (node != null) {
					owners.merge(node.getName(), 1, Integer::sum);
				}
			}
			Json ownersJson = new Json();

			for (Map.Entry<String,Integer> entry : owners.entrySet()) {
				ownersJson.put(entry.getKey(), entry.getValue());
			}
			out.write(new Json("snapshot", t).put("reason", reason).raw("nodes", list.toString())
				.put("unowned", unowned).raw("owners", ownersJson.toString()).put("longestUnownedMs", longest)
				.put("clientErrors", errors).put("clientTimeouts", timeouts).put("succeeded", load.succeeded.get())
				.put("failed", load.failed.get()).put("maxLatencyMs", load.maxLatencyMs())
				.put("lastError", load.lastError.get()).toString());
		}
	}

	/**
	 * Probes every address of {@code HOST:PORT} every {@link #PROBE_INTERVAL_MS} until all accept a
	 * connection in {@link #SERVING_ROUNDS} consecutive rounds, then checks every
	 * {@link #PROBE_INTERVAL_MS} whether the node is usable, and replies.
	 */
	private final class Watch {
		private final String address;
		private final String node;
		private final Probe probe;
		private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(daemon("l5-watch"));
		private volatile boolean done;

		Watch(String address, Host host, String node) {
			this.address = address;
			this.node = node;
			this.probe = new Probe(host.name, host.port, InetAddress::getAllByName);
		}

		void start() {
			executor.execute(this::tick);
		}

		void stop(long deadline) throws InterruptedException {
			executor.awaitTermination(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
			executor.shutdownNow();
		}

		private void tick() {
			long begin = System.nanoTime();

			try {
				if (probe.servingAt == null) {
					probe.round(now());
				}

				if (probe.servingAt != null) {
					long t = now();

					if (usable()) {
						reply(t);
					}
					else if (t - probe.servingAt >= WATCH_BOUND_MS) {
						reply(null);
					}
				}
			}
			catch (RuntimeException e) {
				error("watch " + address + ' ' + node + ": " + message(e));
				finish();
			}

			if (! done) {
				long delay = TimeUnit.MILLISECONDS.toNanos(PROBE_INTERVAL_MS) - (System.nanoTime() - begin);
				executor.schedule(this::tick, Math.max(0, delay), TimeUnit.NANOSECONDS);
			}
		}

		private boolean usable() {
			Map<String,Node> nodes = nodesByName(client.getNodes());
			Node n = nodes.get(node);
			return n != null && n.isActive() && unowned(nodes, new Node[Node.PARTITIONS]) == 0;
		}

		private void reply(Long usableAt) {
			out.write(new Json("watch", now()).put("address", address).put("node", node).put("servingAt", probe.servingAt)
				.put("lastClosedAt", probe.lastClosedAt).raw("addresses", probe.addresses()).put("usableAt", usableAt)
				.put("usableMs", usableAt == null ? null : usableAt - probe.servingAt).put("ownershipChecked", true).toString());
			finish();
		}

		private void finish() {
			done = true;
			executor.shutdown();
			watch.compareAndSet(this, null);
		}
	}

	/**
	 * The watch probe. Each round resolves the host and opens a connection to every address it
	 * resolves to, all at once, each within {@link #PROBE_CONNECT_MS}, and closes them.
	 */
	static final class Probe {
		interface Resolver {
			InetAddress[] resolve(String host) throws UnknownHostException;
		}

		private final String host;
		private final int port;
		private final Resolver resolver;
		private final Map<InetAddress,Long> firstConnectedAt = new LinkedHashMap<>();
		private int servingRounds;
		private long servingStart;
		volatile Long servingAt;
		volatile Long lastClosedAt;

		Probe(String host, int port, Resolver resolver) {
			this.host = host;
			this.port = port;
			this.resolver = resolver;
		}

		/**
		 * Run the round that starts at {@code t}.
		 */
		void round(long t) {
			if (connectAll(t)) {
				if (servingRounds++ == 0) {
					servingStart = t;
				}

				if (servingRounds == SERVING_ROUNDS) {
					servingAt = servingStart;
				}
			}
			else {
				servingRounds = 0;
				lastClosedAt = t;
			}
		}

		synchronized String addresses() {
			StringBuilder sb = new StringBuilder("[");

			for (Map.Entry<InetAddress,Long> entry : firstConnectedAt.entrySet()) {
				if (sb.length() > 1) {
					sb.append(',');
				}
				sb.append(new Json().put("address", entry.getKey().getHostAddress()).put("firstConnectedAt", entry.getValue()));
			}
			return sb.append(']').toString();
		}

		private boolean connectAll(long t) {
			InetAddress[] addresses;

			try {
				addresses = resolver.resolve(host);
			}
			catch (UnknownHostException e) {
				return false;
			}

			synchronized (this) {
				for (InetAddress address : addresses) {
					firstConnectedAt.putIfAbsent(address, null);
				}
			}

			int connected = 0;

			try (Selector selector = Selector.open()) {
				int pending = 0;

				for (InetAddress address : addresses) {
					SocketChannel channel = SocketChannel.open();

					try {
						channel.configureBlocking(false);

						if (channel.connect(new InetSocketAddress(address, port))) {
							connected(address, t);
							connected++;
							channel.close();
						}
						else {
							channel.register(selector, SelectionKey.OP_CONNECT, address);
							pending++;
						}
					}
					catch (IOException e) {
						channel.close();
					}
				}

				long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PROBE_CONNECT_MS);
				long remaining;

				while (pending > 0 && (remaining = deadline - System.nanoTime()) > 0) {
					selector.select(TimeUnit.NANOSECONDS.toMillis(remaining + 999999));

					for (SelectionKey key : selector.selectedKeys()) {
						SocketChannel channel = (SocketChannel)key.channel();

						try {
							if (! channel.finishConnect()) {
								continue;
							}
							connected((InetAddress)key.attachment(), t);
							connected++;
						}
						catch (IOException e) {
						}
						channel.close();
						pending--;
					}
					selector.selectedKeys().clear();
				}

				for (SelectionKey key : selector.keys()) {
					key.channel().close();
				}
			}
			catch (IOException e) {
				return false;
			}
			return addresses.length > 0 && connected == addresses.length;
		}

		private synchronized void connected(InetAddress address, long t) {
			if (firstConnectedAt.get(address) == null) {
				firstConnectedAt.put(address, t);
			}
		}
	}

	/**
	 * The load: each worker runs one command at a time, and issues a failed command again after
	 * one tend interval until it succeeds.
	 */
	private final class Load {
		final AtomicLong succeeded = new AtomicLong();
		final AtomicLong failed = new AtomicLong();
		final AtomicReference<String> lastError = new AtomicReference<>();
		private final AtomicBoolean started = new AtomicBoolean();
		private final Runnable onFirstSuccess;
		private final List<Thread> workers = new ArrayList<>();
		private volatile boolean stop;
		private long resetAtNanos = START;
		private long maxLatencyNanos;

		Load(Runnable onFirstSuccess) {
			this.onFirstSuccess = onFirstSuccess;
		}

		void start() {
			WritePolicy wp = options.writePolicy();
			Policy rp = options.readPolicy();

			for (int w = 0; w < options.workers; w++) {
				int worker = w;
				Thread thread = new Thread(() -> run(worker, wp, rp), "l5-load-" + w);
				thread.setDaemon(true);
				workers.add(thread);
				thread.start();
			}
		}

		private void run(int worker, WritePolicy wp, Policy rp) {
			for (long i = 0; ! stop; i++) {
				Key key = new Key(options.namespace, options.set, "load-" + ((worker + i * options.workers) % options.keys));
				long first = System.nanoTime();

				while (true) {
					long startedAt = now();

					try {
						if (i % 2 == 0) {
							client.put(wp, key, new Bin("v", i));
						}
						else {
							client.get(rp, key);
						}
						break;
					}
					catch (RuntimeException e) {
						String message = message(e);
						failed.incrementAndGet();
						lastError.set(message);
						out.writeIfOpen(failure(now(), startedAt, message));
					}

					if (stop) {
						return;
					}

					try {
						Thread.sleep(options.tendInterval);
					}
					catch (InterruptedException e) {
						return;
					}

					if (stop) {
						return;
					}
				}
				succeeded.incrementAndGet();
				record(first, System.nanoTime());

				if (started.compareAndSet(false, true)) {
					onFirstSuccess.run();
				}
			}
		}

		private synchronized void record(long first, long end) {
			if (first - resetAtNanos >= 0) {
				maxLatencyNanos = Math.max(maxLatencyNanos, end - first);
			}
		}

		synchronized void reset() {
			resetAtNanos = System.nanoTime();
			maxLatencyNanos = 0;
		}

		synchronized long maxLatencyMs() {
			return TimeUnit.NANOSECONDS.toMillis(maxLatencyNanos);
		}

		/**
		 * Let each worker finish its current attempt, giving up at {@code deadline}.
		 */
		void stop(long deadline) {
			stop = true;

			try {
				for (Thread thread : workers) {
					long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());

					if (remaining > 0) {
						thread.join(remaining);
					}
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * The flags, validated.
	 */
	static final class Options {
		Host seed;
		String shape;
		Map<String,String> portMap;
		PortMapAddressTranslator portMapTranslator;
		TranslationMissMode onMiss = TranslationMissMode.STRICT;
		String user;
		String password;
		int tendInterval = -1;
		int timeout = -1;
		String namespace;
		String set;
		int workers = 4;
		int keys = 1000;

		Options(String[] args, String password) {
			Map<String,String> flags = new LinkedHashMap<>();

			for (int i = 0; i < args.length; i += 2) {
				if (! args[i].startsWith("--")) {
					throw new IllegalArgumentException("unexpected argument: " + args[i]);
				}

				if (i + 1 >= args.length) {
					throw new IllegalArgumentException(args[i] + " needs a value");
				}

				if (flags.put(args[i], args[i + 1]) != null) {
					throw new IllegalArgumentException(args[i] + " given twice");
				}
			}

			for (Map.Entry<String,String> flag : flags.entrySet()) {
				String value = flag.getValue();

				switch (flag.getKey()) {
				case "--seed":
					seed = hostPort(value);
					if (seed == null) {
						throw new IllegalArgumentException("--seed must be HOST:PORT: " + value);
					}
					break;

				case "--shape":
					if (! value.equals("pass-through") && ! value.equals("port-map")) {
						throw new IllegalArgumentException("--shape must be pass-through or port-map: " + value);
					}
					shape = value;
					break;

				case "--port-map":
					portMap = portMap(value);
					break;

				case "--on-miss":
					if (value.equals("strict")) {
						onMiss = TranslationMissMode.STRICT;
					}
					else if (value.equals("pass-through")) {
						onMiss = TranslationMissMode.PASS_THROUGH;
					}
					else {
						throw new IllegalArgumentException("--on-miss must be strict or pass-through: " + value);
					}
					break;

				case "--user":
					if (password == null) {
						throw new IllegalArgumentException("--user needs AEROSPIKE_ADMIN_PASSWORD in the environment");
					}
					user = value;
					this.password = password;
					break;

				case "--tend-interval":
					tendInterval = positive(flag.getKey(), value);
					break;

				case "--timeout":
					timeout = positive(flag.getKey(), value);
					break;

				case "--namespace":
					namespace = value;
					break;

				case "--set":
					set = value;
					break;

				case "--workers":
					workers = positive(flag.getKey(), value);
					break;

				case "--keys":
					keys = positive(flag.getKey(), value);
					break;

				default:
					throw new IllegalArgumentException("unknown flag: " + flag.getKey());
				}
			}
			require(seed, "--seed");
			require(shape, "--shape");
			require(tendInterval > 0 ? tendInterval : null, "--tend-interval");
			require(timeout > 0 ? timeout : null, "--timeout");
			require(namespace, "--namespace");
			require(set, "--set");

			if (shape.equals("port-map")) {
				require(portMap, "--port-map with --shape port-map");

				try {
					portMapTranslator = new PortMapAddressTranslator(portMap);
				}
				catch (AerospikeException e) {
					throw new IllegalArgumentException("--port-map: " + message(e));
				}
			}
			else if (portMap != null) {
				throw new IllegalArgumentException("--port-map only with --shape port-map");
			}
		}

		private static void require(Object value, String flag) {
			if (value == null) {
				throw new IllegalArgumentException("missing " + flag);
			}
		}

		private static int positive(String flag, String value) {
			try {
				int n = Integer.parseInt(value);

				if (n > 0) {
					return n;
				}
			}
			catch (NumberFormatException e) {
			}
			throw new IllegalArgumentException(flag + " must be a positive integer: " + value);
		}

		private static Map<String,String> portMap(String value) {
			Map<String,String> map = new LinkedHashMap<>();

			for (String entry : value.split(",")) {
				int eq = entry.indexOf('=');

				if (eq <= 0 || hostPort(entry.substring(eq + 1)) == null) {
					throw new IllegalArgumentException("--port-map entry must be NODE=HOST:PORT: " + entry);
				}

				if (map.put(entry.substring(0, eq), entry.substring(eq + 1)) != null) {
					throw new IllegalArgumentException("--port-map names " + entry.substring(0, eq) + " twice");
				}
			}
			return map;
		}

		ClientPolicy policy() {
			ClientPolicy policy = new ClientPolicy();
			policy.tendInterval = tendInterval;
			policy.timeout = timeout;
			policy.user = user;
			policy.password = password;
			policy.translationOnMiss = onMiss;

			if (shape.equals("pass-through")) {
				policy.useServicesAlternate = true;
				policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
			}
			else {
				policy.useServicesAlternate = false;
				policy.addressTranslator = portMapTranslator;
			}
			policy.readPolicyDefault = readPolicy();
			policy.writePolicyDefault = writePolicy();
			return policy;
		}

		Policy readPolicy() {
			Policy policy = new Policy();
			policy.socketTimeout = timeout;
			policy.totalTimeout = timeout;
			policy.maxRetries = 0;
			return policy;
		}

		WritePolicy writePolicy() {
			WritePolicy policy = new WritePolicy();
			policy.socketTimeout = timeout;
			policy.totalTimeout = timeout;
			policy.maxRetries = 0;
			return policy;
		}
	}

	/**
	 * Stdout, one JSON object per line, each flushed. Lines written before {@code ready} are held
	 * and follow it. Anything else printing to {@code System.out} goes to stderr.
	 */
	static final class Output {
		private final PrintStream stdout;
		private List<String> held = new ArrayList<>();

		Output() {
			this(new PrintStream(new FileOutputStream(FileDescriptor.out), false, StandardCharsets.UTF_8));
			System.setOut(System.err);
		}

		Output(PrintStream stdout) {
			this.stdout = stdout;
		}

		synchronized void write(String line) {
			if (held != null) {
				held.add(line);
			}
			else {
				print(line);
			}
		}

		/**
		 * Print the line once {@code ready} is out, otherwise drop it: a held line would precede
		 * {@code ready}'s {@code t}.
		 */
		synchronized void writeIfOpen(String line) {
			if (held == null) {
				print(line);
			}
		}

		synchronized void writeNow(String line) {
			print(line);
		}

		synchronized void open(String ready) {
			print(ready);

			for (String line : held) {
				print(line);
			}
			held = null;
		}

		private void print(String line) {
			stdout.print(line);
			stdout.print('\n');
			stdout.flush();
		}
	}

	/**
	 * A JSON object written by hand.
	 */
	static final class Json {
		private final StringBuilder sb = new StringBuilder("{");

		Json() {
		}

		Json(String type, long t) {
			put("type", type);
			put("t", t);
		}

		Json put(String name, String value) {
			return raw(name, value == null ? "null" : quote(value));
		}

		Json put(String name, long value) {
			return raw(name, Long.toString(value));
		}

		Json put(String name, Long value) {
			return raw(name, value == null ? "null" : value.toString());
		}

		Json put(String name, Boolean value) {
			return raw(name, value == null ? "null" : value.toString());
		}

		Json raw(String name, String json) {
			if (sb.length() > 1) {
				sb.append(',');
			}
			sb.append(quote(name)).append(':').append(json);
			return this;
		}

		@Override
		public String toString() {
			return sb + "}";
		}

		static String quote(String s) {
			StringBuilder q = new StringBuilder(s.length() + 2).append('"');

			for (int i = 0; i < s.length(); i++) {
				char c = s.charAt(i);

				switch (c) {
				case '"':
					q.append("\\\"");
					break;
				case '\\':
					q.append("\\\\");
					break;
				case '\n':
					q.append("\\n");
					break;
				case '\r':
					q.append("\\r");
					break;
				case '\t':
					q.append("\\t");
					break;
				default:
					if (c < 0x20 || c == 0x2028 || c == 0x2029) {
						q.append(String.format("\\u%04x", (int)c));
					}
					else {
						q.append(c);
					}
				}
			}
			return q.append('"').toString();
		}
	}
}
