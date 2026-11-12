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
package com.aerospike.test.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.test.util.TestBase;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

/**
 * A TLS connect that is ended by the command's own connect timeout must not
 * disturb the command's next attempt. Each attempt gets its own TCP connect
 * and its full connectTimeout, and the reported sub-exceptions describe only
 * attempts that reached the server.
 *
 * The scenario needs a TLS endpoint that accepts the TCP connect and then
 * never answers the handshake, plus a TCP connect that takes measurable time.
 * The first comes from an in-process proxy. The second cannot be produced
 * from Java, so the test measures the connect latency to the proxy and skips
 * unless the host adds at least {@link #MIN_CONNECT_LATENCY_MS}:
 *
 * <pre>
 * Linux:  sudo tc qdisc add dev lo root netem delay 20ms
 *         sudo tc qdisc del dev lo root
 *
 * macOS:  sudo dnctl pipe 1 config delay 20ms
 *         (cat /etc/pf.conf; echo 'dummynet-anchor "latency"'; echo 'anchor "latency"') | sudo pfctl -f -
 *         echo 'dummynet in quick proto tcp from any to 127.0.0.1 port 3443 pipe 1' | sudo pfctl -a latency -f -
 *         sudo pfctl -e
 *         (undo) sudo pfctl -a latency -F all; sudo dnctl -q flush; sudo pfctl -f /etc/pf.conf
 * </pre>
 *
 * The proxy port defaults to 3443 and can be changed with -DtlsProxyPort. The latency
 * floor can be lowered with -DtlsProxyMinLatencyMs to exercise the rig on plain loopback,
 * where the outcome depends on which timer wins and the test may pass without latency.
 */
public class TestAsyncTlsConnectTimeout extends TestBase {
	private static final int PROXY_PORT = Integer.getInteger("tlsProxyPort", 3443);
	private static final int CONNECT_TIMEOUT_MS = 1000;
	private static final int MAX_RETRIES = 3;
	private static final int MIN_CONNECT_LATENCY_MS = Integer.getInteger("tlsProxyMinLatencyMs", 10);
	private static final String STALE_EVENT_MESSAGE = "TLS connect failed: null";

	private static Host server;
	private static StallingProxy proxy;
	private static NettyEventLoops eventLoops;
	private static AerospikeClient client;

	@BeforeClass
	public static void connect() throws Exception {
		Assume.assumeTrue("Requires -tls", args.tlsPolicy != null && !args.tlsPolicy.forLoginOnly);
		server = Host.parseHosts(args.host, args.port)[0];
		Assume.assumeTrue("Requires a tlsName in -h", server.tlsName != null);

		proxy = new StallingProxy(PROXY_PORT, server);
		proxy.start();

		long latency = measureConnectLatencyMs(proxy.address());
		Assume.assumeTrue("Requires at least " + MIN_CONNECT_LATENCY_MS +
			"ms TCP connect latency to " + proxy.address() + " (measured " + latency +
			"ms). See the class comment for netem / dummynet setup.",
			latency >= MIN_CONNECT_LATENCY_MS);

		EventPolicy eventPolicy = new EventPolicy();
		// Smallest wheel tick the client allows. The command's timer fires on the
		// first tick after its deadline, so a small tick keeps it ahead of the
		// handshake timer by the connect latency.
		eventPolicy.minTimeout = 5;
		EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
		eventLoops = new NettyEventLoops(eventPolicy, group, EventLoopType.NETTY_NIO);

		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		policy.eventLoops = eventLoops;
		policy.timeout = 5000;
		policy.failIfNotConnected = true;
		policy.asyncMinConnsPerNode = 0;
		policy.maxErrorRate = 0;

		// A loopback seed is never treated as a load balancer, so the node keeps
		// the proxy address instead of the server's advertised one.
		Host seed = new Host("127.0.0.1", server.tlsName, PROXY_PORT);
		client = new AerospikeClient(policy, seed);

		// Peers are discovered directly, so with more nodes the key could route
		// around the proxy and the command would succeed.
		Assume.assumeTrue("Requires a single-node cluster", client.getNodes().length == 1);
	}

	@AfterClass
	public static void close() {
		if (client != null) {
			client.close();
		}
		if (eventLoops != null) {
			eventLoops.close();
		}
		if (proxy != null) {
			proxy.stop();
		}
	}

	@Test
	public void connectTimeoutDoesNotDisturbNextAttempt() throws Exception {
		Policy policy = new Policy();
		policy.connectTimeout = CONNECT_TIMEOUT_MS;
		policy.socketTimeout = CONNECT_TIMEOUT_MS;
		policy.totalTimeout = 30000;
		policy.maxRetries = MAX_RETRIES;
		policy.sleepBetweenRetries = 0;
		policy.timeoutDelay = 0;

		proxy.stall();

		CompletableFuture<Record> future = new CompletableFuture<>();
		AtomicInteger callbacks = new AtomicInteger();
		client.get(eventLoops.next(), new RecordListener() {
			public void onSuccess(Key key, Record record) {
				callbacks.incrementAndGet();
				future.complete(record);
			}

			public void onFailure(AerospikeException ae) {
				callbacks.incrementAndGet();
				future.completeExceptionally(ae);
			}
		}, policy, new Key(args.namespace, args.set, "tls-connect-timeout"));

		AerospikeException ae;
		try {
			future.get(20, TimeUnit.SECONDS);
			fail("Command must fail: the proxy never completes the TLS handshake");
			return;
		}
		catch (ExecutionException ee) {
			assertTrue("Unexpected cause: " + ee.getCause(), ee.getCause() instanceof AerospikeException);
			ae = (AerospikeException)ee.getCause();
		}

		// Give the proxy's reader threads a moment to observe the last close.
		List<Long> lifetimes = proxy.awaitStalledLifetimes(MAX_RETRIES + 1, 2000);

		assertEquals("Each attempt must reach the server: " + describe(ae, lifetimes),
			MAX_RETRIES + 1, lifetimes.size());

		// A connection killed by a stale event would live only milliseconds. The
		// bounds are loose because the proxy measures from accept to close, and
		// the client's attempt starts before the connect.
		for (long lifetime : lifetimes) {
			assertTrue("Each attempt must get its full connectTimeout: " + describe(ae, lifetimes),
				lifetime >= CONNECT_TIMEOUT_MS / 2 && lifetime < CONNECT_TIMEOUT_MS * 3);
		}

		assertEquals("Listener must be called exactly once: " + describe(ae, lifetimes),
			1, callbacks.get());

		// The list is null when every attempt ended on the command's own timer.
		List<AerospikeException> subs = ae.getSubExceptions();

		if (subs != null) {
			for (AerospikeException sub : subs) {
				assertTrue("Sub-exception from an abandoned channel: " + describe(ae, lifetimes),
					sub.getMessage() == null || !sub.getMessage().contains(STALE_EVENT_MESSAGE));
			}
		}
	}

	private static String describe(AerospikeException ae, List<Long> lifetimes) {
		// The message already lists the sub-exceptions.
		return "stalled connects (ms alive) = " + lifetimes + "; " + ae.getMessage();
	}

	private static long measureConnectLatencyMs(InetSocketAddress address) throws IOException {
		long min = Long.MAX_VALUE;

		for (int i = 0; i < 3; i++) {
			long begin = System.nanoTime();

			try (Socket socket = new Socket()) {
				socket.connect(address, 5000);
			}
			min = Math.min(min, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin));
		}
		return min;
	}

	/**
	 * TCP proxy in front of the TLS server. In pass-through mode it relays bytes
	 * both ways, which lets the client connect and tend normally. In stall mode
	 * it accepts, swallows whatever the client sends and never answers, so a
	 * TLS handshake on a new connection can only end by timeout. Connections
	 * relayed before the switch keep working.
	 */
	private static final class StallingProxy implements Runnable {
		private final ServerSocket listener;
		private final InetSocketAddress target;
		private final AtomicBoolean stalling = new AtomicBoolean(false);
		private final List<Long> stalledLifetimes = new ArrayList<>();
		private final List<Socket> sockets = new ArrayList<>();
		private final Thread acceptor;

		StallingProxy(int port, Host target) throws IOException {
			this.listener = new ServerSocket();
			this.listener.setReuseAddress(true);
			this.listener.bind(new InetSocketAddress("127.0.0.1", port));
			this.target = new InetSocketAddress(target.name, target.port);
			this.acceptor = daemon(this, "tls-proxy-accept");
		}

		InetSocketAddress address() {
			return (InetSocketAddress)listener.getLocalSocketAddress();
		}

		void start() {
			acceptor.start();
		}

		void stall() {
			stalling.set(true);
		}

		List<Long> awaitStalledLifetimes(int expected, long waitMillis) throws InterruptedException {
			long deadline = System.currentTimeMillis() + waitMillis;

			synchronized (stalledLifetimes) {
				while (stalledLifetimes.size() < expected && System.currentTimeMillis() < deadline) {
					stalledLifetimes.wait(50);
				}
				return new ArrayList<>(stalledLifetimes);
			}
		}

		void stop() {
			closeQuietly(listener);

			synchronized (sockets) {
				for (Socket socket : sockets) {
					closeQuietly(socket);
				}
			}
		}

		public void run() {
			while (!listener.isClosed()) {
				Socket socket;

				try {
					socket = listener.accept();
				}
				catch (IOException e) {
					return;
				}
				track(socket);

				if (stalling.get()) {
					daemon(() -> swallow(socket), "tls-proxy-stall").start();
				}
				else {
					relay(socket);
				}
			}
		}

		private void swallow(Socket socket) {
			long begin = System.nanoTime();

			try (InputStream in = socket.getInputStream()) {
				byte[] buf = new byte[4096];

				while (in.read(buf) >= 0) {
				}
			}
			catch (IOException e) {
			}
			finally {
				long lifetime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

				synchronized (stalledLifetimes) {
					stalledLifetimes.add(lifetime);
					stalledLifetimes.notifyAll();
				}
				closeQuietly(socket);
			}
		}

		private void relay(Socket client) {
			Socket upstream;

			try {
				upstream = new Socket();
				upstream.connect(target, 5000);
			}
			catch (IOException e) {
				closeQuietly(client);
				return;
			}
			track(upstream);
			daemon(() -> pump(client, upstream), "tls-proxy-up").start();
			daemon(() -> pump(upstream, client), "tls-proxy-down").start();
		}

		private static void pump(Socket from, Socket to) {
			try {
				InputStream in = from.getInputStream();
				OutputStream out = to.getOutputStream();
				byte[] buf = new byte[8192];
				int n;

				while ((n = in.read(buf)) >= 0) {
					out.write(buf, 0, n);
					out.flush();
				}
			}
			catch (IOException e) {
			}
			finally {
				closeQuietly(from);
				closeQuietly(to);
			}
		}

		private void track(Socket socket) {
			synchronized (sockets) {
				sockets.add(socket);
			}
		}

		private static Thread daemon(Runnable r, String name) {
			Thread t = new Thread(r, name);
			t.setDaemon(true);
			return t;
		}

		private static void closeQuietly(ServerSocket s) {
			try {
				s.close();
			}
			catch (IOException e) {
			}
		}

		private static void closeQuietly(Socket s) {
			try {
				s.close();
			}
			catch (IOException e) {
			}
		}
	}
}
