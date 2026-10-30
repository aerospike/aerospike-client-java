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
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.L2Topology.Load;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3.2 row F-1 against the default {@link L2Topology}, in its own
 * run_l2.sh profile because it fails on Java until pooled sockets are checked on checkout. Every
 * toxic the case adds is removed in {@link #restore()}.
 */
public class TestL2IdleReset extends TestBase {
	private static final String BLOCKER = "F-1 needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)";
	private static final String SET = "l2f";
	private static final int WARM_THREADS = 4;
	private static final int WARM_ITERATIONS = 50;
	private static final int KEY_COUNT = 100;
	private static final int PROBE_TIMEOUT = 5000;

	private final List<String[]> toxics = new ArrayList<>();

	@After
	public void restore() throws Exception {
		for (String[] toxic : toxics) {
			L2Topology.removeToxic(toxic[0], toxic[1]);
		}
		toxics.clear();
	}

	/**
	 * F-1: idle flows torn down with RST, as an NLB tears down an idle flow. Every listener's
	 * connections are made to close with RST rather than FIN, and the listeners are then cycled
	 * while the client's connections sit idle in the pool. With the default {@link ClientPolicy}
	 * reaping the next commands succeed: the pool drops the dead sockets before use, or the policy's
	 * retry absorbs the error, and no command fails to the caller. A raw probe connection on every
	 * listener and the client's connection and error counters prove the resets happened.
	 */
	@Test
	public void f1IdleResetAbsorbedByPool() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.idleReset.isEmpty(), BLOCKER);
		List<L2Node> nodes = L2Topology.nodes();
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));
		assertEquals(new ClientPolicy().maxSocketIdle, policy.maxSocketIdle);

		for (L2Node node : nodes) {
			addToxic(node.proxy, "f1-rst", "reset_peer", 0.0, "{\"timeout\":0}");
		}

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("F-1", client);

			WritePolicy wp = client.copyWritePolicyDefault();
			Policy rp = client.copyReadPolicyDefault();
			Load warm = new Load("l2-f1-warm", WARM_THREADS, (thread, iteration) -> {
				Key key = new Key(L2Topology.namespace, SET, "f1-" + ((thread * WARM_ITERATIONS + iteration) % KEY_COUNT));
				client.put(wp, key, new Bin("v", iteration));
				client.get(rp, key);
			});
			L2Topology.waitFor(() -> warm.outcomes.size() >= WARM_THREADS * WARM_ITERATIONS);
			assertTrue("Warm-up threads blocked", warm.stop(L2Topology.WAIT_MS));
			assertEquals("Warm-up errors: " + L2Topology.kinds(warm.errors()), 0, warm.errors().size());

			List<Socket> probes = new ArrayList<>();
			long pooled = 0;

			try {
				for (L2Node node : nodes) {
					probes.add(probe(node));
				}

				Map<String,long[]> idle = L2Topology.connectionStats(client);

				for (long[] v : idle.values()) {
					pooled += v[L2Topology.IN_POOL];
				}
				System.out.println("L2-F1-IDLE maxSocketIdle=" + policy.maxSocketIdle + " pooled=" + pooled + ' ' +
					L2Topology.format(idle));
				assertTrue("No idle pooled connection to reset", pooled >= nodes.size());

				for (L2Node node : nodes) {
					L2Topology.setListener(node.proxy, false);
					L2Topology.setListener(node.proxy, true);
				}

				for (int i = 0; i < nodes.size(); i++) {
					String result = probeResult(probes.get(i));
					System.out.println("L2-F1-PROBE listener=" + nodes.get(i).port + " result=" + result);
					assertTrue("F-1: listener " + nodes.get(i).port + " did not reset an idle flow with RST: " + result,
						result.startsWith("RST"));
				}
			}
			finally {
				for (Socket probe : probes) {
					probe.close();
				}
			}

			ClusterStats before = client.getClusterStats();
			Map<String,long[]> start = L2Topology.connectionStats(client);
			List<String> failures = new ArrayList<>();
			int commands = 0;

			for (int i = 0; i < KEY_COUNT; i++) {
				Key key = new Key(L2Topology.namespace, SET, "f1-" + i);

				try {
					commands++;
					client.put(wp, key, new Bin("v", i));
				}
				catch (AerospikeException ae) {
					failures.add("put " + i + ": " + ae.getClass().getSimpleName() + ' ' + TestL2Topology.messages(ae));
				}

				try {
					commands++;
					client.get(rp, key);
				}
				catch (AerospikeException ae) {
					failures.add("get " + i + ": " + ae.getClass().getSimpleName() + ' ' + TestL2Topology.messages(ae));
				}
			}

			Map<String,long[]> end = L2Topology.connectionStats(client);
			List<String> undetected = new ArrayList<>();
			long closed = 0;
			long errors = 0;

			for (Map.Entry<String,long[]> entry : end.entrySet()) {
				long[] s = start.get(entry.getKey());
				long nodeClosed = entry.getValue()[L2Topology.CLOSED] - s[L2Topology.CLOSED];

				if (nodeClosed == 0) {
					undetected.add(entry.getKey());
				}
				closed += nodeClosed;
				errors += entry.getValue()[L2Topology.ERRORS] - s[L2Topology.ERRORS];
			}

			long retries = client.getClusterStats().retryCount - before.retryCount;
			System.out.println("L2-F1-RESULT commands=" + commands + " failedToCaller=" + failures.size() + " pooledReset=" +
				pooled + " closedDelta=" + closed + " errorDelta=" + errors + " retryDelta=" + retries +
				" writeMaxRetries=" + wp.maxRetries + " readMaxRetries=" + rp.maxRetries);
			System.out.println("L2-F1-STATS " + L2Topology.format(end));

			for (String failure : failures) {
				System.out.println("L2-F1-FAILED " + failure);
			}
			assertTrue("No reset pooled connection was detected on " + undetected, undetected.isEmpty());
			assertEquals("F-1: commands failed to the caller after idle RST: " + failures, 0, failures.size());
		}
	}

	private void addToxic(String proxy, String name, String type, double toxicity, String attributes) throws Exception {
		L2Topology.addToxic(proxy, name, type, "upstream", toxicity, attributes);
		toxics.add(new String[] {proxy, name});
	}

	/**
	 * Open a connection through a listener and complete one info request on it, so Toxiproxy has
	 * linked it to the node.
	 */
	static Socket probe(L2Node node) throws IOException {
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress(L2Topology.host, node.port), PROBE_TIMEOUT);
		socket.setSoTimeout(PROBE_TIMEOUT);

		byte[] command = "node\n".getBytes(StandardCharsets.UTF_8);
		ByteBuffer buffer = ByteBuffer.allocate(8 + command.length);
		buffer.putLong((2L << 56) | (1L << 48) | command.length).put(command);
		OutputStream out = socket.getOutputStream();
		out.write(buffer.array());

		DataInputStream in = new DataInputStream(socket.getInputStream());
		long header = in.readLong();
		byte[] reply = new byte[(int)(header & 0xFFFFFFFFFFFFL)];
		in.readFully(reply);
		return socket;
	}

	/**
	 * Read the next byte of a probe connection: RST when the peer reset it, FIN when it closed it.
	 */
	static String probeResult(Socket socket) {
		try {
			int b = socket.getInputStream().read();
			return b < 0 ? "FIN" : "DATA " + b;
		}
		catch (IOException e) {
			return (e.getMessage() != null && e.getMessage().contains("reset") ? "RST " : "ERROR ") + e;
		}
	}
}
