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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.L2Topology.Load;
import com.aerospike.test.discovery.L2Topology.Outcome;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3.2 row F-3 against the port-range profile of
 * test/discovery/run_l2.sh, standing in for NLB source NAT port exhaustion (section 4.7). Toxiproxy
 * runs with a narrow local port range, which caps the upstream connections it can open to each
 * node. In its own class and profile so that a failing F-3 does not fail the other rows.
 */
public class TestL2SourcePortRange extends TestBase {
	private static final String BLOCKER = "F-3 needs DISCOVERY_PROXY_PORT_RANGE, Toxiproxy with a narrow " +
		"net.ipv4.ip_local_port_range (run_l2.sh L2_PROFILE=port-range)";
	private static final String SET = "l2ports";
	private static final int KEY_COUNT = 1000;
	private static final int RECOVERY_KEY_COUNT = 100;
	private static final int SOCKET_TIMEOUT = 2000;
	private static final int TOTAL_TIMEOUT = 4000;

	/**
	 * F-3: with more concurrent commands than the proxy has source ports for, and maxConnsPerNode
	 * raised above that, the client surfaces a clear error within a bound from socketTimeout and
	 * totalTimeout rather than hanging, the error types and counts are observable from the client,
	 * and commands succeed again once the load drops.
	 */
	@Test
	public void f3SourcePortExhaustionSurfacesErrorAndRecovers() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.proxyPortRange.isEmpty(), BLOCKER);

		String[] range = L2Topology.proxyPortRange.trim().split("\\s+");
		int ports = Integer.parseInt(range[1]) - Integer.parseInt(range[0]) + 1;
		List<L2Node> nodes = L2Topology.nodes();
		int threads = 2 * ports * nodes.size();
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));
		policy.maxConnsPerNode = 4 * threads;
		System.out.println("L2-F3-RANGE proxyLocalPortRange=" + L2Topology.proxyPortRange + " ports=" + ports +
			" nodes=" + nodes.size() + " threads=" + threads + " maxConnsPerNode=" + policy.maxConnsPerNode +
			" defaultMaxConnsPerNode=" + new ClientPolicy().maxConnsPerNode);

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("F-3-before", client);

			WritePolicy wp = client.copyWritePolicyDefault();
			wp.socketTimeout = SOCKET_TIMEOUT;
			wp.totalTimeout = TOTAL_TIMEOUT;
			long bound = wp.totalTimeout + wp.socketTimeout;
			Map<String,long[]> start = L2Topology.connectionStats(client);
			long begin = System.nanoTime();

			Load load = new Load("l2-f3", threads, (thread, iteration) ->
				client.put(wp, new Key(L2Topology.namespace, SET, "f3-" + ((thread * 31 + iteration) % KEY_COUNT)),
					new Bin("v", iteration)));
			boolean finished;

			try {
				L2Topology.waitFor(() -> load.errors().size() >= threads);
			}
			finally {
				finished = load.stop(bound);
			}

			List<Outcome> errors = load.errors();
			long firstErrorMs = -1;
			long maxElapsed = 0;
			List<Outcome> timeouts = new ArrayList<>();

			for (Outcome outcome : errors) {
				long at = TimeUnit.NANOSECONDS.toMillis(outcome.startNanos - begin) + outcome.elapsedMs;
				firstErrorMs = firstErrorMs < 0 ? at : Math.min(firstErrorMs, at);
				maxElapsed = Math.max(maxElapsed, outcome.elapsedMs);

				if (outcome.error instanceof AerospikeException &&
					((AerospikeException)outcome.error).getResultCode() == ResultCode.TIMEOUT) {
					timeouts.add(outcome);
				}
			}

			Map<String,long[]> end = L2Topology.connectionStats(client);
			long errorDelta = 0;
			long pooled = 0;

			for (Map.Entry<String,long[]> entry : end.entrySet()) {
				long[] v = entry.getValue();
				errorDelta += v[L2Topology.ERRORS] - start.get(entry.getKey())[L2Topology.ERRORS];
				pooled = Math.max(pooled, v[L2Topology.IN_USE] + v[L2Topology.IN_POOL]);
			}

			System.out.println("L2-F3-LOAD commands=" + load.outcomes.size() + " successes=" + load.successes() +
				" errors=" + L2Topology.kinds(errors) + " firstErrorMs=" + firstErrorMs + " maxErrorElapsedMs=" + maxElapsed +
				" boundMs=" + bound + " threadsReturned=" + finished + " clientErrorDelta=" + errorDelta +
				" maxConnsPerNodeAfterLoad=" + pooled);
			System.out.println("L2-F3-STATS " + L2Topology.format(end));

			if (! errors.isEmpty()) {
				System.out.println("L2-F3-ERROR " + errors.get(0).kind() + ' ' + TestL2Topology.messages(errors.get(0).error));
			}

			assertTrue("F-3: command threads still blocked " + bound + "ms after stop", finished);
			assertFalse("F-3: " + threads + " threads never exceeded the proxy's " + ports + " source ports per node",
				errors.isEmpty());
			assertTrue("F-3: an error took " + maxElapsed + "ms, bound " + bound + "ms", maxElapsed <= bound);
			assertTrue("F-3: client counted no error: " + L2Topology.format(end), errorDelta > 0);

			for (Outcome outcome : errors) {
				assertTrue("F-3: not a client error: " + outcome.kind(), outcome.error instanceof AerospikeException);
			}
			assertEquals("F-3: source port exhaustion surfaced as a client timeout: " + L2Topology.kinds(timeouts), 0,
				timeouts.size());

			Policy rp = client.copyReadPolicyDefault();
			long dropped = System.nanoTime();
			boolean recovered = L2Topology.waitFor(() -> {
				for (Node node : client.getNodes()) {
					if (! node.isActive()) {
						return false;
					}
				}
				return roundSucceeds(client, wp, rp, 4 * nodes.size());
			});
			long recoveredMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - dropped);
			List<String> failures = new ArrayList<>();

			for (int i = 0; i < RECOVERY_KEY_COUNT; i++) {
				Key key = new Key(L2Topology.namespace, SET, "f3-" + i);

				try {
					client.put(wp, key, new Bin("v", i));
					assertEquals(i, client.get(rp, key).getInt("v"));
				}
				catch (AerospikeException ae) {
					failures.add(i + ": " + ae.getClass().getSimpleName() + ' ' + ae.getMessage());
				}
			}
			System.out.println("L2-F3-RECOVERY recovered=" + recovered + " ms=" + recoveredMs + " keys=" + RECOVERY_KEY_COUNT +
				" failed=" + failures.size() + " nodes=" + L2Topology.nodeNames(client));
			assertTrue("F-3: no successful round of commands after the load dropped", recovered);
			assertEquals("F-3: commands failed after the load dropped: " + failures, 0, failures.size());
		}
	}

	private static boolean roundSucceeds(AerospikeClient client, WritePolicy wp, Policy rp, int count) {
		try {
			for (int i = 0; i < count; i++) {
				Key key = new Key(L2Topology.namespace, SET, "f3-round-" + i);
				client.put(wp, key, new Bin("v", i));
				client.get(rp, key);
			}
			return true;
		}
		catch (AerospikeException ae) {
			return false;
		}
	}
}
