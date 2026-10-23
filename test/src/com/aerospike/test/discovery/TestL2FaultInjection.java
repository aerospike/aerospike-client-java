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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.L2Topology.Load;
import com.aerospike.test.discovery.L2Topology.Outcome;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3.2 row F-4 against the default {@link L2Topology}: a Toxiproxy
 * timeout toxic on one node's listener holds data with no reply and no close, as a conntrack
 * mismatch does. No client marks down a node the cluster still lists (strategy TG-24), so F-4
 * asserts bounded timeouts, no blocked threads, recovery, and the error-rate circuit breaker. Every
 * toxic a case adds is removed in {@link #restore()}.
 */
public class TestL2FaultInjection extends TestBase {
	private static final String SET = "l2f";
	private static final String TOXIC = "f4-hold";
	private static final int THREADS = 4;
	private static final int KEY_COUNT = 20;
	private static final int SOCKET_TIMEOUT = 500;
	private static final int TOTAL_TIMEOUT = 1500;
	private static final int MAX_RETRIES = 2;
	private static final int BREAKER_MAX_ERROR_RATE = 2;
	private static final int BREAKER_WINDOW = 4;

	private final List<String[]> toxics = new ArrayList<>();

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		for (String[] toxic : toxics) {
			L2Topology.removeToxic(toxic[0], toxic[1]);
		}
		toxics.clear();
	}

	/**
	 * F-4: commands to the node fail with a timeout within a bound from socketTimeout and
	 * totalTimeout, every error is a timeout, no command thread stays blocked past the bound, and the
	 * node serves again once the toxic is removed. Whether the client marks the node down is
	 * recorded, not asserted (TG-24).
	 */
	@Test
	public void f4SilentDropTimesOutWithoutBlocking() throws Exception {
		ClientPolicy policy = L2Topology.policy();
		TestL2Topology.captureWarnings();
		run(policy, (client, victim, wp, load, injected, timeoutBound) -> {
			assertTrue("F-4: fewer than " + THREADS + " timeouts after the drop",
				L2Topology.waitFor(() -> count(load.errors(), ResultCode.TIMEOUT) >= THREADS));
			boolean finished = load.stop(timeoutBound);
			List<Outcome> errors = load.errors();
			Node node = client.getCluster().getNode(victim.name);
			long firstTimeoutMs = -1;
			long maxElapsed = 0;

			for (Outcome outcome : errors) {
				maxElapsed = Math.max(maxElapsed, outcome.elapsedMs);
				assertEquals("F-4: error is not a timeout: " + outcome.kind() + ' ' + TestL2Topology.messages(outcome.error),
					ResultCode.TIMEOUT, resultCode(outcome));
				long at = TimeUnit.NANOSECONDS.toMillis(outcome.startNanos - injected) + outcome.elapsedMs;

				if (firstTimeoutMs < 0 || at < firstTimeoutMs) {
					firstTimeoutMs = at;
				}
			}

			int refreshFailures = 0;

			for (String warning : TestL2Topology.warnings) {
				if (warning.contains(victim.name) && warning.contains("refresh failed")) {
					refreshFailures++;
				}
			}

			System.out.println("L2-F4-TIMEOUT victim=" + victim.name + " listener=" + victim.port + " firstTimeoutMs=" +
				firstTimeoutMs + " maxErrorElapsedMs=" + maxElapsed + " timeoutBoundMs=" + timeoutBound + " errors=" +
				L2Topology.kinds(errors) + " successes=" + load.successes() + " threadsReturned=" + finished);
			System.out.println("L2-F4-NODE active=" + (node != null && node.isActive()) + " nodes=" + L2Topology.nodeNames(client) +
				" owners=" + L2Topology.owners(client) + " refreshFailedWarnings=" + refreshFailures);

			assertTrue("F-4: command threads still blocked " + timeoutBound + "ms after stop", finished);
			assertTrue("F-4: first timeout " + firstTimeoutMs + "ms after the drop, bound " + timeoutBound + "ms",
				firstTimeoutMs <= timeoutBound);
			assertTrue("F-4: an error took " + maxElapsed + "ms, bound " + timeoutBound + "ms", maxElapsed <= timeoutBound);
		});
	}

	/**
	 * F-4 with a low maxErrorRate: the timeouts trip the node's error-rate circuit breaker within a
	 * bound from errorRateWindow and tendInterval, and commands started after the trip fail fast with
	 * MAX_ERROR_RATE instead of waiting out socketTimeout.
	 */
	@Test
	public void f4SilentDropTripsCircuitBreaker() throws Exception {
		ClientPolicy policy = L2Topology.policy();
		policy.maxErrorRate = BREAKER_MAX_ERROR_RATE;
		policy.errorRateWindow = BREAKER_WINDOW;
		run(policy, (client, victim, wp, load, injected, timeoutBound) -> {
			long tripBound = (long)(policy.errorRateWindow + 1) * policy.tendInterval + timeoutBound;
			boolean tripped = L2Topology.waitFor(() -> count(load.errors(), ResultCode.MAX_ERROR_RATE) > 0);
			boolean finished = load.stop(timeoutBound);
			List<Outcome> backoffs = new ArrayList<>();
			long trippedNanos = Long.MAX_VALUE;

			for (Outcome outcome : load.errors()) {
				if (resultCode(outcome) == ResultCode.MAX_ERROR_RATE) {
					backoffs.add(outcome);
					trippedNanos = Math.min(trippedNanos, outcome.startNanos + TimeUnit.MILLISECONDS.toNanos(outcome.elapsedMs));
				}
			}

			long trippedMs = backoffs.isEmpty() ? -1 : TimeUnit.NANOSECONDS.toMillis(trippedNanos - injected);
			int afterTrip = 0;
			long maxBackoffMs = 0;

			for (Outcome outcome : backoffs) {
				if (outcome.startNanos >= trippedNanos) {
					afterTrip++;
					maxBackoffMs = Math.max(maxBackoffMs, outcome.elapsedMs);
				}
			}

			System.out.println("L2-F4-BREAKER victim=" + victim.name + " maxErrorRate=" + policy.maxErrorRate +
				" errorRateWindow=" + policy.errorRateWindow + " tendInterval=" + policy.tendInterval + " trippedMs=" + trippedMs +
				" tripBoundMs=" + tripBound + " backoffsAfterTrip=" + afterTrip + " maxBackoffElapsedMs=" + maxBackoffMs + " errors=" + L2Topology.kinds(load.errors()) +
				" threadsReturned=" + finished);

			assertTrue("F-4: circuit breaker never tripped: " + L2Topology.kinds(load.errors()), tripped);
			assertTrue("F-4: command threads still blocked " + timeoutBound + "ms after stop", finished);
			assertTrue("F-4: breaker tripped " + trippedMs + "ms after the drop, bound " + tripBound + "ms", trippedMs <= tripBound);
			assertTrue("F-4: no command started after the breaker tripped", afterTrip > 0);
			assertTrue("F-4: a command started after the trip took " + maxBackoffMs + "ms to fail, socketTimeout " +
				wp.socketTimeout + "ms", maxBackoffMs < wp.socketTimeout);
		});
	}

	private interface Drop {
		void check(AerospikeClient client, L2Node victim, WritePolicy wp, Load load, long injected, long timeoutBound)
			throws Exception;
	}

	/**
	 * Load the victim node with writes, drop its traffic silently, run the check, then remove the
	 * toxic and assert the node serves again and every partition is owned.
	 */
	private void run(ClientPolicy policy, Drop drop) throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		L2Node victim = nodes.get(nodes.size() - 1);
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned("F-4-before", client);
			Node node = client.getCluster().getNode(victim.name);
			List<Key> keys = new ArrayList<>();

			for (int i = 0; keys.size() < KEY_COUNT; i++) {
				Key key = new Key(L2Topology.namespace, SET, "f4-" + i);

				if (masters[Partition.getPartitionId(key.digest)] == node) {
					keys.add(key);
				}
			}

			WritePolicy wp = client.copyWritePolicyDefault();
			wp.socketTimeout = SOCKET_TIMEOUT;
			wp.totalTimeout = TOTAL_TIMEOUT;
			wp.maxRetries = MAX_RETRIES;
			long timeoutBound = wp.totalTimeout + wp.socketTimeout;
			Load load = new Load("l2-f4", THREADS, (thread, iteration) ->
				client.put(wp, keys.get((thread + iteration * THREADS) % keys.size()), new Bin("v", iteration)));

			try {
				assertTrue("No command succeeded before the drop", L2Topology.waitFor(() -> load.successes() >= THREADS));
				long injected = System.nanoTime();
				addToxic(victim.proxy, TOXIC, "timeout", 1.0, "{\"timeout\":0}");
				drop.check(client, victim, wp, load, injected, timeoutBound);
			}
			finally {
				load.stop(timeoutBound);
			}

			removeToxic(victim.proxy, TOXIC);
			long removed = System.nanoTime();
			Key key = keys.get(0);
			boolean back = L2Topology.waitFor(() -> L2Topology.nodeNames(client).contains(victim.name) &&
				client.getCluster().getNode(victim.name).isActive() && L2Topology.owners(client).containsKey(victim.name) &&
				writes(client, wp, key));
			System.out.println("L2-F4-RECOVERED back=" + back + " ms=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - removed) +
				" nodes=" + L2Topology.nodeNames(client) + " owners=" + L2Topology.owners(client));
			assertTrue("F-4: " + victim.name + " did not come back after the toxic was removed", back);
			L2Topology.assertAllPartitionsOwned("F-4-after", client);
		}
	}

	private void addToxic(String proxy, String name, String type, double toxicity, String attributes) throws Exception {
		L2Topology.addToxic(proxy, name, type, "upstream", toxicity, attributes);
		toxics.add(new String[] {proxy, name});
	}

	private void removeToxic(String proxy, String name) throws Exception {
		toxics.removeIf(toxic -> toxic[0].equals(proxy) && toxic[1].equals(name));
		L2Topology.removeToxic(proxy, name);
	}

	private static int resultCode(Outcome outcome) {
		return outcome.error instanceof AerospikeException ? ((AerospikeException)outcome.error).getResultCode() :
			ResultCode.CLIENT_ERROR;
	}

	private static long count(List<Outcome> outcomes, int resultCode) {
		return outcomes.stream().filter(o -> resultCode(o) == resultCode).count();
	}

	private static boolean writes(AerospikeClient client, WritePolicy policy, Key key) {
		try {
			client.put(policy, key, new Bin("v", 0));
			return true;
		}
		catch (AerospikeException.InvalidNode | AerospikeException.Timeout | AerospikeException.Connection |
			AerospikeException.Backoff e) {
			return false;
		}
	}
}
