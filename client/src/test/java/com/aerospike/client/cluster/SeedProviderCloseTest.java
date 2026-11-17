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
package com.aerospike.client.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.policy.ClientPolicy;

public class SeedProviderCloseTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);

	private static final Consumer<ClientPolicy> CONNS_INVALID = policy -> {
		policy.minConnsPerNode = 2;
		policy.maxConnsPerNode = 1;
	};
	private static final Consumer<ClientPolicy> NOT_CONNECTED = policy -> policy.failIfNotConnected = true;
	private static final Consumer<ClientPolicy> SINGLE_NODE = policy -> policy.forceSingleNode = true;

	@Test
	public void tendModeClosesProviderOnceWhenConstructionFails() throws Exception {
		for (Consumer<ClientPolicy> failure : List.of(CONNS_INVALID, NOT_CONNECTED, SINGLE_NODE)) {
			assertClosedOnce(new TestSeedProvider(true, true, DEAD_SEED), DiscoveryExecutionMode.TEND, failure);
		}
	}

	@Test
	public void rejectedProviderIsClosedOnce() throws Exception {
		assertClosedOnce(new TestSeedProvider(true, false, DEAD_SEED), DiscoveryExecutionMode.TEND, policy -> {});
	}

	@Test
	public void nonPeriodicProviderIsClosedOnceWhenConstructionFails() throws Exception {
		for (Consumer<ClientPolicy> failure : List.of(CONNS_INVALID, NOT_CONNECTED, SINGLE_NODE)) {
			assertClosedOnce(new TestSeedProvider(false, false, DEAD_SEED), DiscoveryExecutionMode.THREAD, failure);
		}
	}

	@Test
	public void threadModeClosesProviderOnceWhenConstructionFails() throws Exception {
		for (Consumer<ClientPolicy> failure : List.of(CONNS_INVALID, NOT_CONNECTED, SINGLE_NODE)) {
			assertClosedOnce(new TestSeedProvider(true, false, DEAD_SEED), DiscoveryExecutionMode.THREAD, failure);
		}
	}

	private static void assertClosedOnce(TestSeedProvider provider, DiscoveryExecutionMode mode,
		Consumer<ClientPolicy> failure) throws Exception {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.discoveryExecutionMode = mode;
		policy.seedCandidateProvider = provider;
		failure.accept(policy);

		assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));

		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (provider.closes.get() == 0) {
			assertTrue("provider not closed in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
		Thread.sleep(300);
		assertEquals(1, provider.closes.get());
	}
}
