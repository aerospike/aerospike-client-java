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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.policy.ClientPolicy;

public class AddressTranslatorCloseTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);

	private final List<String> warnings = new CopyOnWriteArrayList<>();

	static final class Translator implements AddressTranslator {
		final AtomicInteger closes = new AtomicInteger();
		volatile Cluster cluster;
		volatile String closeThread;
		volatile boolean activeAtClose;
		volatile RuntimeException closeError;

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}

		@Override
		public void close() {
			closeThread = Thread.currentThread().getName();
			activeAtClose = cluster != null && cluster.isActive();
			closes.incrementAndGet();

			if (closeError != null) {
				throw closeError;
			}
		}
	}

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
	}

	@After
	public void tearDown() {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static ClientPolicy policy(AddressTranslator translator) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.addressTranslator = translator;
		return policy;
	}

	@Test
	public void closeCalledOnceAfterTendStops() throws Exception {
		Translator translator = new Translator();
		AerospikeClient client = new AerospikeClient(policy(translator), DEAD_SEED);
		translator.cluster = client.getCluster();
		Thread tend = tendThread(client.getCluster());
		assertEquals(0, translator.closes.get());

		client.close();
		await(() -> translator.closes.get() == 1);
		assertEquals("tend", translator.closeThread);
		assertFalse(translator.activeAtClose);
		tend.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(tend.isAlive());

		client.close();
		Thread.sleep(500);
		assertEquals(1, translator.closes.get());
	}

	@Test
	public void closeCalledOnceWhenConstructionFails() {
		List<Consumer<ClientPolicy>> failures = Arrays.asList(
			policy -> {
				policy.minConnsPerNode = 2;
				policy.maxConnsPerNode = 1;
			},
			policy -> {
				policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;
				policy.seedCandidateProvider = new TestSeedProvider(true, false, DEAD_SEED);
			},
			policy -> policy.failIfNotConnected = true,
			policy -> policy.forceSingleNode = true);

		for (Consumer<ClientPolicy> failure : failures) {
			Translator translator = new Translator();
			ClientPolicy policy = policy(translator);
			failure.accept(policy);

			assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));
			assertEquals(1, translator.closes.get());
			assertEquals(Thread.currentThread().getName(), translator.closeThread);
		}
	}

	@Test
	public void closeExceptionDoesNotStopShutdown() throws Exception {
		Translator translator = new Translator();
		translator.closeError = new IllegalStateException("close boom");
		AerospikeClient client = new AerospikeClient(policy(translator), DEAD_SEED);
		Thread tend = tendThread(client.getCluster());

		client.close();
		await(() -> translator.closes.get() == 1);
		tend.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(tend.isAlive());
		assertTrue(warnings.toString(), warnings.stream().anyMatch(m ->
			m.startsWith("Address translator " + Translator.class.getName() + " close failed") &&
			m.contains("close boom")));

		Translator failing = new Translator();
		failing.closeError = new IllegalStateException("close boom");
		ClientPolicy policy = policy(failing);
		policy.forceSingleNode = true;

		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));
		assertTrue(ae.getMessage(), ae.getMessage().contains("Seed " + DEAD_SEED + " failed"));
		assertEquals(1, failing.closes.get());
	}

	private static Thread tendThread(Cluster cluster) throws Exception {
		Field field = Cluster.class.getDeclaredField("tendThread");
		field.setAccessible(true);
		return (Thread)field.get(cluster);
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}
}
