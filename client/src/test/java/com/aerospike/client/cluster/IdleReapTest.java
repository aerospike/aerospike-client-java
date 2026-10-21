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
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

/**
 * U-12: idle connections are reaped under the AWS NLB idle timeout of 350 seconds, by default and,
 * with minConnsPerNode set, when maxSocketIdle is set below that timeout.
 */
public class IdleReapTest {
	private static final long NLB_IDLE_TIMEOUT_SECS = 350;
	private static final int MAX_SOCKET_IDLE_UNDER_NLB = 340;

	private static AerospikeClient client(int minConnsPerNode, int maxSocketIdle) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.maxSocketIdle = maxSocketIdle;
		policy.minConnsPerNode = minConnsPerNode;
		return new AerospikeClient(policy, new Host("127.0.0.1", 1));
	}

	private static long idleFor(long secs) {
		return System.nanoTime() - TimeUnit.SECONDS.toNanos(secs);
	}

	private static Pool reservePool() {
		Pool pool = new Pool(2, 4);
		pool.total.set(2);
		assertEquals(0, pool.excess());
		return pool;
	}

	@Test
	public void defaultTrimLimitUnderNlbIdleTimeout() throws Exception {
		Field field = Cluster.class.getDeclaredField("MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS");
		field.setAccessible(true);
		long trim = field.getLong(null);

		assertEquals(55, trim);
		assertTrue(trim < NLB_IDLE_TIMEOUT_SECS);
	}

	@Test
	public void connectionAboveMinConnsPerNodeIsTrimmed() {
		try (AerospikeClient client = client(2, 0)) {
			Pool pool = new Pool(2, 4);
			pool.total.set(3);

			assertEquals(1, pool.excess());
			assertFalse(client.getCluster().isConnCurrentTrim(idleFor(NLB_IDLE_TIMEOUT_SECS)));
		}
	}

	@Test
	public void reserveConnectionKeptWhenMaxSocketIdleIsZero() {
		try (AerospikeClient client = client(2, 0)) {
			reservePool();
			assertTrue(client.getCluster().isConnCurrentTran(idleFor(NLB_IDLE_TIMEOUT_SECS)));
		}
	}

	@Test
	public void reserveConnectionDiscardedOnCheckoutWhenMaxSocketIdleUnderNlbTimeout() {
		assertTrue(MAX_SOCKET_IDLE_UNDER_NLB < NLB_IDLE_TIMEOUT_SECS);

		try (AerospikeClient client = client(2, MAX_SOCKET_IDLE_UNDER_NLB)) {
			Cluster cluster = client.getCluster();
			reservePool();

			assertFalse(cluster.isConnCurrentTran(idleFor(NLB_IDLE_TIMEOUT_SECS)));
			assertFalse(cluster.isConnCurrentTran(idleFor(MAX_SOCKET_IDLE_UNDER_NLB + 1)));
			assertTrue(cluster.isConnCurrentTran(idleFor(MAX_SOCKET_IDLE_UNDER_NLB - 10)));
		}
	}
}
