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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3.2 row F-2 against the low-mtu profile of test/discovery/run_l2.sh,
 * standing in for PrivateLink's MTU of 8500 with no path MTU discovery (section 4.7). The client
 * network is created with a small MTU and the client writes and reads records well above it. In
 * its own class and profile so that a failing F-2 does not fail the other rows.
 */
public class TestL2LowMtu extends TestBase {
	private static final String BLOCKER = "F-2 needs DISCOVERY_CLIENT_NETWORK_MTU, a client network created with " +
		"a small MTU (run_l2.sh L2_PROFILE=low-mtu)";
	private static final String SET = "l2mtu";
	private static final int[] RECORD_SIZES = {64 * 1024, 1024 * 1024};
	private static final int SOCKET_TIMEOUT = 5000;
	private static final int TOTAL_TIMEOUT = 10000;

	/**
	 * F-2: with the client network's MTU well below the record size, a write and a read of a 1 MiB
	 * value through every node's listener succeed, or fail with a diagnosable error, within a bound
	 * from socketTimeout and totalTimeout. A hang or a client timeout fails the row.
	 */
	@Test
	public void f2LargeRecordOverLowMtuNeverHangs() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.clientNetworkMtu.isEmpty(), BLOCKER);

		int mtu = Integer.parseInt(Files.readString(Paths.get("/sys/class/net/eth0/mtu"), StandardCharsets.US_ASCII).trim());
		System.out.println("L2-F2-MTU eth0=" + mtu + " network=" + L2Topology.clientNetworkMtu);
		assertEquals(Integer.parseInt(L2Topology.clientNetworkMtu), mtu);

		List<L2Node> nodes = L2Topology.nodes();
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));
		ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "l2-f2"));

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned("F-2", client);
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.socketTimeout = SOCKET_TIMEOUT;
			wp.totalTimeout = TOTAL_TIMEOUT;
			Policy rp = client.copyReadPolicyDefault();
			rp.socketTimeout = SOCKET_TIMEOUT;
			rp.totalTimeout = TOTAL_TIMEOUT;
			long bound = TOTAL_TIMEOUT + SOCKET_TIMEOUT;
			Random random = new Random(mtu);

			for (Node node : client.getNodes()) {
				Key key = keyOn(masters, node);

				for (int size : RECORD_SIZES) {
					byte[] value = new byte[size];
					random.nextBytes(value);

					String write = run(executor, bound, () -> {
						client.put(wp, key, new Bin("v", value));
						return null;
					});
					String read = run(executor, bound, () -> {
						Record record = client.get(rp, key);
						assertNotNull(record);
						assertArrayEquals(value, (byte[])record.getValue("v"));
						return null;
					});
					System.out.println("L2-F2-RECORD node=" + node.getName() + " listener=" + node.getHost().port + " mtu=" + mtu +
						" bytes=" + size + " write=" + write + " read=" + read + " boundMs=" + bound);
				}
			}
		}
		finally {
			executor.shutdownNow();
		}
	}

	private static Key keyOn(Node[] masters, Node node) {
		for (int i = 0; ; i++) {
			Key key = new Key(L2Topology.namespace, SET, "f2-" + i);

			if (masters[Partition.getPartitionId(key.digest)] == node) {
				return key;
			}
		}
	}

	/**
	 * Run a command with a bound and return {@code ok <ms>}, or {@code error <ms> <message>} for a
	 * diagnosable error. A hang past the bound or a client timeout fails the row.
	 */
	private static String run(ExecutorService executor, long bound, Callable<Void> command) throws Exception {
		long begin = System.nanoTime();
		Future<Void> future = executor.submit(command);

		try {
			future.get(bound, TimeUnit.MILLISECONDS);
			return "ok " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) + "ms";
		}
		catch (TimeoutException te) {
			future.cancel(true);
			throw new AssertionError("F-2: command still running after " + bound + "ms");
		}
		catch (ExecutionException ee) {
			long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			if (ee.getCause() instanceof AerospikeException) {
				AerospikeException ae = (AerospikeException)ee.getCause();
				String message = TestL2Topology.messages(ae);
				System.out.println("L2-F2-ERROR " + ms + "ms " + ae.getClass().getSimpleName() + ' ' + message);
				assertTrue("F-2: client timeout instead of a diagnosable error after " + ms + "ms: " + message,
					ae.getResultCode() != ResultCode.TIMEOUT);
				return "error " + ms + "ms " + message;
			}

			if (ee.getCause() instanceof Error) {
				throw (Error)ee.getCause();
			}
			throw ee;
		}
	}
}
