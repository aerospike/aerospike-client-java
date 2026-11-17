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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;

/**
 * The L5 driver's port-map shape against two stub nodes: the seed advertises its peer at an
 * address the client has no route to, and the port-map, keyed by node name or by the advertised
 * {@code HOST:PORT}, maps it to the peer's listener. Needs no server.
 */
public class TestL5DriverPortMap {
	private static final String PEERS = "1,3000,[[BB9B,,[10.255.0.2]]]";

	@Test
	public void nodeNameKeyRoutesPeer() throws Exception {
		try (StubInfoNode a = new StubInfoNode("BB9A", null); StubInfoNode b = new StubInfoNode("BB9B", null)) {
			a.values.put("peers-clear-std", PEERS);
			assertPeerAt(a, b, "BB9B=127.0.0.1:" + b.port());
		}
	}

	@Test
	public void advertisedAddressKeyRoutesPeer() throws Exception {
		try (StubInfoNode a = new StubInfoNode("BB9A", null); StubInfoNode b = new StubInfoNode("BB9B", null)) {
			a.values.put("peers-clear-std", PEERS);
			assertPeerAt(a, b, "10.255.0.2:3000=127.0.0.1:" + b.port());
		}
	}

	private static void assertPeerAt(StubInfoNode a, StubInfoNode b, String portMap) throws InterruptedException {
		L5Driver.Options options = new L5Driver.Options(new String[] {"--seed", "127.0.0.1:" + a.port(), "--shape", "port-map",
			"--port-map", portMap, "--tend-interval", "250", "--timeout", "1000", "--namespace", "test", "--set", "l5"}, null);

		try (AerospikeClient client = new AerospikeClient(options.policy(), options.seed)) {
			Node peer = await(client, "BB9B");
			assertEquals(new Host("127.0.0.1", b.port()), peer.getHost());
			assertTrue(b.commands().contains("node"));
		}
	}

	static Node await(AerospikeClient client, String name) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (System.nanoTime() < deadline) {
			for (Node node : client.getNodes()) {
				if (node.getName().equals(name)) {
					return node;
				}
			}
			Thread.sleep(20);
		}
		throw new AssertionError("client never held " + name + "; nodes " + Arrays.toString(client.getNodes()));
	}
}
