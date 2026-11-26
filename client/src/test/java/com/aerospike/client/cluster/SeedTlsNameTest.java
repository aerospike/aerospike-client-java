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

import static com.aerospike.client.cluster.TendDriver.stopTendThread;
import static com.aerospike.client.cluster.TendDriver.tend;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.StubNodeServer;
import com.aerospike.client.policy.ClientPolicy;

/**
 * With TLS off, a seed's TLS name only aborts the seed loop when it is non-empty.
 */
public class SeedTlsNameTest {
	private static final String LIVE = "BB9A000000000001";
	private static final String LOST = "BB9B000000000001";

	private final List<StubNodeServer> servers = new ArrayList<>();
	private final List<String> seedFailures = new ArrayList<>();
	private AerospikeClient client;

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (message.startsWith("Seed ")) {
				synchronized (seedFailures) {
					seedFailures.add(message);
				}
			}
		});
	}

	@After
	public void tearDown() throws Exception {
		if (client != null) {
			client.close();
		}

		for (StubNodeServer server : servers) {
			server.close();
		}
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	@Test
	public void reseedSkipsEmptyTlsNameSeed() throws Exception {
		StubNodeServer live = server(LIVE);
		Host lost = new Host("127.0.0.1", 1);
		Host emptyTlsName = new Host("127.0.0.1", "", 2);
		Host liveHost = new Host("127.0.0.1", live.port());
		Cluster cluster = lostEveryNode();

		cluster.setSeeds(new Host[] {lost, emptyTlsName, liveHost});
		tend(cluster);

		Node[] nodes = cluster.getNodes();
		assertEquals(1, nodes.length);
		assertEquals(LIVE, nodes[0].getName());
		assertEquals(2, seedFailures().size());
		assertTrue(seedFailures().get(1), seedFailures().get(1).startsWith("Seed " + emptyTlsName + " failed"));
	}

	@Test
	public void nonEmptyTlsNameSeedFailsFast() throws Exception {
		StubNodeServer live = server(LIVE);
		Cluster cluster = lostEveryNode();

		cluster.setSeeds(new Host[] {new Host("127.0.0.1", "tls-name", 2), new Host("127.0.0.1", live.port())});

		try {
			tend(cluster);
			fail("tend did not fail on a TLS name with TLS off");
		}
		catch (AerospikeException.Connection ae) {
			assertTrue(ae.getMessage(), ae.getMessage().contains("Seed host tlsName 'tls-name' defined but client tlsPolicy not enabled"));
		}
		assertEquals(0, cluster.getNodes().length);
		assertEquals(0, live.connections());
	}

	@Test
	public void nullTlsNameSeedDoesNotFailFast() throws Exception {
		StubNodeServer live = server(LIVE);
		Cluster cluster = lostEveryNode();

		cluster.setSeeds(new Host[] {new Host("127.0.0.1", 2), new Host("127.0.0.1", live.port())});
		tend(cluster);

		assertEquals(1, cluster.getNodes().length);
		assertEquals(LIVE, cluster.getNodes()[0].getName());
	}

	private Cluster lostEveryNode() throws Exception {
		StubNodeServer lost = server(LOST);
		ClientPolicy policy = new ClientPolicy();
		policy.timeout = 500;
		policy.failIfNotConnected = false;

		client = new AerospikeClient(policy, new Host("127.0.0.1", lost.port()));
		Cluster cluster = client.getCluster();
		stopTendThread(cluster);
		assertEquals(1, cluster.getNodes().length);
		assertNull(cluster.tlsPolicy);

		lost.close();

		for (int i = 0; i < 10 && cluster.getNodes().length > 0; i++) {
			tend(cluster);
		}
		assertEquals(0, cluster.getNodes().length);

		synchronized (seedFailures) {
			seedFailures.clear();
		}
		return cluster;
	}

	private StubNodeServer server(String nodeName) throws Exception {
		StubNodeServer server = new StubNodeServer(InetAddress.getByName("127.0.0.1"), 0, nodeName);
		servers.add(server);
		return server;
	}

	private List<String> seedFailures() {
		synchronized (seedFailures) {
			return new ArrayList<>(seedFailures);
		}
	}
}
