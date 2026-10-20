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
package com.aerospike.test.translation;

import static com.aerospike.test.translation.PortMapCluster.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;

/**
 * A node whose port-map entry exists before the node does joins when it is added to the cluster,
 * with no client change. Starts node B, so it runs before {@link TestPortMap}.
 */
public class TestPortMapNodeJoin {
	@Test
	public void prefilledEntryJoinsNewNode() throws Exception {
		assertFalse("Node B container " + containerB + " is already running", containerRunning());

		ClientPolicy policy = policy();
		policy.addressTranslator = new PortMapAddressTranslator(Map.of(
			addressA, host + ':' + portA, addressB, host + ':' + portB));

		try (AerospikeClient client = client(policy)) {
			assertEquals(List.of(nodeA), nodeNames(client));
			assertAllPartitionsOwned("n19-before", client);

			Node seed = client.getNode(nodeA);
			String generation = Info.request(seed, "peers-generation");
			System.out.println("PORT-MAP-RESULT n19-before nodes=" + client.getNodes().length + ' ' + nodeNames(client) +
				" peersGeneration=" + generation);

			startContainerB();
			long started = System.currentTimeMillis();
			long peersChanged = 0;
			long joined = 0;
			long deadline = started + START_WAIT_MS;

			while (joined == 0 && System.currentTimeMillis() < deadline) {
				if (peersChanged == 0 && ! Info.request(seed, "peers-generation").equals(generation)) {
					peersChanged = System.currentTimeMillis();
				}

				if (client.getNodes().length == 2) {
					joined = System.currentTimeMillis();
				}
				Thread.sleep(10);
			}

			assertTrue("Node B did not join within " + START_WAIT_MS + "ms of being started", joined > 0);
			assertTrue("Peers change not observed before node B joined", peersChanged > 0);

			long joinMs = joined - peersChanged;
			System.out.println("PORT-MAP-RESULT n19-join nodes=" + client.getNodes().length + ' ' + nodeNames(client) +
				" startedToPeersChangeMs=" + (peersChanged - started) + " peersChangeToJoinMs=" + joinMs +
				" limitMs=" + (2 * TEND_INTERVAL) + " tendInterval=" + TEND_INTERVAL);

			assertEquals(List.of(nodeA, nodeB), nodeNames(client));
			assertTrue("Node B joined " + joinMs + "ms after the peers change, limit " + (2 * TEND_INTERVAL) + "ms",
				joinMs <= 2 * TEND_INTERVAL);

			Map<String,Integer> owners = assertAllPartitionsOwned("n19-after", client);
			assertEquals(owners.toString(), 2, owners.size());
		}
	}
}
