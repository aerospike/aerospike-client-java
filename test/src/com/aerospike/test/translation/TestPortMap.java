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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

/**
 * port-map translator against both nodes, each reachable only through its port-map entry.
 */
public class TestPortMap {
	private static final int MISS_TENDS = 5;
	private static final int KEY_COUNT = 200;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	@BeforeClass
	public static void init() throws Exception {
		startContainerB();
		assertUnreachable(addressA);
		assertUnreachable(addressB);

		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});
	}

	@AfterClass
	public static void destroy() {
		Log.setCallback(null);
	}

	@Before
	public void clearWarnings() {
		warnings.clear();
	}

	@Test
	public void hostPortKeysJoinBothNodes() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new PortMapAddressTranslator(Map.of(
			addressA, host + ':' + portA, addressB, host + ':' + portB));
		assertJoins("n2-host-port-keys", policy);
	}

	@Test
	public void nodeNameKeysJoinBothNodes() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new PortMapAddressTranslator(Map.of(
			nodeA, host + ':' + portA, nodeB, host + ':' + portB));
		assertJoins("node-name-keys", policy);
	}

	@Test
	public void strictUnmappedNodeNeverCreated() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new PortMapAddressTranslator(Map.of(addressA, host + ':' + portA));
		policy.translationOnMiss = TranslationMissMode.STRICT;

		try (AerospikeClient client = client(policy)) {
			for (int i = 0; i < MISS_TENDS; i++) {
				Thread.sleep(TEND_INTERVAL);
				assertEquals(List.of(nodeA), nodeNames(client));
			}

			List<String> misses = new ArrayList<>();

			for (String warning : warnings) {
				if (warning.contains("has no mapping for node " + nodeB)) {
					misses.add(warning);
				}
			}

			System.out.println("PORT-MAP-RESULT strict-unmapped nodes=" + client.getNodes().length + ' ' +
				nodeNames(client) + " missWarnings=" + misses.size());

			if (misses.isEmpty()) {
				fail("No translation miss logged for node " + nodeB + ": " + warnings);
			}

			String message = misses.get(0);
			System.out.println("PORT-MAP-MISS " + message);

			for (String part : new String[] {"Address translator port-map ", nodeB, addressB, "standard"}) {
				assertTrue(message, message.contains(part));
			}
		}
	}

	private static void assertJoins(String name, ClientPolicy policy) throws Exception {
		try (AerospikeClient client = client(policy)) {
			waitNodes(client, 2);

			List<String> names = nodeNames(client);
			System.out.println("PORT-MAP-RESULT " + name + " nodes=" + names.size() + ' ' + names);
			assertEquals(List.of(nodeA, nodeB), names);

			Map<String,Integer> owners = assertAllPartitionsOwned(name, client);
			assertEquals(owners.toString(), 2, owners.size());

			for (int i = 0; i < KEY_COUNT; i++) {
				Key key = new Key(namespace, "portmap", i);
				client.put(null, key, new Bin("v", i));
				Record record = client.get(null, key);
				assertEquals(i, record.getInt("v"));
			}
			System.out.println("PORT-MAP-KEYS " + name + " written=" + KEY_COUNT + " read=" + KEY_COUNT);
			assertEquals(List.of(), warnings);
		}
	}
}
