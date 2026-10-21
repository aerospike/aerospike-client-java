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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

public class DefaultTranslationTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Map<String,String> IP_MAP = Map.of("10.0.0.1", "192.168.1.1", "node2.internal", "node2.external",
		"2001:db8::1", "2001:db8::99");
	private static final String PEERS = "peers-clear-std\t5,3000,[[BB9A,tls1,[10.0.0.1:3001,10.0.0.9]]," +
		"[BB9B,,[node2.internal,node3.internal:3002]],[BB9C,,[[2001:db8::1]:3003,[2001:db8::2]]]]\n";
	private static final String[][] ADDRESSES = new String[][] {
		{"BB9A", "tls1", "10.0.0.1"}, {"BB9A", "tls1", "10.0.0.9"}, {"BB9B", null, "node2.internal"},
		{"BB9B", null, "node3.internal"}, {"BB9C", null, "2001:db8::1"}, {"BB9C", null, "2001:db8::2"}
	};

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	/**
	 * U-3: a client built in code with no translator, no seed candidate provider and no YAML
	 * translates through static-map and starts no thread besides tend.
	 */
	@Test
	public void codeBuiltClientDefaultsToStaticMap() {
		ClientPolicy policy = policy();
		policy.ipMap = IP_MAP;

		assertNull(policy.addressTranslator);
		assertNull(policy.seedCandidateProvider);
		assertNull(YamlConfigProvider.getConfigPath());

		Set<Thread> before = Thread.getAllStackTraces().keySet();

		try (AerospikeClient client = new AerospikeClient(policy, DEAD_SEED)) {
			assertEquals(List.of("tend"), newThreadNames(before));

			Cluster cluster = client.getCluster();
			assertTrue(cluster.addressTranslator instanceof StaticMapAddressTranslator);
			assertEquals("static-map", cluster.translatorName());

			Host hit = cluster.translateAddress("BB9A", "tls1", "10.0.0.1", 3000);
			assertEquals(new Host("192.168.1.1", 3000), hit);
			assertEquals("tls1", hit.tlsName);

			Host miss = cluster.translateAddress("BB9A", "tls1", "10.0.0.9", 3001);
			assertEquals(new Host("10.0.0.9", 3001), miss);
			assertEquals("tls1", miss.tlsName);

			assertEquals(List.of("tend"), newThreadNames(before));
		}
	}

	/**
	 * U-7: ipMap and an explicit StaticMapAddressTranslator over the same map produce identical
	 * hosts for the same peers, hits and misses, for every on_miss mode.
	 */
	@Test
	public void ipMapMatchesExplicitStaticMap() throws Exception {
		for (TranslationMissMode mode : TranslationMissMode.values()) {
			ClientPolicy legacy = policy();
			legacy.ipMap = IP_MAP;
			legacy.translationOnMiss = mode;

			ClientPolicy explicit = policy();
			explicit.addressTranslator = new StaticMapAddressTranslator(IP_MAP);
			explicit.translationOnMiss = mode;

			try (AerospikeClient legacyClient = new AerospikeClient(legacy, DEAD_SEED);
				AerospikeClient explicitClient = new AerospikeClient(explicit, DEAD_SEED)) {
				Cluster legacyCluster = legacyClient.getCluster();
				Cluster explicitCluster = explicitClient.getCluster();
				List<Peer> legacyPeers = PeerReply.parse(legacyCluster, PEERS);
				List<Peer> explicitPeers = PeerReply.parse(explicitCluster, PEERS);

				assertEquals(describe(legacyPeers), describe(explicitPeers));

				if (mode == TranslationMissMode.STRICT) {
					assertEquals(List.of("BB9A [192.168.1.1 3001 'tls1']", "BB9B [node2.external 3000 '']",
						"BB9C [2001:db8::99 3003 '']"), describe(legacyPeers));
				}
				else {
					assertEquals(List.of("BB9A [192.168.1.1 3001 'tls1', 10.0.0.9 3000 'tls1']",
						"BB9B [node2.external 3000 '', node3.internal 3002 '']",
						"BB9C [2001:db8::99 3003 '', 2001:db8::2 3000 '']"), describe(legacyPeers));
				}

				for (String[] a : ADDRESSES) {
					assertEquals(translate(legacyCluster, a), translate(explicitCluster, a));
				}
			}
		}
	}

	private static String translate(Cluster cluster, String[] address) {
		try {
			Host host = cluster.translateAddress(address[0], address[1], address[2], 3000);
			return host + " " + host.tlsName;
		}
		catch (AerospikeException.TranslationMiss tm) {
			return tm.getMessage();
		}
	}

	private static List<String> describe(List<Peer> peers) {
		List<String> list = new ArrayList<>();

		for (Peer peer : peers) {
			List<String> hosts = new ArrayList<>();

			for (Host host : peer.hosts) {
				hosts.add(host + " '" + host.tlsName + "'");
			}
			list.add(peer.nodeName + ' ' + hosts);
		}
		return list;
	}

	private static List<String> newThreadNames(Set<Thread> before) {
		List<String> names = new ArrayList<>();

		for (Thread thread : new HashSet<>(Thread.getAllStackTraces().keySet())) {
			if (! before.contains(thread) && thread.isAlive()) {
				names.add(thread.getName());
			}
		}
		names.sort(null);
		return names;
	}
}
