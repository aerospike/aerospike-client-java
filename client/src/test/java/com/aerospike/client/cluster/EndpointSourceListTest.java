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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.Endpoint.SourceList;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

/**
 * U-13: {@link PeerParser} sets {@link Endpoint#sourceList} from useServicesAlternate, and a
 * port-map keyed from the wrong list is detectable.
 */
public class EndpointSourceListTest {
	private static final String STD = "peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1]],[BB9B,,[10.0.0.2:3001]]]\n";
	private static final String ALT = "peers-clear-alt\t5,3000,[[BB9A,,[203.0.113.1:30001]],[BB9B,,[203.0.113.2:30002]]]\n";
	private static final Map<String,String> STD_PORT_MAP = Map.of("10.0.0.1:3000", "lb.example.com:31001",
		"10.0.0.2:3001", "lb.example.com:31002");

	private static ClientPolicy policy(boolean useServicesAlternate, AddressTranslator translator) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.useServicesAlternate = useServicesAlternate;
		policy.addressTranslator = translator;
		return policy;
	}

	private static List<Endpoint> endpoints(boolean useServicesAlternate, String reply) throws Exception {
		List<Endpoint> seen = new ArrayList<>();
		AddressTranslator recording = advertised -> {
			seen.add(advertised);
			return Optional.of(new Host(advertised.host, advertised.tlsName, advertised.port));
		};

		try (AerospikeClient client = new AerospikeClient(policy(useServicesAlternate, recording), new Host("127.0.0.1", 1))) {
			PeerReply.parse(client.getCluster(), reply);
		}
		return seen;
	}

	@Test
	public void standardFlagSetsStandardSourceList() throws Exception {
		List<Endpoint> seen = endpoints(false, STD);

		assertEquals(2, seen.size());
		assertEquals("BB9A 10.0.0.1:3000 STANDARD", seen.get(0).toString());
		assertEquals("BB9B 10.0.0.2:3001 STANDARD", seen.get(1).toString());
	}

	@Test
	public void alternateFlagSetsAlternateSourceList() throws Exception {
		List<Endpoint> seen = endpoints(true, STD + ALT);

		assertEquals(2, seen.size());
		assertEquals("BB9A 203.0.113.1:30001 ALTERNATE", seen.get(0).toString());
		assertEquals("BB9B 203.0.113.2:30002 ALTERNATE", seen.get(1).toString());
	}

	@Test
	public void portMapKeyedFromStandardListMissesAlternateEndpoints() throws Exception {
		ClientPolicy standard = policy(false, new PortMapAddressTranslator(STD_PORT_MAP));
		standard.translationOnMiss = TranslationMissMode.STRICT;

		try (AerospikeClient client = new AerospikeClient(standard, new Host("127.0.0.1", 1))) {
			List<Peer> peers = PeerReply.parse(client.getCluster(), STD);

			assertEquals(List.of(new Host("lb.example.com", 31001)), peers.get(0).hosts);
			assertEquals(List.of(new Host("lb.example.com", 31002)), peers.get(1).hosts);
		}

		ClientPolicy alternate = policy(true, new PortMapAddressTranslator(STD_PORT_MAP));
		alternate.translationOnMiss = TranslationMissMode.STRICT;
		List<String> warnings = new ArrayList<>();
		Thread testThread = Thread.currentThread();

		try (AerospikeClient client = new AerospikeClient(alternate, new Host("127.0.0.1", 1))) {
			Log.setLevel(Log.Level.WARN);
			Log.setCallback((level, message) -> {
				if (level == Log.Level.WARN && Thread.currentThread() == testThread) {
					warnings.add(message);
				}
			});

			List<Peer> peers;

			try {
				peers = PeerReply.parse(client.getCluster(), STD + ALT);
			}
			finally {
				Log.setCallback(null);
				Log.setLevel(Log.Level.INFO);
			}

			assertEquals(2, peers.size());
			assertTrue(peers.get(0).hosts.isEmpty());
			assertTrue(peers.get(1).hosts.isEmpty());
			assertEquals(2, warnings.size());
			assertNames(warnings.get(0), "port-map", "BB9A", "203.0.113.1:30001", "in the alternate list");
			assertNames(warnings.get(1), "port-map", "BB9B", "203.0.113.2:30002", "in the alternate list");
		}
	}

	private static void assertNames(String message, String... parts) {
		for (String part : parts) {
			assertTrue(message, message.contains(part));
		}
	}
}
