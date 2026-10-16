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
package com.aerospike.client.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.discovery.Endpoint.SourceList;

public class PortMapAddressTranslatorTest {
	private static final String NODE_A = "BB9020011AC4202";
	private static final String NODE_B = "BB9030011AC4202";

	private static Host translate(Map<String,String> map, Endpoint advertised) {
		return new PortMapAddressTranslator(map).translate(advertised).get();
	}

	private static void assertHost(String name, int port, String tlsName, Host host) {
		assertEquals(name, host.name);
		assertEquals(port, host.port);
		assertEquals(tlsName, host.tlsName);
	}

	@Test
	public void hostPortKeyHitReplacesHostAndPort() {
		Map<String,String> map = Map.of("10.0.0.1:3000", "lb.example.com:30001", "10.0.0.2:3000", "lb.example.com:30002");
		Host host = translate(map, new Endpoint(NODE_A, "db1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertHost("lb.example.com", 30001, "db1", host);
	}

	@Test
	public void hostPortKeyHitKeepsNullTlsName() {
		Host host = translate(Map.of("10.0.0.2:3000", "lb.example.com:30002"),
			new Endpoint(NODE_B, null, "10.0.0.2", 3000, SourceList.STANDARD));

		assertHost("lb.example.com", 30002, null, host);
	}

	@Test
	public void hostPortKeyOtherPortMisses() {
		PortMapAddressTranslator translator = new PortMapAddressTranslator(Map.of("10.0.0.1:3000", "lb.example.com:30001"));

		assertFalse(translator.translate(new Endpoint(NODE_A, "db1", "10.0.0.1", 4333, SourceList.STANDARD)).isPresent());
	}

	@Test
	public void nodeNameKeyHit() {
		Map<String,String> map = Map.of(NODE_A, "lb.example.com:30001", NODE_B, "lb.example.com:30002");
		Host host = translate(map, new Endpoint(NODE_B, "db2", "10.0.0.2", 3000, SourceList.STANDARD));

		assertHost("lb.example.com", 30002, "db2", host);
	}

	@Test
	public void nodeNameKeyMatchesAnyAdvertisedAddress() {
		PortMapAddressTranslator translator = new PortMapAddressTranslator(Map.of(NODE_A, "lb.example.com:30001"));

		for (Endpoint advertised : new Endpoint[] {
			new Endpoint(NODE_A, null, "10.0.0.1", 3000, SourceList.STANDARD),
			new Endpoint(NODE_A, null, "198.51.100.1", 4333, SourceList.ALTERNATE),
			new Endpoint(NODE_A, null, "2001:db8::10", 3000, SourceList.ALTERNATE)}) {
			assertHost("lb.example.com", 30001, null, translator.translate(advertised).get());
		}
	}

	@Test
	public void nodeNameKeyMissesOtherNode() {
		PortMapAddressTranslator translator = new PortMapAddressTranslator(Map.of(NODE_A, "lb.example.com:30001"));

		assertFalse(translator.translate(new Endpoint(NODE_B, "db2", "10.0.0.2", 3000, SourceList.STANDARD)).isPresent());
	}

	@Test
	public void nullNodeNameMissesNodeNameKeys() {
		PortMapAddressTranslator translator = new PortMapAddressTranslator(Map.of(NODE_A, "lb.example.com:30001"));

		assertFalse(translator.translate(new Endpoint(null, null, "10.0.0.1", 3000, SourceList.STANDARD)).isPresent());
	}

	@Test
	public void ipv6KeyHit() {
		Host host = translate(Map.of("[2001:db8::10]:3000", "lb.example.com:30001"),
			new Endpoint(NODE_A, "db1", "2001:db8::10", 3000, SourceList.STANDARD));

		assertHost("lb.example.com", 30001, "db1", host);
	}

	@Test
	public void ipv6ValueIsUnbracketed() {
		Host host = translate(Map.of("10.0.0.1:3000", "[2001:db8::1]:30001"),
			new Endpoint(NODE_A, null, "10.0.0.1", 3000, SourceList.STANDARD));

		assertHost("2001:db8::1", 30001, null, host);
	}

	@Test
	public void ipv6KeyAndValue() {
		Host host = translate(Map.of("[2001:db8::10]:3000", "[2001:db8::1]:30001"),
			new Endpoint(NODE_A, "db1", "2001:db8::10", 3000, SourceList.ALTERNATE));

		assertHost("2001:db8::1", 30001, "db1", host);
	}

	@Test
	public void hostPortKeyTakesPrecedenceOverNodeName() {
		Map<String,String> map = Map.of("10.0.0.1:3000", "lb.example.com:30001", NODE_A, "lb.example.com:30002");
		PortMapAddressTranslator translator = new PortMapAddressTranslator(map);

		assertHost("lb.example.com", 30001, "db1",
			translator.translate(new Endpoint(NODE_A, "db1", "10.0.0.1", 3000, SourceList.STANDARD)).get());
		assertHost("lb.example.com", 30002, "db1",
			translator.translate(new Endpoint(NODE_A, "db1", "10.0.0.1", 4000, SourceList.STANDARD)).get());
	}

	@Test
	public void mapIsCopied() {
		Map<String,String> map = new HashMap<>();
		map.put("10.0.0.1:3000", "lb.example.com:30001");
		PortMapAddressTranslator translator = new PortMapAddressTranslator(map);
		map.put("10.0.0.1:3000", "lb.example.com:30009");
		map.put("10.0.0.2:3000", "lb.example.com:30002");

		assertHost("lb.example.com", 30001, null,
			translator.translate(new Endpoint(NODE_A, null, "10.0.0.1", 3000, SourceList.STANDARD)).get());
		assertFalse(translator.translate(new Endpoint(NODE_B, null, "10.0.0.2", 3000, SourceList.STANDARD)).isPresent());
	}

	@Test
	public void emptyMapMissesEverything() {
		PortMapAddressTranslator translator = new PortMapAddressTranslator(Map.of());

		assertFalse(translator.translate(new Endpoint(NODE_A, null, "10.0.0.1", 3000, SourceList.STANDARD)).isPresent());
	}

	@Test
	public void nullMapRejected() {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> new PortMapAddressTranslator(null));

		assertTrue(ae.getMessage(), ae.getMessage().contains("port-map"));
	}

	@Test
	public void invalidEntriesRejectedNamingEntry() {
		String[][] entries = {
			{"10.0.0.1:3000", "lb.example.com"},
			{"10.0.0.1:3000", "lb.example.com:"},
			{"10.0.0.1:3000", "lb.example.com:abc"},
			{"10.0.0.1:3000", "lb.example.com:0"},
			{"10.0.0.1:3000", "lb.example.com:65536"},
			{"10.0.0.1:3000", "lb.example.com:-1"},
			{"10.0.0.1:3000", ":30001"},
			{"10.0.0.1:3000", "2001:db8::1:30001"},
			{"10.0.0.1:3000", "[2001:db8::1:30001"},
			{"10.0.0.1:3000", "[]:30001"},
			{"10.0.0.1:3000", ""},
			{"10.0.0.1:70000", "lb.example.com:30001"},
			{"10.0.0.1:x", "lb.example.com:30001"},
			{"2001:db8::10:3000", "lb.example.com:30001"},
			{":3000", "lb.example.com:30001"},
			{NODE_A, "lb.example.com:99999"},
		};

		for (String[] entry : entries) {
			Map<String,String> map = Map.of(entry[0], entry[1]);
			AerospikeException ae = assertThrows(entry[0] + " " + entry[1], AerospikeException.class,
				() -> new PortMapAddressTranslator(map));

			assertTrue(ae.getMessage(), ae.getMessage().contains("Invalid port-map entry " + entry[0] + ": " + entry[1]));
		}
	}

	@Test
	public void nullValueRejectedNamingEntry() {
		Map<String,String> map = new HashMap<>();
		map.put("10.0.0.1:3000", null);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> new PortMapAddressTranslator(map));

		assertTrue(ae.getMessage(), ae.getMessage().contains("Invalid port-map entry 10.0.0.1:3000"));
	}

	@Test
	public void nullKeyRejected() {
		Map<String,String> map = new HashMap<>();
		map.put(null, "lb.example.com:30001");
		AerospikeException ae = assertThrows(AerospikeException.class, () -> new PortMapAddressTranslator(map));

		assertTrue(ae.getMessage(), ae.getMessage().contains("lb.example.com:30001"));
	}

	@Test
	public void equivalentHostPortKeysRejected() {
		Map<String,String> map = new HashMap<>();
		map.put("10.0.0.1:3000", "lb.example.com:30001");
		map.put("10.0.0.1:03000", "lb.example.com:30002");

		assertThrows(AerospikeException.class, () -> new PortMapAddressTranslator(map));
	}

	@Test
	public void constructAndTranslateDoNoNameLookup() {
		GuardedResolverProvider.arm();

		try {
			PortMapAddressTranslator translator = new PortMapAddressTranslator(
				Map.of("node1.internal:3000", "node1.external.invalid:30001", NODE_B, "node2.external.invalid:30002"));

			assertHost("node1.external.invalid", 30001, "tls1",
				translator.translate(new Endpoint(NODE_A, "tls1", "node1.internal", 3000, SourceList.STANDARD)).get());
			assertHost("node2.external.invalid", 30002, null,
				translator.translate(new Endpoint(NODE_B, null, "node2.internal", 3000, SourceList.ALTERNATE)).get());
			assertFalse(translator.translate(new Endpoint(NODE_A, null, "node3.internal", 3000, SourceList.STANDARD)).isPresent());
		}
		finally {
			GuardedResolverProvider.disarm();
		}
	}

	@Test
	public void portRangeBoundsAccepted() {
		Host host = translate(Map.of("10.0.0.1:65535", "lb.example.com:1"),
			new Endpoint(NODE_A, null, "10.0.0.1", 65535, SourceList.STANDARD));

		assertHost("lb.example.com", 1, null, host);
		assertNull(host.tlsName);
	}
}
