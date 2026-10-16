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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.Endpoint.SourceList;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

public class AddressTranslationTest {
	private static final Endpoint MISSED = new Endpoint("BB9", "tls1", "10.0.0.9", 3000, SourceList.STANDARD);
	private static final AddressTranslator STATIC_MAP = new StaticMapAddressTranslator(Map.of("10.0.0.1", "192.168.1.1"));

	static final class EmptyTranslator implements AddressTranslator {
		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}
	}
	@Test
	public void customTranslatorCannotChangeTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", "rewritten", 4000));
		Host host = Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals(4000, host.port);
		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void customTranslatorCannotAddTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", "rewritten", advertised.port));
		Host host = Cluster.translate(translator, new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.STANDARD));

		assertNull(host.tlsName);
	}

	@Test
	public void customTranslatorCannotDropTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", advertised.port));
		Host host = Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void endpointCarriesAdvertisedFields() {
		Endpoint[] seen = new Endpoint[1];
		AddressTranslator translator = advertised -> {
			seen[0] = advertised;
			return Optional.of(new Host(advertised.host, advertised.tlsName, advertised.port));
		};
		Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.ALTERNATE));

		assertEquals("BB9", seen[0].nodeName);
		assertEquals("tls1", seen[0].tlsName);
		assertEquals("10.0.0.1", seen[0].host);
		assertEquals(3000, seen[0].port);
		assertEquals(SourceList.ALTERNATE, seen[0].sourceList);
	}

	@Test
	public void passThroughMissReturnsAdvertised() {
		Host[] hosts = new Host[] {
			Cluster.translate(STATIC_MAP, TranslationMissMode.PASS_THROUGH, MISSED),
			Cluster.translate(new EmptyTranslator(), TranslationMissMode.PASS_THROUGH, MISSED),
			Cluster.translate(new StaticMapAddressTranslator(null), MISSED)
		};

		for (Host h : hosts) {
			assertEquals("10.0.0.9", h.name);
			assertEquals(3000, h.port);
			assertEquals("tls1", h.tlsName);
		}

		Host host = Cluster.translate(STATIC_MAP, TranslationMissMode.PASS_THROUGH,
			new Endpoint("BB9", null, "10.0.0.9", 3000, SourceList.STANDARD));
		assertEquals("10.0.0.9", host.name);
		assertNull(host.tlsName);
	}

	@Test
	public void strictHitTranslates() {
		Host host = Cluster.translate(STATIC_MAP, TranslationMissMode.STRICT,
			new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals(new Host("192.168.1.1", "tls1", 3000), host);
	}

	@Test
	public void strictHitEqualToAdvertisedIsHit() {
		AddressTranslator translator = new StaticMapAddressTranslator(Map.of("10.0.0.9", "10.0.0.9"));

		assertEquals(new Host("10.0.0.9", "tls1", 3000), Cluster.translate(translator, TranslationMissMode.STRICT, MISSED));
	}

	@Test
	public void strictMissFailsNamingStaticMap() {
		AerospikeException.TranslationMiss tm = assertThrows(AerospikeException.TranslationMiss.class,
			() -> Cluster.translate(STATIC_MAP, TranslationMissMode.STRICT, MISSED));
		String message = tm.getMessage();

		for (String part : new String[] {"BB9", "10.0.0.9:3000", "standard", "static-map", "translation.on_miss", "pass_through"}) {
			assertTrue(message, message.contains(part));
		}
	}

	@Test
	public void strictMissFailsNamingCustomTranslator() {
		AerospikeException.TranslationMiss tm = assertThrows(AerospikeException.TranslationMiss.class,
			() -> Cluster.translate(new EmptyTranslator(), TranslationMissMode.STRICT,
				new Endpoint("BB9", null, "10.0.0.9", 3000, SourceList.ALTERNATE)));
		String message = tm.getMessage();

		assertTrue(message, message.contains(EmptyTranslator.class.getName()));
		assertTrue(message, message.contains("alternate"));
	}

	@Test
	public void nullResultFailsNamingTranslator() {
		AddressTranslator translator = new AddressTranslator() {
			@Override
			public Optional<Host> translate(Endpoint advertised) {
				return null;
			}
		};

		for (TranslationMissMode mode : TranslationMissMode.values()) {
			AerospikeException ae = assertThrows(AerospikeException.class, () -> Cluster.translate(translator, mode, MISSED));

			assertFalse(ae instanceof AerospikeException.TranslationMiss);
			assertTrue(ae.getMessage(), ae.getMessage().contains(translator.getClass().getName()));
		}
	}

	@Test
	public void translatorExceptionPropagates() {
		IllegalStateException ise = new IllegalStateException("broken");
		AddressTranslator translator = advertised -> {
			throw ise;
		};

		assertSame(ise, assertThrows(IllegalStateException.class,
			() -> Cluster.translate(translator, TranslationMissMode.STRICT, MISSED)));
	}

	@Test
	public void policyDefaultSetterAndCopy() {
		ClientPolicy policy = new ClientPolicy();
		assertEquals(TranslationMissMode.PASS_THROUGH, policy.translationOnMiss);

		policy.setTranslationOnMiss(TranslationMissMode.STRICT);
		assertEquals(TranslationMissMode.STRICT, new ClientPolicy(policy).translationOnMiss);
	}

	@Test
	public void clusterAppliesOnMiss() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.ipMap = Map.of("10.0.0.1", "192.168.1.1");

		for (TranslationMissMode mode : new TranslationMissMode[] {null, TranslationMissMode.PASS_THROUGH}) {
			policy.translationOnMiss = mode;

			try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
				Cluster cluster = client.getCluster();
				assertEquals(new Host("10.0.0.9", "tls1", 3000), cluster.translateAddress("BB9", "tls1", "10.0.0.9", 3000));
				assertEquals(new Host("192.168.1.1", "tls1", 3000), cluster.translateAddress("BB9", "tls1", "10.0.0.1", 3000));
			}
		}

		policy.translationOnMiss = TranslationMissMode.STRICT;

		try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
			Cluster cluster = client.getCluster();
			assertThrows(AerospikeException.TranslationMiss.class, () -> cluster.translateAddress("BB9", "tls1", "10.0.0.9", 3000));
			assertEquals(new Host("192.168.1.1", "tls1", 3000), cluster.translateAddress("BB9", "tls1", "10.0.0.1", 3000));
		}
	}
}
