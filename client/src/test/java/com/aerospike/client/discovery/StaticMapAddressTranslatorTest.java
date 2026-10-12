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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.Endpoint.SourceList;

public class StaticMapAddressTranslatorTest {
	private static Map<String,String> ipMap() {
		Map<String,String> map = new HashMap<String,String>();
		map.put("10.0.0.1", "192.168.1.1");
		return map;
	}

	@Test
	public void hitReplacesHostName() {
		Host host = new StaticMapAddressTranslator(ipMap()).translate(
			new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals(3000, host.port);
		assertNull(host.tlsName);
	}

	@Test
	public void missReturnsAdvertised() {
		Host host = new StaticMapAddressTranslator(ipMap()).translate(
			new Endpoint("BB9", "tls1", "10.0.0.2", 3000, SourceList.STANDARD));

		assertEquals(new Host("10.0.0.2", "tls1", 3000), host);
		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void nullMapReturnsAdvertised() {
		Host host = new StaticMapAddressTranslator(null).translate(
			new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.ALTERNATE));

		assertEquals("10.0.0.1", host.name);
		assertEquals("tls1", host.tlsName);
		assertEquals(3000, host.port);
	}

	@Test
	public void hitPreservesPort() {
		Host host = new StaticMapAddressTranslator(ipMap()).translate(
			new Endpoint("BB9", null, "10.0.0.1", 4333, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals(4333, host.port);
	}

	@Test
	public void hitPreservesTlsName() {
		Host host = new StaticMapAddressTranslator(ipMap()).translate(
			new Endpoint("BB9", "tls1", "10.0.0.1", 4333, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void mapIsReferencedNotCopied() {
		Map<String,String> map = new HashMap<String,String>();
		StaticMapAddressTranslator translator = new StaticMapAddressTranslator(map);
		map.put("10.0.0.1", "192.168.1.1");

		Host host = translator.translate(new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.STANDARD));
		assertSame(map.get("10.0.0.1"), host.name);
	}
}
