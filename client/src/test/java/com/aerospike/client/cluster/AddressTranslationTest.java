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

import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.Endpoint.SourceList;

public class AddressTranslationTest {
	@Test
	public void customTranslatorCannotChangeTlsName() {
		AddressTranslator translator = advertised -> new Host("192.168.1.1", "rewritten", 4000);
		Host host = Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals(4000, host.port);
		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void customTranslatorCannotAddTlsName() {
		AddressTranslator translator = advertised -> new Host("192.168.1.1", "rewritten", advertised.port);
		Host host = Cluster.translate(translator, new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.STANDARD));

		assertNull(host.tlsName);
	}

	@Test
	public void customTranslatorCannotDropTlsName() {
		AddressTranslator translator = advertised -> new Host("192.168.1.1", advertised.port);
		Host host = Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("tls1", host.tlsName);
	}

	@Test
	public void endpointCarriesAdvertisedFields() {
		Endpoint[] seen = new Endpoint[1];
		AddressTranslator translator = advertised -> {
			seen[0] = advertised;
			return new Host(advertised.host, advertised.tlsName, advertised.port);
		};
		Cluster.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.ALTERNATE));

		assertEquals("BB9", seen[0].nodeName);
		assertEquals("tls1", seen[0].tlsName);
		assertEquals("10.0.0.1", seen[0].host);
		assertEquals(3000, seen[0].port);
		assertEquals(SourceList.ALTERNATE, seen[0].sourceList);
	}
}
