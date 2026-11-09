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
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.Endpoint.SourceList;

public class PassThroughAddressTranslatorTest {
	private static void assertUnchanged(String host, String tlsName, int port) {
		for (SourceList sourceList : SourceList.values()) {
			Optional<Host> result = PassThroughAddressTranslator.INSTANCE.translate(
				new Endpoint("BB9", tlsName, host, port, sourceList));

			assertTrue(result.isPresent());
			assertEquals(host, result.get().name);
			assertEquals(port, result.get().port);
			assertEquals(tlsName, result.get().tlsName);
		}
	}

	@Test
	public void ipv4Unchanged() {
		assertUnchanged("10.0.0.1", "db1.example.com", 3000);
	}

	@Test
	public void ipv6LiteralUnchanged() {
		assertUnchanged("2001:db8::10", "db1.example.com", 3000);
	}

	@Test
	public void hostNameUnchanged() {
		assertUnchanged("node2.priv.example.com", "db2.example.com", 3000);
	}

	@Test
	public void sharedHostPortPerNodeUnchanged() {
		assertUnchanged("as.priv.example.com", "db1.example.com", 31001);
		assertUnchanged("as.priv.example.com", "db2.example.com", 31002);
	}

	@Test
	public void tlsNameDifferentFromHostKept() {
		assertUnchanged("node1.priv.example.com", "cluster.example.com", 4333);
	}

	@Test
	public void nullTlsNameKept() {
		assertUnchanged("10.0.0.1", null, 3000);
		assertUnchanged("2001:db8::10", null, 3000);
		assertUnchanged("node1.priv.example.com", null, 3000);

		Host host = new PassThroughAddressTranslator().translate(
			new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.ALTERNATE)).get();
		assertNull(host.tlsName);
	}
}
