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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.ResultCode;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.StaticMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;

public class DiscoveryResultCodeTest {
	@Test
	public void strictMissIsTranslationMiss() {
		Endpoint advertised = new Endpoint("BB9A", null, "10.0.0.7", 3000, Endpoint.SourceList.ALTERNATE);
		StaticMapAddressTranslator translator = new StaticMapAddressTranslator(Map.of("10.0.0.1", "127.0.0.1"));

		AerospikeException.TranslationMiss tm = assertThrows(AerospikeException.TranslationMiss.class,
			() -> Cluster.translate(translator, TranslationMissMode.STRICT, advertised));

		assertEquals(ResultCode.TRANSLATION_MISS, tm.getResultCode());
		assertEquals(-25, tm.getResultCode());
		assertFalse(tm.getInDoubt());

		for (String part : new String[] {"BB9A", "10.0.0.7:3000", "alternate", "static-map"}) {
			assertTrue(tm.getMessage(), tm.getMessage().contains(part));
		}
	}

	@Test
	public void collisionIsTranslationCollision() {
		AerospikeException.TranslationCollision tc = new AerospikeException.TranslationCollision(
			"BB9B", "BB9C", new Host("127.0.0.1", 3000), "port-map");

		assertEquals(-26, tc.getResultCode());

		for (String part : new String[] {"BB9B", "BB9C", "127.0.0.1:3000", "port-map"}) {
			assertTrue(tc.getMessage(), tc.getMessage().contains(part));
		}
	}

	@Test
	public void alternateAddressMissingIsClientError() {
		AerospikeException.AlternateAddressMissing e = new AerospikeException.AlternateAddressMissing("BB9A", null);

		assertEquals(ResultCode.CLIENT_ERROR, e.getResultCode());
	}
}
