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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

/**
 * The L5 driver's {@code large} command against a stub store. Needs no server.
 */
public class TestL5DriverLarge {
	private static final int BYTES = 1 << 20;

	@Test
	public void writesAndReadsRecordsOfTheGivenSize() {
		Map<String,String> stored = new LinkedHashMap<>();
		L5Driver.Large large = L5Driver.Large.of("mtu", Integer.toString(BYTES), "3");
		large.run("test", "l5", new L5Driver.Large.Store() {
			@Override
			public void put(Key key, String value) {
				stored.put((String)key.userKey.getObject(), value);
			}

			@Override
			public Object get(Key key) {
				return stored.get((String)key.userKey.getObject());
			}
		});

		assertEquals("[mtu-0, mtu-1, mtu-2]", stored.keySet().toString());

		for (Map.Entry<String,String> entry : stored.entrySet()) {
			assertEquals(BYTES, entry.getValue().length());
			assertTrue(entry.getValue().startsWith(entry.getKey()));
		}
		assertTrue(large.reply(7).matches("\\{\"type\":\"large\",\"t\":7,\"step\":\"mtu\",\"bytes\":1048576,\"keys\":3," +
			"\"written\":3,\"read\":3,\"mismatched\":0,\"failures\":0,\"firstFailure\":null,\"maxMs\":\\d+,\"elapsedMs\":\\d+\\}"));
	}

	@Test
	public void countsMismatchesAndFailures() {
		L5Driver.Large large = L5Driver.Large.of("s", "16", "3");
		large.run("test", "l5", new L5Driver.Large.Store() {
			@Override
			public void put(Key key, String value) {
				if (key.userKey.getObject().equals("s-1")) {
					throw new AerospikeException("put refused");
				}
			}

			@Override
			public Object get(Key key) {
				return key.userKey.getObject().equals("s-0") ? L5Driver.Large.value("s-0", 16) : "other";
			}
		});

		assertTrue(large.reply(9).matches("\\{\"type\":\"large\",\"t\":9,\"step\":\"s\",\"bytes\":16,\"keys\":3," +
			"\"written\":2,\"read\":1,\"mismatched\":1,\"failures\":1,\"firstFailure\":\"s-1 put: .*put refused\"," +
			"\"maxMs\":\\d+,\"elapsedMs\":\\d+\\}"));
	}

	@Test
	public void malformedArgumentsAreRefused() {
		assertNull(L5Driver.Large.of("s", "x", "1"));
		assertNull(L5Driver.Large.of("s", "16", "0"));
		assertNull(L5Driver.Large.of("s", "-1", "1"));
		assertNull(L5Driver.Large.of("step", "5", "10"));
		assertNotNull(L5Driver.Large.of("step", "6", "10"));
		assertEquals(L5Driver.Large.value("step-9", 6), "step-9");
	}
}
