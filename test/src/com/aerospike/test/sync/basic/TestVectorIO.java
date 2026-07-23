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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.vector.Vector;
import com.aerospike.test.sync.TestSync;

/**
 * Server-round-trip tests for the vector particle type: writing and reading vector bins, and
 * vectors nested in list/map bins.
 * <p>
 * All tests are {@link Ignore}d until a server build that supports the vector particle type is
 * available. They are kept in {@code SuiteSync} so they compile in CI but are skipped at runtime.
 */
@Ignore("Requires server build with vector particle support")
public class TestVectorIO extends TestSync {
	private static final String binName = "vecbin";

	@Test
	public void putGetFloat16() {
		putGetRoundTrip("veckey16", Vector.ofFloat16(new short[] {0x3c00, (short)0xbc00, 0x4000}));
	}

	@Test
	public void putGetInt32() {
		putGetRoundTrip("veckey32i", Vector.ofInt32(new int[] {-5, 0, 7, 12345}));
	}

	@Test
	public void putGetFloat32() {
		putGetRoundTrip("veckey32f", Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.14159f}));
	}

	@Test
	public void putGetFloat64() {
		putGetRoundTrip("veckey64", Vector.ofFloat64(new double[] {1.5, -2.25, 3.14159}));
	}

	private void putGetRoundTrip(String userKey, Vector v) {
		Key key = new Key(args.namespace, args.set, userKey);
		client.delete(null, key);
		client.put(null, key, new Bin(binName, v));

		Record record = client.get(null, key);
		assertRecordFound(key, record);
		assertEquals(v, record.getVector(binName));
	}

	@Test
	public void overwriteWithDifferentDimensions() {
		Key key = new Key(args.namespace, args.set, "vecoverwrite");
		client.delete(null, key);

		client.put(null, key, new Bin(binName, Vector.ofFloat32(new float[] {1.0f, 2.0f})));
		Vector second = Vector.ofFloat32(new float[] {9.0f, 8.0f, 7.0f, 6.0f});
		client.put(null, key, new Bin(binName, second));

		Record record = client.get(null, key);
		assertRecordFound(key, record);
		assertEquals(second, record.getVector(binName));
	}

	@Test
	public void vectorInListBin() {
		Key key = new Key(args.namespace, args.set, "veclistbin");
		client.delete(null, key);

		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		client.put(null, key, new Bin("listbin", Collections.singletonList(v)));

		Record record = client.get(null, key);
		assertRecordFound(key, record);

		List<?> list = record.getList("listbin");
		assertEquals(1, list.size());
		assertEquals(v, list.get(0));
	}

	@Test
	public void vectorInMapBin() {
		Key key = new Key(args.namespace, args.set, "vecmapbin");
		client.delete(null, key);

		Vector v = Vector.ofFloat32(new float[] {1.5f, 2.5f});
		client.put(null, key, new Bin("mapbin", Collections.singletonMap("k", v)));

		Record record = client.get(null, key);
		assertRecordFound(key, record);

		Map<?, ?> map = record.getMap("mapbin");
		assertEquals(v, map.get("k"));
	}

	@Test
	public void operateVectorRoundTrip() {
		Key key = new Key(args.namespace, args.set, "vecoperate");
		client.delete(null, key);

		Vector v = Vector.ofFloat64(new double[] {1.1, 2.2, 3.3});
		Record record = client.operate(null, key,
			Operation.put(new Bin(binName, v)),
			Operation.get(binName));

		assertRecordFound(key, record);
		assertEquals(v, record.getVector(binName));
	}

	@Test
	public void batchWriteReadVectors() {
		int count = 5;
		Key[] keys = new Key[count];
		Vector[] vectors = new Vector[count];

		for (int i = 0; i < count; i++) {
			keys[i] = new Key(args.namespace, args.set, "vecbatch" + i);
			client.delete(null, keys[i]);
			vectors[i] = Vector.ofFloat32(new float[] {i, i + 0.5f, i + 1.0f});
			client.put(null, keys[i], new Bin(binName, vectors[i]));
		}

		Record[] records = client.get(null, keys);
		assertEquals(count, records.length);

		for (int i = 0; i < count; i++) {
			assertRecordFound(keys[i], records[i]);
			assertEquals(vectors[i], records[i].getVector(binName));
		}
	}

	@Test
	public void writeAllElementTypesPreservesBytes() {
		Vector v = Vector.ofInt32(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE});
		Key key = new Key(args.namespace, args.set, "vecbytes");
		client.delete(null, key);
		client.put(null, key, new Bin(binName, v));

		Record record = client.get(null, key);
		assertRecordFound(key, record);
		assertArrayEquals(v.getInt32Data(), record.getVector(binName).getInt32Data());
	}
}
