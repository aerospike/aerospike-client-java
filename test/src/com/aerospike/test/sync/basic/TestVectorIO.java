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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.vector.Vector;
import com.aerospike.test.sync.TestSync;

/**
 * Server-round-trip tests for the vector particle type: writing and reading vector bins, the
 * server's refusal of a vector nested in a list or map bin, and vectors through UDFs.
 */
public class TestVectorIO extends TestSync {
	private static final String binName = "vecbin";
	private static final String udfSet = "vecStream";
	private static final String udfPackage = "vector_example";
	private static final String udfModule = "udf/vector_example.lua";

	@BeforeClass
	public static void prepare() {
		Assume.assumeTrue("Vectors require server version 8.2.1 or later",
			args.serverVersion.isGreaterOrEqual(8, 2, 1, 0));

		RegisterTask task = client.register(null, TestVectorIO.class.getClassLoader(), udfModule,
			"vector_example.lua", Language.LUA);
		task.waitTillComplete();
	}

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
	public void vectorInListBinRejectedByServer() {
		Key key = new Key(args.namespace, args.set, "veclistbin");
		client.delete(null, key);

		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		assertParameterError(() -> client.put(null, key, new Bin("listbin", Collections.singletonList(v))));
		assertNull(client.get(null, key));
	}

	@Test
	public void vectorInMapBinRejectedByServer() {
		Key key = new Key(args.namespace, args.set, "vecmapbin");
		client.delete(null, key);

		Vector v = Vector.ofFloat32(new float[] {1.5f, 2.5f});
		assertParameterError(() -> client.put(null, key, new Bin("mapbin", Collections.singletonMap("k", v))));
		assertNull(client.get(null, key));
	}

	private static void assertParameterError(ThrowingRunnable action) {
		AerospikeException e = assertThrows(AerospikeException.class, action);
		assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
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
	public void batchOperateWritesVectors() {
		int count = 5;
		Key[] keys = new Key[count];
		Vector[] vectors = new Vector[count];
		List<BatchRecord> writes = new ArrayList<>();

		for (int i = 0; i < count; i++) {
			keys[i] = new Key(args.namespace, args.set, "vecbatchwrite" + i);
			client.delete(null, keys[i]);
			vectors[i] = Vector.ofFloat64(new double[] {i, i * 0.5, -i});
			writes.add(new BatchWrite(keys[i], Operation.array(Operation.put(new Bin(binName, vectors[i])))));
		}

		BatchPolicy policy = new BatchPolicy();
		assertTrue(client.operate(policy, writes));

		for (BatchRecord write : writes) {
			assertEquals(ResultCode.OK, write.resultCode);
		}

		Record[] records = client.get(policy, keys);

		for (int i = 0; i < count; i++) {
			assertRecordFound(keys[i], records[i]);
			assertEquals(vectors[i], records[i].getVector(binName));
		}
	}

	@Test
	public void oversizedVectorRejectedByServer() {
		Key key = new Key(args.namespace, args.set, "vecoversized");
		client.delete(null, key);

		// One element over the 32-bit float ceiling of 65,536 dimensions.
		Vector v = Vector.ofFloat32(new float[65537]);
		assertParameterError(() -> client.put(null, key, new Bin(binName, v)));
		assertNull(client.get(null, key));
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

	@Test
	public void recordUdfReturnsVector() {
		Key key = new Key(args.namespace, udfSet, "vececho");
		Vector[] vectors = {
			Vector.ofFloat16(new short[] {0x3c00, (short)0xc000, 0x7bff}),
			Vector.ofInt32(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}),
			Vector.ofFloat32(new float[] {-Float.MAX_VALUE, Float.MIN_VALUE, 0.5f}),
			Vector.ofFloat64(new double[] {-Double.MAX_VALUE, Double.MIN_VALUE, 0.25})
		};

		for (Vector v : vectors) {
			assertEquals(v, client.execute(null, key, udfPackage, "echo", Value.get(v)));
		}
	}

	@Test
	public void aggregationReceivesStoredVectorAsBytes() {
		Vector stored = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
		assertDescribes(stored, aggregate(stored, "describe_bin", Value.get(binName)));
	}

	@Test
	public void aggregationReceivesVectorArgumentAsBytes() {
		Vector argument = Vector.ofInt32(new int[] {7, 8, 9});
		assertDescribes(argument, aggregate(argument, "describe_arg", Value.get(argument)));
	}

	private static Object aggregate(Vector stored, String function, Value arg) {
		// The set holds only this record, so the aggregation yields exactly one result.
		client.put(null, new Key(args.namespace, udfSet, "vecstream"), new Bin(binName, stored));

		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(udfSet);
		statement.setAggregateFunction(TestVectorIO.class.getClassLoader(), udfModule, udfPackage,
			function, arg);

		ResultSet rs = client.queryAggregate(null, statement);

		try {
			assertTrue("Expected an aggregation result", rs.next());
			Object result = rs.getObject();
			assertFalse("Expected exactly one aggregation result", rs.next());
			return result;
		}
		finally {
			rs.close();
		}
	}

	private static void assertDescribes(Vector expected, Object result) {
		Map<?,?> description = (Map<?,?>)result;
		byte[] wire = expected.getWireBytes();

		assertEquals((long)wire.length, description.get("size"));
		assertEquals((long)ParticleType.VECTOR, description.get("type"));
		assertArrayEquals(wire, (byte[])description.get("value"));
	}
}
