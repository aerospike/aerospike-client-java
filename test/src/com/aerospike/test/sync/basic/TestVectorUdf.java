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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.vector.Vector;
import com.aerospike.test.sync.TestSync;

/**
 * Vectors through UDFs: a record UDF that returns a vector gives the client a vector, and the
 * client-side stage of a stream UDF aggregation receives a vector as bytes, the same bytes the
 * server stores.
 */
public class TestVectorUdf extends TestSync {
	private static final String SET = "vecLua";
	private static final String PACKAGE = "vector_example";
	private static final String MODULE = "udf/vector_example.lua";
	private static final String BIN = "vec";
	private static final Vector STORED = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});

	@BeforeClass
	public static void prepare() {
		Assume.assumeTrue("Vectors require server version 8.2.1 or later",
			args.serverVersion.isGreaterOrEqual(8, 2, 1, 0));

		RegisterTask task = client.register(null, TestVectorUdf.class.getClassLoader(), MODULE,
			"vector_example.lua", Language.LUA);
		task.waitTillComplete();

		client.put(null, key(), new Bin(BIN, STORED));
	}

	@AfterClass
	public static void destroy() {
		client.delete(null, key());
	}

	@Test
	public void recordUdfReturnsVector() {
		Vector[] vectors = {
			Vector.ofFloat16(new short[] {0x3c00, (short)0xc000, 0x7bff}),
			Vector.ofInt32(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}),
			Vector.ofFloat32(new float[] {-Float.MAX_VALUE, Float.MIN_VALUE, 0.5f}),
			Vector.ofFloat64(new double[] {-Double.MAX_VALUE, Double.MIN_VALUE, 0.25})
		};

		for (Vector vector : vectors) {
			assertEquals(vector, client.execute(null, key(), PACKAGE, "echo", Value.get(vector)));
		}
	}

	@Test
	public void aggregationReceivesStoredVectorAsBytes() {
		Statement statement = statement();
		statement.setAggregateFunction(TestVectorUdf.class.getClassLoader(), MODULE, PACKAGE,
			"describe_bin", Value.get(BIN));

		assertDescribes(STORED, aggregate(statement));
	}

	@Test
	public void aggregationReceivesVectorArgumentAsBytes() {
		Vector argument = Vector.ofInt32(new int[] {7, 8, 9});
		Statement statement = statement();
		statement.setAggregateFunction(TestVectorUdf.class.getClassLoader(), MODULE, PACKAGE,
			"describe_arg", Value.get(argument));

		assertDescribes(argument, aggregate(statement));
	}

	private static void assertDescribes(Vector expected, Object result) {
		Map<?,?> description = (Map<?,?>)result;
		byte[] wire = expected.getWireBytes();

		assertEquals((long)wire.length, description.get("size"));
		assertEquals((long)ParticleType.VECTOR, description.get("type"));
		assertArrayEquals(wire, (byte[])description.get("value"));
	}

	private static Object aggregate(Statement statement) {
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

	private static Statement statement() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		return statement;
	}

	private static Key key() {
		return new Key(args.namespace, SET, "k1");
	}
}
