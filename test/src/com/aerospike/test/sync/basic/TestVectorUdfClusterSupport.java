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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.query.Statement;
import com.aerospike.client.vector.Vector;
import com.aerospike.test.sync.TestSync;

/**
 * UDF calls whose arguments hold a vector fail before anything is sent when a node lacks
 * vector support. The UDF is never registered, because the client must reject the call first.
 */
public class TestVectorUdfClusterSupport extends TestSync {
	private static final String SET = "vecUdf";
	private static final String PACKAGE = "vector_udf";
	private static final String FUNCTION = "echo";
	private static final Value VECTOR_ARG = Value.get(Vector.ofFloat32(new float[] {1.0f, 2.0f}));

	@Test
	public void singleRecordUdf() {
		assertRejected(() -> client.execute(null, key("k1"), PACKAGE, FUNCTION, VECTOR_ARG));
	}

	@Test
	public void vectorNestedInListArgument() {
		Value list = Value.get(Collections.singletonList(Vector.ofInt32(new int[] {1, 2})));
		assertRejected(() -> client.execute(null, key("k1"), PACKAGE, FUNCTION, list));
	}

	@Test
	public void batchUdfOnKeys() {
		Key[] keys = new Key[] {key("k1"), key("k2")};
		assertRejected(() -> client.execute(null, null, keys, PACKAGE, FUNCTION, VECTOR_ARG));
	}

	@Test
	public void batchUdfRecords() {
		List<BatchRecord> records = new ArrayList<>();
		records.add(new BatchUDF(key("k1"), PACKAGE, FUNCTION, new Value[] {VECTOR_ARG}));
		records.add(new BatchUDF(key("k2"), PACKAGE, FUNCTION, new Value[] {VECTOR_ARG}));
		assertRejected(() -> client.operate(null, records));
	}

	@Test
	public void backgroundQueryUdf() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		assertRejected(() -> client.execute(null, statement, PACKAGE, FUNCTION, VECTOR_ARG));
	}

	private static Key key(String userKey) {
		return new Key(args.namespace, SET, userKey);
	}

	private static void assertRejected(ThrowingRunnable call) {
		Cluster cluster = client.getCluster();
		boolean saved = cluster.hasVector;
		cluster.hasVector = false;

		try {
			AerospikeException ae = assertThrows(AerospikeException.class, call);
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertTrue(ae.getMessage().contains("Vector is not supported by all nodes in the cluster"));
		}
		finally {
			cluster.hasVector = saved;
		}
	}
}
