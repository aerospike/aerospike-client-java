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
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.VectorExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.VectorDistanceMetric;
import com.aerospike.test.sync.TestSync;

/**
 * Vector-distance expression tests: filtering records by {@link VectorExp#distance} and a
 * "vector search" style Top-K over a projected distance value.
 */
public class TestVectorExp extends TestSync {
	private static final String vecBin = "embedding";
	private static final String idBin = "id";
	private static final String keyPrefix = "vecexp";
	private static final String setName = "vectorExpSet";
	private static final int size = 20;
	private static final int dims = 4;

	@BeforeClass
	public static void prepare() {
		for (int i = 0; i < size; i++) {
			Key key = new Key(args.namespace, setName, keyPrefix + i);
			float[] data = new float[dims];

			for (int d = 0; d < dims; d++) {
				data[d] = i + d * 0.1f;
			}
			client.put(null, key, new Bin(idBin, i), new Bin(vecBin, Vector.ofFloat32(data)));
		}
	}

	@AfterClass
	public static void destroy() {
		for (int i = 0; i < size; i++) {
			client.delete(null, new Key(args.namespace, setName, keyPrefix + i));
		}
	}

	private static Vector query(int base) {
		float[] data = new float[dims];

		for (int d = 0; d < dims; d++) {
			data[d] = base + d * 0.1f;
		}
		return Vector.ofFloat32(data);
	}

	@Test
	public void filterByEuclideanDistance() {
		assertFilterIds(VectorDistanceMetric.EUCLIDEAN, false, 0.01, 5);
	}

	@Test
	public void filterByDotProductDistance() {
		assertFilterIds(VectorDistanceMetric.DOT_PRODUCT, true, 200.0, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
	}

	@Test
	public void filterByCosineDistance() {
		assertFilterIds(VectorDistanceMetric.COSINE, true, 0.999999, 5);
	}

	private void assertFilterIds(VectorDistanceMetric metric, boolean greaterThan, double threshold, long... expectedIds) {
		QueryPolicy policy = new QueryPolicy();
		Exp distance = VectorExp.distance(metric, query(5), Exp.vectorBin(vecBin));
		policy.filterExp = Exp.build(greaterThan ?
			Exp.gt(distance, Exp.val(threshold)) :
			Exp.lt(distance, Exp.val(threshold)));

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);

		RecordSet rs = client.query(policy, stmt);
		Set<Long> actual = new HashSet<>();

		try {
			while (rs.next()) {
				actual.add(rs.getRecord().getLong(idBin));
			}
		}
		finally {
			rs.close();
		}

		Set<Long> expected = new HashSet<>();

		for (long id : expectedIds) {
			expected.add(id);
		}
		assertEquals(Arrays.toString(expectedIds), expected, actual);
	}

	@Test
	public void vectorSearchTopKNearest() {
		int k = 5;
		String distBin = "dist";

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);

		// Project distance, then keep the k nearest records.
		stmt.setOperations(new Operation[] {
			Operation.get(idBin),
			ExpOperation.read(distBin,
				Exp.build(VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query(0), Exp.vectorBin(vecBin))),
				ExpReadFlags.DEFAULT)
		});
		stmt.setOrderBy(distBin, BinDataType.DOUBLE, Order.ASC);
		stmt.setTopK(k);

		RecordSet rs = client.query(null, stmt);
		int count = 0;
		double previous = Double.NEGATIVE_INFINITY;

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				double dist = record.getDouble(distBin);
				assertTrue("Top-K nearest must be in ascending distance order", dist >= previous);
				assertEquals("Top-K nearest must return records nearest to query(0)", count, record.getLong(idBin));
				previous = dist;
				count++;
			}
		}
		finally {
			rs.close();
		}

		assertEquals(k, count);
	}
}
