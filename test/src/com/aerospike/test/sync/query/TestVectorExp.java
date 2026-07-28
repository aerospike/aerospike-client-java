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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
 * <p>
 * All tests are {@link Ignore}d until a server build that supports the vector distance expression
 * is available. NOTE: the query-vector wire envelope (headerless elements vs full vector value) and
 * the exact metric semantics (e.g. cosine similarity vs cosine distance, which flips sort direction)
 * are NOT finalized upstream. When enabling these tests, revisit every distance threshold and
 * Top-K sort direction below against the shipped server contract.
 */
@Ignore("Requires server build with vector distance expression support; semantics not finalized")
public class TestVectorExp extends TestSync {
	private static final String vecBin = "embedding";
	private static final String idBin = "id";
	private static final String keyPrefix = "vecexp";
	private static final int size = 20;
	private static final int dims = 4;

	@BeforeClass
	public static void prepare() {
		for (int i = 0; i < size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
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
			client.delete(null, new Key(args.namespace, args.set, keyPrefix + i));
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
		assertFilterReturnsSome(VectorDistanceMetric.EUCLIDEAN);
	}

	@Test
	public void filterByDotProductDistance() {
		assertFilterReturnsSome(VectorDistanceMetric.DOT_PRODUCT);
	}

	@Test
	public void filterByCosineDistance() {
		assertFilterReturnsSome(VectorDistanceMetric.COSINE);
	}

	private void assertFilterReturnsSome(VectorDistanceMetric metric) {
		QueryPolicy policy = new QueryPolicy();
		// Records whose embedding is within a distance threshold of the query vector.
		policy.filterExp = Exp.build(
			Exp.lt(
				VectorExp.distance(metric, query(5), Exp.vectorBin(vecBin)),
				Exp.val(1000.0)));

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);

		RecordSet rs = client.query(policy, stmt);
		int count = 0;

		try {
			while (rs.next()) {
				count++;
			}
		}
		finally {
			rs.close();
		}

		assertTrue("Expected at least one record within distance threshold", count > 0);
	}

	@Test
	public void vectorSearchTopKNearest() {
		int k = 5;
		String distBin = "dist";

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);

		// Project a per-record distance to the query vector, then keep the k nearest (smallest).
		stmt.setOperations(new Operation[] {
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
