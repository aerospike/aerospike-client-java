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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.VectorExp;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.VectorDistanceMetric;

/**
 * Demonstrate a hybrid vector query: a secondary index filter selects the candidates, a vector
 * distance projection scores them, and {@link Statement#setOrderBy} + {@link Statement#setTopK}
 * keep the nearest.
 */
public class QueryVectorTopK extends Example {

	/**
	 * Write products with a category and an embedding, then find the shoes nearest to a query
	 * vector.
	 */
	@Override
	public void runExample() throws Exception {
		String indexName = "vectopkindex";
		createIndex(indexName, "category");

		for (int i = 0; i < 10; i++) {
			client().put(writePolicy(), new Key(namespace(), set(), "product" + i), new Bin("id", i),
				new Bin("category", i % 2 == 0 ? "shoes" : "hats"), new Bin("embedding", Vector.ofFloat32(new float[] {i, i * 0.5f, 1.0f})));
		}

		Exp distance = VectorExp.distance(VectorDistanceMetric.EUCLIDEAN_SQUARED,
			Vector.ofFloat32(new float[] {6.5f, 3.25f, 1.0f}), Exp.vectorBin("embedding"));
		Statement stmt = new Statement();
		stmt.setNamespace(namespace());
		stmt.setSetName(set());
		stmt.setFilter(Filter.equal("category", "shoes"));
		stmt.setOperations(new Operation[] {Operation.get("id"),
			ExpOperation.read("distance", Exp.build(distance), ExpReadFlags.DEFAULT)});
		stmt.setOrderBy("distance", BinDataType.DOUBLE, Order.ASC);
		stmt.setTopK(3);

		List<Long> ids = new ArrayList<>();

		try (RecordSet rs = client().query(null, stmt)) {
			while (rs.next()) {
				console.info("Nearest shoe: id=%d distance=%.4f", rs.getRecord().getLong("id"), rs.getRecord().getDouble("distance"));
				ids.add(rs.getRecord().getLong("id"));
			}
		}

		// Shoes are the even ids; the three nearest to the query vector are 6, 8 and 4.
		if (! ids.equals(Arrays.asList(6L, 8L, 4L))) {
			console.error("Top-K mismatch. Expected [6, 8, 4]. Received %s.", ids);
		}
		client().dropIndex(readPolicy(), namespace(), set(), indexName);
	}

	private void createIndex(String indexName, String binName) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			namespace(), set(), indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client().createIndex(policy, namespace(), set(), indexName, binName, IndexType.STRING);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}
}
