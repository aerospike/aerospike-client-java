/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class QueryRegionFilter extends Example {

	/**
	 * Perform region query using a Geo index with an aggregation filter.
	 */
	@Override
	public void runExample() throws Exception {
		String indexName = "filterindexloc";
		String keyPrefix = "filterkeyloc";
		String binName1 = "filterloc";
		String binName2 = "filteramenity";
		int size = 20;

		register();
		createIndex(indexName, binName1);
		writeRecords(keyPrefix, binName1, binName2, size);
		runQuery(indexName, binName1, binName2);
	}

	private void register() throws Exception {
		RegisterTask task = client().register(readPolicy(), "udf/geo_filter_example.lua",
											"geo_filter_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	private void createIndex(String indexName, String binName) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			namespace(), set(), indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client().createIndex(policy, namespace(), set(),
												indexName, binName,
												IndexType.GEO2DSPHERE);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(String keyPrefix, String binName1, String binName2, int size) throws Exception {
		console.info("Write " + size + " records.");

		for (int i = 0; i < size; i++) {
			double lng = -122 + (0.1 * i);
			double lat = 37.5 + (0.1 * i);
			StringBuilder ptsb = new StringBuilder();
			ptsb.append("{ \"type\": \"Point\", \"coordinates\": [");
			ptsb.append(String.valueOf(lng));
			ptsb.append(", ");
			ptsb.append(String.valueOf(lat));
			ptsb.append("] }");
			Key key = new Key(namespace(), set(), keyPrefix + i);
			Bin bin1 = Bin.asGeoJSON(binName1, ptsb.toString());
			Bin bin2;
			if (i % 7 == 0) {
				bin2 = new Bin(binName2, "hospital");
			}
			else if (i % 2 == 0) {
				bin2 = new Bin(binName2, "school");
			}
			else {
				bin2 = new Bin(binName2, "store");
			}
			client().put(writePolicy(), key, bin1, bin2);
		}
	}

	private void runQuery(String indexName, String binName1, String binName2) throws Exception {

		StringBuilder rgnsb = new StringBuilder();

		rgnsb.append("{ ");
		rgnsb.append("    \"type\": \"Polygon\", ");
		rgnsb.append("    \"coordinates\": [ ");
		rgnsb.append("        [[-122.500000, 37.000000],[-121.000000, 37.000000], ");
		rgnsb.append("         [-121.000000, 38.080000],[-122.500000, 38.080000], ");
		rgnsb.append("         [-122.500000, 37.000000]] ");
		rgnsb.append("    ] ");
		rgnsb.append(" } ");

		console.info("Query for: ns=%s set=%s index=%s bin1=%s bin2=%s within %s",
					 namespace(), set(), indexName, binName1, binName2, rgnsb);

		String amenStr = "school";

		Statement stmt = new Statement();
		stmt.setNamespace(namespace());
		stmt.setSetName(set());
		stmt.setFilter(Filter.geoWithinRegion(binName1, rgnsb.toString()));
		stmt.setAggregateFunction("geo_filter_example", "match_amenity", Value.get(amenStr));

		try (ResultSet rs = client().queryAggregate(null, stmt)) {
			int count = 0;

			while (rs.next()) {
				Object result = rs.getObject();
				console.info("Record found: value2=%s", result);
				count++;
			}

			if (count != 2) {
				console.error("wrong number of schools found. %d != 2", count);
			}
		}
	}
}
