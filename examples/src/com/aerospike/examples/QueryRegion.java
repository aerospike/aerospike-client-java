/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryRegion extends Example {

	public QueryRegion(Console console) {
		super(console);
	}

	/**
	 * Perform region/radius queries using a Geo index.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasGeo) {
			console.info("Geospatial functions are not supported by the connected Aerospike server.");
			return;
		}

		String indexName = "queryindexloc";
		String keyPrefix = "querykeyloc";
		String binName = params.getBinName("querybinloc");
		int size = 20;

		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName, size);
		runQuery(client, params, indexName, binName);
		runRadiusQuery(client, params, indexName, binName);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
	}

	private void createIndex(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set,
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

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) throws Exception {
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
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = Bin.asGeoJSON(binName, ptsb.toString());
			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {

		StringBuilder rgnsb = new StringBuilder();

		rgnsb.append("{ ");
		rgnsb.append("    \"type\": \"Polygon\", ");
		rgnsb.append("    \"coordinates\": [ ");
		rgnsb.append("        [[-122.500000, 37.000000],[-121.000000, 37.000000], ");
		rgnsb.append("         [-121.000000, 38.080000],[-122.500000, 38.080000], ");
		rgnsb.append("         [-122.500000, 37.000000]] ");
		rgnsb.append("    ] ");
		rgnsb.append(" } ");

		console.info("QueryRegion for: ns=%s set=%s index=%s bin=%s within %s",
			params.namespace, params.set, indexName, binName, rgnsb);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.geoWithinRegion(binName, rgnsb.toString()));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				String result = record.getGeoJSON(binName);

				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

				count++;
			}

			if (count != 6) {
				console.error("Query count mismatch. Expected 6. Received " + count);
			}
		}
		finally {
			rs.close();
		}
	}

	private void runRadiusQuery(
			AerospikeClient client,
			Parameters params,
			String indexName,
			String binName
		) throws Exception {

		double lon= -122.0;
		double lat= 37.5;
		double radius=50000.0;
		console.info("QueryRadius for: ns=%s set=%s index=%s bin=%s within long=%f lat=%f radius=%f",
			params.namespace, params.set, indexName, binName, lon,lat,radius);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.geoWithinRadius(binName, lon, lat, radius));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				String result = record.getGeoJSON(binName);

				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

				count++;
			}

			if (count != 4) {
				console.error("Query count mismatch. Expected 4. Received " + count);
			}
		}
		finally {
			rs.close();
		}
	}
}
