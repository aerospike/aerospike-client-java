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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryGeoCollection extends Example {

	public QueryGeoCollection(Console console) {
		super(console);
	}

	/**
	 * Perform region queries using a Geo index on a collection.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {

		runMapExample(client,params);
		runMapKeyExample(client,params);
		runListExample(client,params);
	}

	private void runMapExample(AerospikeClient client, Parameters params) throws Exception {
		String indexName = "geo_map";
		String keyPrefix = "map";
		String mapValuePrefix = "mv";
		String binName = "geo_map_bin";
		String binName2 = "geo_uniq_bin";
		int size = 1000;

		// create collection index on mapValue
		createIndex(client, params, IndexCollectionType.MAPVALUES, indexName, binName);
		writeMapRecords(client, params, keyPrefix, binName, binName2, mapValuePrefix, size);
		runQuery(client, params, binName, binName2, IndexCollectionType.MAPVALUES);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
		deleteRecords(client,params, keyPrefix, size);
	}

	private void runMapKeyExample(AerospikeClient client, Parameters params) throws Exception {
		String indexName = "geo_mapkey";
		String keyPrefix = "mapkey";
		String mapValuePrefix = "mk";
		String binName = "geo_mkey_bin";
		String binName2 = "geo_uniq_bin";
		int size = 1000;

		// create collection index on mapKey
		createIndex(client, params, IndexCollectionType.MAPKEYS, indexName, binName);
		writeMapKeyRecords(client, params, keyPrefix, binName, binName2, mapValuePrefix, size);
		runQuery(client, params, binName, binName2, IndexCollectionType.MAPKEYS);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
		deleteRecords(client,params, keyPrefix, size);
	}

	private void runListExample(AerospikeClient client, Parameters params) throws Exception {
		String indexName = "geo_list";
		String keyPrefix = "list";
		String binName = "geo_list_bin";
		String binName2 = "geo_uniq_bin";
		int size = 1000;

		// create collection index on list
		createIndex(client, params, IndexCollectionType.LIST, indexName, binName);
		writeListRecords(client, params, keyPrefix, binName, binName2, size);
		runQuery(client, params, binName, binName2, IndexCollectionType.LIST);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
		deleteRecords(client,params, keyPrefix, size);
	}

	private void createIndex(
		AerospikeClient client,
		Parameters params,
		IndexCollectionType indexType,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create GeoJSON %s index: ns=%s set=%s index=%s bin=%s",
			indexType, params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set,
					indexName, binName, IndexType.GEO2DSPHERE, indexType);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeMapRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String binName2,
		String valuePrefix,
		int size
	) throws Exception {
		for (int i = 0; i < size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			HashMap<String, Value> map = new HashMap<String,Value>();

			for (int jj = 0; jj < 10; ++jj) {

				double plat = 0.0 + (0.01 * i);
				double plng = 0.0 + (0.10 * jj);
				String geoString = generatePoint(plat, plng);

				map.put(valuePrefix+"pointkey_"+i+"_"+jj, Value.getAsGeoJSON(geoString));

				double rlat = 0.0 + (0.01 * i);
				double rlng = 0.0 - (0.10 * jj);

				geoString = generatePolygon(rlat, rlng);

				map.put(valuePrefix+"regionkey_"+i+"_"+jj, Value.getAsGeoJSON(geoString));

			}
			Bin bin = new Bin(binName, map);
			Bin bin2 = new Bin(binName2, "other_bin_value_"+i);
			client.put(params.writePolicy, key, bin, bin2);
		}

		console.info("Write " + size + " records.");
	}

	private void writeMapKeyRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String binName2,
		String valuePrefix,
		int size
	) throws Exception {
		for (int i = 0; i < size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			HashMap<Value, String> map = new HashMap<Value, String>();

			for (int jj = 0; jj < 10; ++jj) {

				double plat = 0.0 + (0.01 * i);
				double plng = 0.0 + (0.10 * jj);
				String geoString = generatePoint(plat, plng);

				map.put(Value.getAsGeoJSON(geoString),valuePrefix+"pointkey_"+i+"_"+jj);

				double rlat = 0.0 + (0.01 * i);
				double rlng = 0.0 - (0.10 * jj);

				geoString = generatePolygon(rlat, rlng);

				map.put(Value.getAsGeoJSON(geoString), valuePrefix+"regionkey_"+i+"_"+jj);

			}
			Bin bin = new Bin(binName, map);
			Bin bin2 = new Bin(binName2, "other_bin_value_"+i);
			client.put(params.writePolicy, key, bin, bin2);
		}

		console.info("Write " + size + " records.");
	}

	private void writeListRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String binName2,
		int size
	) throws Exception {
		for (int i = 0; i < size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			List<Value> mylist = new ArrayList<Value>();

			for (int jj = 0; jj < 10; ++jj) {

				double plat = 0.0 + (0.01 * i);
				double plng = 0.0 + (0.10 * jj);
				String geoString = generatePoint(plat, plng);

				mylist.add(Value.getAsGeoJSON(geoString));

				double rlat = 0.0 + (0.01 * i);
				double rlng = 0.0 - (0.10 * jj);

				geoString = generatePolygon(rlat, rlng);

				mylist.add(Value.getAsGeoJSON(geoString));

			}

			Bin bin = new Bin(binName, mylist);
			Bin bin2 = new Bin(binName2, "other_bin_value_"+i);
			client.put(params.writePolicy, key, bin, bin2);
		}

		console.info("Write " + size + " records.");
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String binName,
		String binName2,
		IndexCollectionType indexType
	) throws Exception {

		console.info("Query for: ns=%s set=%s bin=%s %s within <region>",
			params.namespace, params.set, binName, indexType.toString());

		StringBuilder rgnsb = generateQueryRegion();

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.geoWithinRegion(binName, indexType, rgnsb.toString()));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;
			Set <String> uniques = new HashSet<String>();

			while (rs.next()) {
				Record record = rs.getRecord();

				uniques.add(record.getString(binName2));
				count++;
			}

			if (count != 697) {
				console.error("Query failed. %d records expected. %d returned.", 697, count);
			}
			else if (uniques.size()!=21) {
				console.error("Query failed. %d unique records expected. %d unique returned.", 21, uniques.size());
			}
			else {
				console.info("query succeeded with %d records %d unique",count,uniques.size());
			}
		}
		finally {
			rs.close();
		}
	}

	private void deleteRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		int size
	) throws Exception {
		for (int i = 0; i < size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			client.delete(params.writePolicy, key);
		}
	}

	private StringBuilder generateQueryRegion() {
		StringBuilder rgnsb = new StringBuilder();
		rgnsb.append("{ ");
		rgnsb.append("    \"type\": \"Polygon\", ");
		rgnsb.append("    \"coordinates\": [[");
		rgnsb.append("        [-0.202, -0.202], ");
		rgnsb.append("        [ 0.202, -0.202], ");
		rgnsb.append("        [ 0.202,  0.202], ");
		rgnsb.append("        [-0.202,  0.202], ");
		rgnsb.append("        [-0.202, -0.202] ");
		rgnsb.append("    ]]");
		rgnsb.append(" } ");
		return rgnsb;
	}

	private String generatePoint(double plat, double plng) {
		return String.format("{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", plng, plat);
	}

	private String generatePolygon(double rlat, double rlng) {
		return String.format("{ \"type\": \"Polygon\", \"coordinates\": [ [[%f, %f], [%f, %f], [%f, %f], [%f, %f], [%f, %f]] ] }",
			rlng - 0.001, rlat - 0.001,
			rlng + 0.001, rlat - 0.001,
			rlng + 0.001, rlat + 0.001,
			rlng - 0.001, rlat + 0.001,
			rlng - 0.001, rlat - 0.001);
	}
}
