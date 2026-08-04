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
package com.aerospike.examples.fixtures;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.examples.ExampleFixture;
import com.aerospike.examples.Parameters;

public final class QueryExampleFixtures {
	private QueryExampleFixtures() {
	}

	public static ExampleFixture queryStringExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "queryindex");
				FixtureSupport.deleteKeyRange(client, params, "querykey", 1, 5);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, params.set(), "querybin", Filter.equal("querybin", "queryvalue3"));
				ExampleAssertions.assertQueryCount(client, params.readPolicy(), stmt, 1);
				ExampleAssertions.assertBinEquals(
					client,
					params.readPolicy(),
					FixtureSupport.key(params, "querykey3"),
					"querybin",
					"queryvalue3");
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "queryindex");
				FixtureSupport.deleteKeyRange(client, params, "querykey", 1, 5);
			}
		};
	}

	public static ExampleFixture storeKeyExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "skindex");
				FixtureSupport.deleteKeyRange(client, params, "skkey", 1, 10);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, params.set(), "skbin", Filter.range("skbin", 2, 5));
				ExampleAssertions.assertQueryUserKeys(client, ExampleAssertions.queryPolicy(params.readPolicy()), stmt, 4);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "skindex");
				FixtureSupport.deleteKeyRange(client, params, "skkey", 1, 10);
			}
		};
	}

	public static ExampleFixture queryCollectionExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "mapkey_index");
				FixtureSupport.deleteKeyRange(client, params, "qkey", 1, 20);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(
					params,
					params.set(),
					"map_bin",
					Filter.contains("map_bin", IndexCollectionType.MAPKEYS, "mkey2"));

				try (RecordSet rs = client.query(null, stmt)) {
					int count = 0;

					while (rs.next()) {
						Record record = rs.getRecord();
						@SuppressWarnings("unchecked")
						Map<String,Object> map = (Map<String,Object>)record.getValue("map_bin");

						if (map == null || ! map.containsKey("mkey2")) {
							throw new IllegalStateException("Expected query map to contain mkey2 but found " + map);
						}
						count++;
					}

					if (count != 10) {
						throw new IllegalStateException("Expected 10 query collection results but found " + count);
					}
				}
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "mapkey_index");
				FixtureSupport.deleteKeyRange(client, params, "qkey", 1, 20);
			}
		};
	}

	public static ExampleFixture asyncQueryExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "asqindex");
				FixtureSupport.deleteKeyRange(client, params, "asqkey", 1, 50);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, params.set(), "asqbin", Filter.range("asqbin", 26, 34));
				ExampleAssertions.assertQueryCount(client, params.readPolicy(), stmt, 9);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "asqindex");
				FixtureSupport.deleteKeyRange(client, params, "asqkey", 1, 50);
			}
		};
	}

	public static ExampleFixture queryExpExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "predidx");
				FixtureSupport.deleteNumericKeyRange(client, params, params.set(), 1, 50);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertQueryCount(client, queryExpPolicy1(client), queryExpStatement(params, 10, 40), 3);
				ExampleAssertions.assertQueryCount(client, queryExpPolicy2(client), queryExpStatement(params, 10, 40), 0);
				ExampleAssertions.assertQueryCount(client, queryExpPolicy3(client), queryExpStatement(params, 20, 30), 6);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "predidx");
				FixtureSupport.deleteNumericKeyRange(client, params, params.set(), 1, 50);
			}
		};
	}

	public static ExampleFixture queryFilterExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "profileindex");
				FixtureSupport.deleteKeys(client, params, "profilekey1", "profilekey2", "profilekey3");
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = new Statement();
				stmt.setNamespace(params.namespace());
				stmt.setSetName(params.set());
				stmt.setFilter(Filter.equal("name", "Bill"));
				stmt.setAggregateFunction("filter_example", "profile_filter", Value.get("hknfpkj"));

				ResultSet rs = client.queryAggregate(null, stmt);
				ExampleAssertions.assertAggregateSingleMapFields(rs, mapOf("name", "Bill", "password", "hknfpkj"));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "profileindex");
				FixtureSupport.deleteKeys(client, params, "profilekey1", "profilekey2", "profilekey3");
			}
		};
	}

	public static ExampleFixture queryExecuteExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "qeindex1");
				FixtureSupport.deleteKeyRange(client, params, "qekey", 1, 10);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "qekey4"), "qebin1", 104L);
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "qekey6"), "qebin1", 106L);
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "qekey8"), "qebin1", 108L);
				ExampleAssertions.assertBinEquals(client, params.readPolicy(), FixtureSupport.key(params, "qekey5"), "qebin1", 5L);
				ExampleAssertions.assertBinRemoved(client, params.readPolicy(), FixtureSupport.key(params, "qekey5"), "qebin2");
				ExampleAssertions.assertRecordMissing(client, params.readPolicy(), FixtureSupport.key(params, "qekey9"));
				Statement stmt = statement(params, params.set(), null, Filter.range("qebin1", 1, 110));
				ExampleAssertions.assertQueryCount(client, params.readPolicy(), stmt, 9);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "qeindex1");
				FixtureSupport.deleteKeyRange(client, params, "qekey", 1, 10);
			}
		};
	}

	public static ExampleFixture querySumExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "aggindex");
				FixtureSupport.deleteKeyRange(client, params, "aggkey", 1, 10);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, params.set(), "aggbin", Filter.range("aggbin", 4, 7));
				stmt.setAggregateFunction("sum_example", "sum_single_bin", Value.get("aggbin"));
				ResultSet rs = client.queryAggregate(null, stmt);
				ExampleAssertions.assertAggregateSingleLong(rs, 22L);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "aggindex");
				FixtureSupport.deleteKeyRange(client, params, "aggkey", 1, 10);
			}
		};
	}

	public static ExampleFixture queryAverageExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "avgindex");
				FixtureSupport.deleteKeyRange(client, params, "avgkey", 1, 10);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = new Statement();
				stmt.setNamespace(params.namespace());
				stmt.setSetName(params.set());
				stmt.setFilter(Filter.range("l2", 0, 1000));
				stmt.setAggregateFunction("average_example", "average");
				ResultSet rs = client.queryAggregate(null, stmt);
				ExampleAssertions.assertAggregateSingleMapFields(rs, mapOf("sum", 55L, "count", 10L));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "avgindex");
				FixtureSupport.deleteKeyRange(client, params, "avgkey", 1, 10);
			}
		};
	}

	public static ExampleFixture queryRegionExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "queryindexloc");
				FixtureSupport.deleteKeyRange(client, params, "querykeyloc", 0, 19);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				ExampleAssertions.assertQueryCount(
					client,
					params.readPolicy(),
					statement(params, params.set(), "querybinloc", Filter.geoWithinRegion("querybinloc", queryRegionPolygon())),
					6);
				ExampleAssertions.assertQueryCount(
					client,
					params.readPolicy(),
					statement(params, params.set(), "querybinloc", Filter.geoWithinRadius("querybinloc", -122.0, 37.5, 50000.0)),
					4);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "queryindexloc");
				FixtureSupport.deleteKeyRange(client, params, "querykeyloc", 0, 19);
			}
		};
	}

	public static ExampleFixture queryRegionFilterExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "filterindexloc");
				FixtureSupport.deleteKeyRange(client, params, "filterkeyloc", 0, 19);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = new Statement();
				stmt.setNamespace(params.namespace());
				stmt.setSetName(params.set());
				stmt.setFilter(Filter.geoWithinRegion("filterloc", queryRegionPolygon()));
				stmt.setAggregateFunction("geo_filter_example", "match_amenity", Value.get("school"));
				assertAggregateCount(client.queryAggregate(null, stmt));
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "filterindexloc");
				FixtureSupport.deleteKeyRange(client, params, "filterkeyloc", 0, 19);
			}
		};
	}

	public static ExampleFixture queryGeoCollectionExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "geo_map");
				FixtureSupport.dropIndexIfExists(client, params, "geo_list");
				FixtureSupport.deleteKeyRange(client, params, "map", 0, 999);
				FixtureSupport.deleteKeyRange(client, params, "list", 0, 999);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement mapStmt = new Statement();
				mapStmt.setNamespace(params.namespace());
				mapStmt.setSetName(params.set());
				mapStmt.setFilter(Filter.geoWithinRegion("geo_map_bin", IndexCollectionType.MAPVALUES, geoCollectionQueryRegion()));
				ExampleAssertions.assertQueryUniqueBinValueCount(
					client,
					ExampleAssertions.queryPolicy(params.readPolicy()),
					mapStmt,
					"geo_uniq_bin",
					21);

				Statement listStmt = new Statement();
				listStmt.setNamespace(params.namespace());
				listStmt.setSetName(params.set());
				listStmt.setFilter(Filter.geoWithinRegion("geo_list_bin", IndexCollectionType.LIST, geoCollectionQueryRegion()));
				ExampleAssertions.assertQueryUniqueBinValueCount(
					client,
					ExampleAssertions.queryPolicy(params.readPolicy()),
					listStmt,
					"geo_uniq_bin",
					21);
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "geo_map");
				FixtureSupport.dropIndexIfExists(client, params, "geo_list");
				FixtureSupport.deleteKeyRange(client, params, "map", 0, 999);
				FixtureSupport.deleteKeyRange(client, params, "list", 0, 999);
			}
		};
	}

	public static ExampleFixture queryPageExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "pq", "pqidx");
				FixtureSupport.deleteNumericKeyRange(client, params, "pq", 1, 190);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, "pq", "bin", Filter.range("bin", 1, 200));
				stmt.setMaxRecords(100);
				PartitionFilter filter = PartitionFilter.all();
				int total = 0;

				while (! filter.isDone()) {
					try (RecordSet rs = client.queryPartitions(null, stmt, filter)) {
						while (rs.next()) {
							total++;
						}
					}
				}

				if (total != 190) {
					throw new IllegalStateException("Expected paged query to return 190 records but found " + total);
				}
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "pq", "pqidx");
				FixtureSupport.deleteNumericKeyRange(client, params, "pq", 1, 190);
			}
		};
	}

	public static ExampleFixture queryResumeExample() {
		return new ExampleFixture() {
			@Override
			public void setup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "qr", "qridx");
				FixtureSupport.deleteNumericKeyRange(client, params, "qr", 1, 200);
			}

			@Override
			public void verify(IAerospikeClient client, Parameters params) {
				Statement stmt = statement(params, "qr", "bin", Filter.range("bin", 1, 200));
				PartitionFilter filter = PartitionFilter.all();
				AtomicInteger firstPass = new AtomicInteger();

				try {
					client.query(null, stmt, filter, (key, record) -> {
						if (firstPass.incrementAndGet() >= 50) {
							throw new AerospikeException.QueryTerminated();
						}
					});
				}
				catch (AerospikeException.QueryTerminated ignored) {
				}

				if (firstPass.get() != 50) {
					throw new IllegalStateException("Expected first query pass to return 50 records but found " + firstPass.get());
				}

				AtomicInteger resumed = new AtomicInteger();
				client.query(null, stmt, filter, (key, record) -> resumed.incrementAndGet());

				if (resumed.get() != 151) {
					throw new IllegalStateException("Expected resumed query to return 151 records but found " + resumed.get());
				}
			}

			@Override
			public void cleanup(IAerospikeClient client, Parameters params) throws Exception {
				FixtureSupport.dropIndexIfExists(client, params, "qr", "qridx");
				FixtureSupport.deleteNumericKeyRange(client, params, "qr", 1, 200);
			}
		};
	}

	private static Statement statement(Parameters params, String setName, String binName, Filter filter) {
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace());
		stmt.setSetName(setName);

		if (binName != null) {
			stmt.setBinNames(binName);
		}
		stmt.setFilter(filter);
		return stmt;
	}

	private static QueryPolicy queryExpPolicy1(IAerospikeClient client) {
		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
			Exp.or(
				Exp.and(
					Exp.gt(Exp.intBin("bin2"), Exp.val(126)),
					Exp.le(Exp.intBin("bin2"), Exp.val(140))),
				Exp.eq(Exp.intBin("bin2"), Exp.val(360))));
		return policy;
	}

	private static QueryPolicy queryExpPolicy2(IAerospikeClient client) {
		QueryPolicy policy = client.copyQueryPolicyDefault();
		Calendar beginTime = new GregorianCalendar(2020, Calendar.JANUARY, 1);
		Calendar endTime = new GregorianCalendar(2021, Calendar.JANUARY, 1);
		policy.filterExp = Exp.build(
			Exp.and(
				Exp.ge(Exp.lastUpdate(), Exp.val(beginTime)),
				Exp.lt(Exp.lastUpdate(), Exp.val(endTime))));
		return policy;
	}

	private static QueryPolicy queryExpPolicy3(IAerospikeClient client) {
		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(Exp.regexCompare("prefix.*suffix", 10, Exp.stringBin("bin3")));
		return policy;
	}

	private static Statement queryExpStatement(Parameters params, int begin, int end) {
		return statement(params, params.set(), null, Filter.range("idxbin", begin, end));
	}

	private static void assertAggregateCount(ResultSet rs) {
		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}

			if (count != 2) {
				throw new IllegalStateException("Expected 2 aggregate results but found " + count);
			}
		}
		finally {
			rs.close();
		}
	}

	private static Map<String,Object> mapOf(Object... entries) {
		Map<String,Object> map = new HashMap<>();

		for (int i = 0; i < entries.length; i += 2) {
			map.put((String)entries[i], entries[i + 1]);
		}
		return map;
	}

	private static String queryRegionPolygon() {
		return """
			{
			  "type": "Polygon",
			  "coordinates": [
			    [
			      [-122.500000, 37.000000],
			      [-121.000000, 37.000000],
			      [-121.000000, 38.080000],
			      [-122.500000, 38.080000],
			      [-122.500000, 37.000000]
			    ]
			  ]
			}
			""";
	}

	private static String geoCollectionQueryRegion() {
		return """
			{
			  "type": "Polygon",
			  "coordinates": [
			    [
			      [-0.202, -0.202],
			      [0.202, -0.202],
			      [0.202, 0.202],
			      [-0.202, 0.202],
			      [-0.202, -0.202]
			    ]
			  ]
			}
			""";
	}

	private static String geoPoint(double lng, double lat) {
		return String.format("{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", lng, lat);
	}

	private static String geoPolygon(double lng, double lat) {
		return String.format(
			"{ \"type\": \"Polygon\", \"coordinates\": [ [[%f, %f], [%f, %f], [%f, %f], [%f, %f], [%f, %f]] ] }",
			lng - 0.001, lat - 0.001,
			lng + 0.001, lat - 0.001,
			lng + 0.001, lat + 0.001,
			lng - 0.001, lat + 0.001,
			lng - 0.001, lat - 0.001);
	}
}
