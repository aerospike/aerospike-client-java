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
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryPredExp extends TestSync {
	private static final String setName = args.set + "p";
	private static final String indexName = "pred";
	private static final String keyPrefix = "pred";
	private static final String binName = "predint";
	private static final int size = 50;

	@BeforeClass
	public static void prepare() {
		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, args.namespace, setName, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, setName, keyPrefix + i);
			List<Integer> list = null;
			Map<String,String> map = null;

			if (i == 1) {
				list = new ArrayList<Integer>(5);
				list.add(1);
				list.add(2);
				list.add(4);
				list.add(9);
				list.add(20);
				// map will be null, which means mapbin will not exist in this record.
			}
			else if (i == 2) {
				list = new ArrayList<Integer>(3);
				list.add(5);
				list.add(9);
				list.add(100);
				// map will be null, which means mapbin will not exist in this record.
			}
			else if (i == 3) {
				map = new HashMap<String,String>();
				map.put("A", "AAA");
				map.put("B", "BBB");
				map.put("C", "BBB");
				// list will be null, which means listbin will not exist in this record.
			}
			else {
				list = new ArrayList<Integer>(0);
				map = new HashMap<String,String>(0);
			}
			client.put(null, key, new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map));
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, setName, indexName);
	}

	@Test
	public void queryPredicate1() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.integerBin("bin2"),
			PredExp.integerValue(40),
			PredExp.integerGreater(),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(44),
			PredExp.integerLess(),
			PredExp.and(2),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(22),
			PredExp.integerEqual(),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(9),
			PredExp.integerEqual(),
			PredExp.or(3),
			PredExp.integerBin(binName),
			PredExp.integerBin("bin2"),
			PredExp.integerEqual(),
			PredExp.and(2)
			);

		/*
		stmt.setPredicate(
			Predicate.bin("bin2").greaterThan(40).and(Predicate.bin("bin2").lessThan(44))
			.or(Predicate.bin("bin2").equal(22))
			.or(Predicate.bin("bin2").equal(9))
			.and(Predicate.bin(binName, Predicate.Type.INTEGER).equal(Predicate.bin("bin2")))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().getValue(binName));
				count++;
			}
			// 22, 41, 42, 43
			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate2() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.integerBin("bin2"),
			PredExp.integerValue(15),
			PredExp.integerGreaterEq(),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(42),
			PredExp.integerLessEq(),
			PredExp.and(2),
			PredExp.not()
			);

		/*
		stmt.setPredicate(
			Predicate.not(Predicate.bin("bin2").greaterThanOrEqual(15).and(Predicate.bin("bin2").lessThanOrEqual(42)))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().getValue(binName));
				count++;
			}
			// 10, 11, 12, 13, 43, 44, 45
			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate3() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.recLastUpdate(),
			PredExp.integerValue(System.currentTimeMillis() * 1000000L + 100),
			PredExp.integerGreater()
			);

		/*
		stmt.setPredicate(
			Predicate.recordLastUpdate().greaterThan(Predicate.nanos(System.currentTimeMillis() + 100))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			//int count = 0;

			while (rs.next()) {
				//Record record = rs.getRecord();
				//System.out.println(record.getValue(binName).toString() + ' ' + record.expiration);
				//count++;
			}
			// Do not asset count since some tests can run after this one.
			//assertEquals(0, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate4() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.integerVar("x"),
			PredExp.integerValue(4),
			PredExp.integerEqual(),
			PredExp.listBin("listbin"),
			PredExp.listIterateOr("x")
			);

		/*
		stmt.setPredicate(
			Predicate.listInclude("listbin", "x", Predicate.var("x").equal(4))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate5() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.integerVar("x"),
			PredExp.integerValue(5),
			PredExp.integerUnequal(),
			PredExp.listBin("listbin"),
			PredExp.listIterateAnd("x")
			);

		/*
		stmt.setPredicate(
			Predicate.listExclude("listbin", "x", Predicate.var("x").equal(5))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate6() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.stringVar("x"),
			PredExp.stringValue("B"),
			PredExp.stringEqual(),
			PredExp.mapBin("mapbin"),
			PredExp.mapKeyIterateOr("x")
			);

		/*
		stmt.setPredicate(
			Predicate.mapKeyInclude("mapbin", "x", Predicate.var("x").equal("B"))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate7() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.stringVar("x"),
			PredExp.stringValue("BBB"),
			PredExp.stringEqual(),
			PredExp.mapBin("mapbin"),
			PredExp.mapValIterateOr("x")
			);

		/*
		stmt.setPredicate(
				Predicate.mapValueInclude("mapbin", "x", Predicate.var("x").equal("BBB"))
				);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate8() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.stringVar("x"),
			PredExp.stringValue("D"),
			PredExp.stringUnequal(),
			PredExp.mapBin("mapbin"),
			PredExp.mapKeyIterateAnd("x")
			);

		/*
		stmt.setPredicate(
			Predicate.mapKeyExclude("mapbin", "x", Predicate.var("x").equal("D"))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate9() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.stringVar("x"),
			PredExp.stringValue("AAA"),
			PredExp.stringUnequal(),
			PredExp.mapBin("mapbin"),
			PredExp.mapValIterateAnd("x")
			);

		/*
		stmt.setPredicate(
			Predicate.mapValueExclude("mapbin", "x", Predicate.var("x").equal("AAA"))
			);
		*/

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(7, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryPredicate10() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.recDigestModulo(3),
			PredExp.integerValue(1),
			PredExp.integerEqual()
			);

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(2, count);
		}
		finally {
			rs.close();
		}
	}

	/* Use this test only after servers have been patched.
	@Test
	public void queryPredicateAER5650() {
		double lon = -122.0;
		double lat = 37.5;
		double radius = 50000.0;
		String rgnstr =
			String.format("{ \"type\": \"AeroCircle\", "
						  + "\"coordinates\": [[%.8f, %.8f], %f] }",
						  lon, lat, radius);
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		PredExp[] predexps = new PredExp[3];
		predexps[0] = PredExp.geoJSONBin(binName);
		predexps[1] = PredExp.geoJSONValue(rgnstr);
		predexps[2] = PredExp.geoJSONWithin();
		stmt.setPredExp(predexps);
		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(0, count);
		}
		finally {
			rs.close();
		}
	}*/
}
