/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.exp.StringExp;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryFilterExp extends TestSync {
	private static final String setName = args.set + "flt";
	private static final String indexName = "flt";
	private static final String keyPrefix = "flt";
	private static final String binName = "fltint";
	private static final int size = 50;

	@BeforeClass
	public static void prepare() {
		try {
			IndexTask task = client.createIndex(args.indexPolicy, args.namespace, setName, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		// sendKey=true so Exp.key() can be evaluated server-side.
		// The stored-key flag is set at record-create time; updates with sendKey=true
		// will NOT retroactively store the key on records created without it. Delete
		// any pre-existing records first so the puts below create fresh records.
		WritePolicy wp = new WritePolicy();
		wp.sendKey = true;

		// Write records with string keys
		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, setName, keyPrefix + i);
			client.delete(null, key);
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

			// Rotate prefixes so StringExp predicates pick deterministic subsets:
			// i % 3 == 0 -> "alpha-i", == 1 -> "beta-i", == 2 -> "gamma-i".
			String prefix;
			switch (i % 3) {
				case 0:  prefix = "alpha"; break;
				case 1:  prefix = "beta";  break;
				default: prefix = "gamma"; break;
			}
			String str = prefix + "-" + i;

			client.put(wp, key, new Bin(binName, i), new Bin("bin2", i), new Bin("listbin", list), new Bin("mapbin", map), new Bin("strbin", str));
		}
	}

	private static void assumeStringExpSupported() {
		Assume.assumeTrue(
			"Skipping: string expressions require server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, setName, indexName);
	}

	@Test
	public void queryAndOr() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// ((bin2 > 40 && bin2 < 44) || bin2 == 22 || bin2 == 9) && (binName == bin2)
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.and(
				Exp.or(
					Exp.and(
						Exp.gt(Exp.intBin("bin2"), Exp.val(40)),
						Exp.lt(Exp.intBin("bin2"), Exp.val(44))),
					Exp.eq(Exp.intBin("bin2"), Exp.val(22)),
					Exp.eq(Exp.intBin("bin2"), Exp.val(9))),
				Exp.eq(Exp.intBin(binName), Exp.intBin("bin2"))));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryNot() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// ! (bin2 >= 15 && bin2 <= 42)
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.not(
				Exp.and(
					Exp.ge(Exp.intBin("bin2"), Exp.val(15)),
					Exp.le(Exp.intBin("bin2"), Exp.val(42)))));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryLastUpdate() {
		int begin = 10;
		int end = 45;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// record last update time > (currentTimeMillis() * 1000000L + 100)
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.gt(
				Exp.lastUpdate(),
				Exp.val(System.currentTimeMillis() * 1000000L + 100)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryList1() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// List bin contains at least one integer item == 4
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.gt(
				ListExp.getByValue(ListReturnType.COUNT, Exp.val(4), Exp.listBin("listbin")),
				Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryList2() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// List bin does not contain integer item == 5
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.getByValue(ListReturnType.COUNT, Exp.val(5), Exp.listBin("listbin")),
				Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryList3() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// list[4] == 20
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(4), Exp.listBin("listbin")),
				Exp.val(20)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap1() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin contains key "B"
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.gt(
				MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val("B"), Exp.mapBin("mapbin")),
				Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap2() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin contains value "BBB"
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			MapExp.getByValue(MapReturnType.EXISTS, Exp.val("BBB"), Exp.mapBin("mapbin"))
			);

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap3() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin does not contains key "D"
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.eq(
				MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val("D"), Exp.mapBin("mapbin")),
				Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap4() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin does not contains value "AAA"
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.eq(
				MapExp.getByValue(MapReturnType.COUNT, Exp.val("AAA"), Exp.mapBin("mapbin")),
				Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap5() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin contains keys "A" and "C".
		QueryPolicy policy = new QueryPolicy();

		List<String> list = new ArrayList<String>();
		list.add("A");
		list.add("C");

		policy.filterExp = Exp.build(
			Exp.eq(
				MapExp.size(
					MapExp.getByKeyList(MapReturnType.KEY_VALUE, Exp.val(list), Exp.mapBin("mapbin"))),
				Exp.val(2)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryMap6() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Map bin contains keys "A" and "C".
		QueryPolicy policy = new QueryPolicy();

		List<String> list = new ArrayList<String>();
		list.add("A");
		list.add("C");

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.size( // return type VALUE returns a list
					MapExp.getByKeyList(MapReturnType.VALUE, Exp.val(list), Exp.mapBin("mapbin"))),
				Exp.val(2)));

		RecordSet rs = client.query(policy, stmt);

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
	public void queryDigestModulo() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// Record key digest % 3 == 1
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.digestModulo(3), Exp.val(1)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getRecord().toString());
				count++;
			}
			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryBinExists() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.binExists("bin2"));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(10, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryBinType() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.binType("listbin"), Exp.val(ParticleType.LIST)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(9, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryRecordSize() {
		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// This just tests that the expression was sent correctly
		// because all record sizes are effectively allowed.
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.ge(Exp.recordSize(), Exp.val(0)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(10, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringStartsWith() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// strbin starts with "alpha" -> records 3, 6, 9.
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.startsWith(Exp.val("alpha"), Exp.stringBin("strbin")));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(3, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringContains() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// strbin contains "amma" -> gamma records 2, 5, 8.
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.contains(Exp.val("amma"), Exp.stringBin("strbin")));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(3, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringEndsWith() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// strbin ends with "-10" -> only record 10.
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.endsWith(Exp.val("-10"), Exp.stringBin("strbin")));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringRegexCompare() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// strbin matches /^beta-/ -> records 1, 4, 7, 10.
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.regexCompare(Exp.val("^beta-"), Exp.stringBin("strbin")));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringContainsOnSetName() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// setName ends with "flt" -> every record's set name contains "flt".
		// Probe to determine whether the new string-ops family supports source
		// expressions other than bin/CDT projections (e.g. Exp.setName(), Exp.key()).
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.contains(Exp.val("flt"), Exp.setName()));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(10, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryKeyStringRegexCompare() {
		assumeStringExpSupported();

		int begin = 1;
		int end = 10;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);
		stmt.setFilter(Filter.range(binName, begin, end));

		// User keys are "flt1".."flt50". Within range 1..10, /^flt3$/ matches "flt3".
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			StringExp.regexCompare(
				Exp.val("^flt3$"),
				StringRegexFlags.DEFAULT,
				Exp.key(Exp.Type.STRING)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}
}
