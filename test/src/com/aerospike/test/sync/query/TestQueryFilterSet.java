/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.test.sync.TestSync;

public class TestQueryFilterSet extends TestSync {
	private static final String set1 = "tqps1";
	private static final String set2 = "tqps2";
	private static final String set3 = "tqps3";
	private static final String binA = "a";
	private static final String binB = "b";

	@BeforeClass
	public static void prepare() {
		WritePolicy policy = new WritePolicy();

		// Write records in set p1.
		for (int i = 1; i <= 5; i++) {
			if (args.hasTtl) {
				policy.expiration = i * 60;
			}

			Key key = new Key(args.namespace, set1, i);
			client.put(policy, key, new Bin(binA, i));
		}

		// Write records in set p2.
		for (int i = 20; i <= 22; i++) {
			Key key = new Key(args.namespace, set2, i);
			client.put(null, key, new Bin(binA, i), new Bin(binB, (double)i));
		}

		// Write records in set p3 with send key.
		policy = new WritePolicy();
		policy.sendKey = true;

		for (int i = 31; i <= 40; i++) {
			Key intKey = new Key(args.namespace, set3, i);
			client.put(policy, intKey, new Bin(binA, i));

			Key strKey = new Key(args.namespace, set3, "key-p3-" + i);
			client.put(policy, strKey, new Bin(binA, i));
		}

		// Write one record in set p3 with send key not set.
		client.put(null, new Key(args.namespace, set3, 25), new Bin(binA, 25));
	}

	@Test
	public void querySetName() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.setName(), Exp.val(set2)));

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
	public void queryDouble() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set2);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.gt(Exp.floatBin(binB), Exp.val(21.5)));

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
	public void queryKeyString() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set3);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.regexCompare("^key-.*-35$", 0, Exp.key(Exp.Type.STRING)));

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
	public void queryKeyInt() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set3);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.lt(Exp.key(Exp.Type.INT), Exp.val(35)));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//System.out.println(rs.getKey().toString() + " - " + rs.getRecord().toString());
				count++;
			}
			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryKeyExists() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set3);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.keyExists());

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(20, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryVoidTime() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set1);

		GregorianCalendar now = new GregorianCalendar();
		GregorianCalendar end = new GregorianCalendar();
		end.add(Calendar.MINUTE, 2);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(
			Exp.and(
				Exp.ge(Exp.voidTime(), Exp.val(now)),
				Exp.lt(Exp.voidTime(), Exp.val(end))));

		RecordSet rs = client.query(policy, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				count++;
			}
			assertEquals(2, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryTTL() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(set1);

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.le(Exp.ttl(), Exp.val(60)));

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
