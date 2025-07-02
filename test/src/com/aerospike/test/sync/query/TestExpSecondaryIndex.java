/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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

import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;


public class TestExpSecondaryIndex extends TestSync {
	private static final String setName = "exp_SI_test_set";
	private static final String indexName = "εχπ_ΣΙ_τεστ_ιδχ";
	private static final List<String> countries =  Arrays.asList("Australia", "Canada", "USA");
	private static final Expression exp = Exp.build(
		// IF (age >= 18 AND country IN ["Australia, "Canada", "USA"])
		Exp.cond(
			Exp.and(
				Exp.ge(   // Is the age 18 or older?
					Exp.intBin("age"),
					Exp.val(18)
				),
				Exp.or( // Do they live in a target country?
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(0))),
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(1))),
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(2)))
				)
			),
			Exp.val(1),
			Exp.unknown()
		)
	);

	public void insertTestRecords() {
		insertPersonRecord( 1, "Tim", 312, "Australia");
		insertPersonRecord( 2, "Bob", 47, "Canada");
		insertPersonRecord( 3, "Jo", 15, "USA");
		insertPersonRecord( 4, "Steven", 23, "Botswana");
		insertPersonRecord( 5, "Susan", 32, "Canada");
		insertPersonRecord( 6, "Jess", 17, "USA");
		insertPersonRecord( 7, "Sam", 18, "USA");
		insertPersonRecord( 8, "Alex", 47, "Canada");
		insertPersonRecord( 9, "Pam", 56, "Australia");
		insertPersonRecord( 10, "Vivek", 12, "India");
		insertPersonRecord( 11, "Kiril", 22, "Sweden");
		insertPersonRecord( 12, "Bill", 23, "UK");
	}

	private static void insertPersonRecord(int key, String name, int age, String country) {
		client.put(null,
				new Key(args.namespace, setName, key),
				new Bin("name", name),
				new Bin("age", age),
				new Bin("country", country));
	}

	public static String getSecondaryIndices() {
		String cmd = "sindex-list/" + args.namespace + '/' + indexName;
		return Info.request(client.getCluster().getRandomNode(), cmd);
	}

	public static void addExpSI() {
		try {
			IndexTask task = client.createIndex(null, args.namespace, setName, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, exp);
			task.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, setName, indexName);
	}


	@Test
	public void createExpSI() {
		addExpSI();
		String indices = getSecondaryIndices();
		Assert.assertTrue(indices.contains("indexname=" + indexName));
	}

	@Test
	public void queryExpSIbyName() {
		String sincdices = getSecondaryIndices();
		if (!sincdices.contains("indexname=" + indexName)) {
			addExpSI();
		}

		insertTestRecords();
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);

		stmt.setFilter(Filter.rangeByIndex(indexName, 1, 1));
		QueryPolicy qp = new QueryPolicy();

		int count = 0;
		try (RecordSet recordSet = client.query(qp, stmt)) {
			while (recordSet.next()) {
				Record record = recordSet.getRecord();
				int age = record.getInt("age");
				String country = record.getString("country");
				assertTrue(age >= 18);
				assertTrue(countries.contains(country));
				count++;
			}
		}
		/*
		(bins:(name:Alex),(age:47),(country:Canada))
		(bins:(name:Tim),(age:312),(country:Australia))
		(bins:(name:Pam),(age:56),(country:Australia))
		(bins:(name:Bob),(age:47),(country:Canada))
		(bins:(name:Sam),(age:18),(country:USA))
		(bins:(name:Susan),(age:32),(country:Canada))
		 */
		assertEquals(6, count);
	}

	@Test
	public void queryExpSIbyExp() {
		String sincdices = getSecondaryIndices();
		if (!sincdices.contains("indexname=" + indexName)) {
			addExpSI();
		}

		insertTestRecords();
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);

		stmt.setFilter(Filter.range(exp, 1, 1));
		QueryPolicy qp = new QueryPolicy();

		int count = 0;
		try (RecordSet recordSet = client.query(qp, stmt)) {
			while (recordSet.next()) {
				Record record = recordSet.getRecord();
				int age = record.getInt("age");
				String country = record.getString("country");
				assertTrue(age >= 18);
				assertTrue(countries.contains(country));
				count++;
			}
		}
		/*
		(bins:(name:Alex),(age:47),(country:Canada))
		(bins:(name:Tim),(age:312),(country:Australia))
		(bins:(name:Pam),(age:56),(country:Australia))
		(bins:(name:Bob),(age:47),(country:Canada))
		(bins:(name:Sam),(age:18),(country:USA))
		(bins:(name:Susan),(age:32),(country:Canada))
		 */
		assertEquals(6, count);
	}
}
