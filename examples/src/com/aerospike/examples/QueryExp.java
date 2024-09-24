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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryExp extends Example {

	public QueryExp(Console console) {
		super(console);
	}

	/**
	 * Perform secondary index query with a predicate filter.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		String indexName = "predidx";
		String binName = "idxbin";
		int size = 50;

		if (!params.useProxyClient) {
			createIndex(client, params, indexName, binName);
		}

		writeRecords(client, params, binName, size);
		runQuery1(client, params, binName);
		runQuery2(client, params, binName);
		runQuery3(client, params, binName);
		runQuery4(client, params, binName);
		runQuery5(client, params, binName);

		//client.dropIndex(params.policy, params.namespace, params.set, indexName);
	}

	private void createIndex(
		IAerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(
		IAerospikeClient client,
		Parameters params,
		String binName,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

		byte[] blob = new byte[] {3, 52, 125};
		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(5);

		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1);
		innerMap.put(2, "string3");
		innerMap.put(3, blob);
		innerMap.put("list", inner);

		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(8);
		list.add(inner);
		list.add(innerMap);

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, i);
			Bin bin1 = new Bin(binName, i);
			Bin bin2 = new Bin("bin2", i * 10);
			Bin bin3;

			if (i % 4 == 0) {
				bin3 = new Bin("bin3", "prefix-" + i + "-suffix");
			}
			else if (i % 2 == 0) {
				bin3 = new Bin("bin3", "prefix-" + i + "-SUFFIX");
			}
			else {
				bin3 = new Bin("bin3", "pre-" + i + "-suf");
			}

			Bin bin4 = new Bin("bin4", list);

			client.put(params.writePolicy, key, bin1, bin2, bin3, bin4);
		}
	}

	private void runQuery1(
		IAerospikeClient client,
		Parameters params,
		String binName
	) throws Exception {

		int begin = 10;
		int end = 40;

		console.info("Query Predicate: (bin2 > 126 && bin2 <= 140) || (bin2 = 360)");

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);

		// Filter applied on query itself.  Filter can only reference an indexed bin.
		stmt.setFilter(Filter.range(binName, begin, end));

		// Predicates are applied on query results on server side.
		// Predicates can reference any bin.
		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
			Exp.or(
				Exp.and(
					Exp.gt(Exp.intBin("bin2"), Exp.val(126)),
					Exp.le(Exp.intBin("bin2"), Exp.val(140))),
				Exp.eq(Exp.intBin("bin2"), Exp.val(360))));

		RecordSet rs = client.query(policy, stmt);

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				console.info("Record: " + record.toString());
			}
		}
		finally {
			rs.close();
		}
	}

	private void runQuery2(
		IAerospikeClient client,
		Parameters params,
		String binName
	) throws Exception {

		int begin = 10;
		int end = 40;

		console.info("Query Predicate: Record updated in 2020");
		Calendar beginTime = new GregorianCalendar(2020, 0, 1);
		Calendar endTime = new GregorianCalendar(2021, 0, 1);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
			Exp.and(
				Exp.ge(Exp.lastUpdate(), Exp.val(beginTime)),
				Exp.lt(Exp.lastUpdate(), Exp.val(endTime))));

		RecordSet rs = client.query(policy, stmt);

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				console.info("Record: " + record.toString());
			}
		}
		finally {
			rs.close();
		}
	}

	private void runQuery3(
		IAerospikeClient client,
		Parameters params,
		String binName
	) throws Exception {

		int begin = 20;
		int end = 30;

		console.info("Query Predicate: bin3 contains string with 'prefix' and 'suffix'");

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
			Exp.regexCompare("prefix.*suffix", RegexFlag.ICASE | RegexFlag.NEWLINE, Exp.stringBin("bin3")));

		RecordSet rs = client.query(policy, stmt);

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				console.info("Record: " + record.toString());
			}
		}
		finally {
			rs.close();
		}
	}
	// Example 4: Get a value from a map inside a list
	private void runQuery4(
			IAerospikeClient client,
			Parameters params,
			String binName
	) throws Exception {

		int begin = 20;
		int end = 30;

		console.info("Query Predicate: bin4 map key 2 at list index 3 contains regex str.*3");

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
				Exp.regexCompare("str.*3", RegexFlag.ICASE | RegexFlag.NEWLINE,
						MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(2),
						ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.MAP, Exp.val(3), Exp.listBin("bin4")))));

		RecordSet rs = client.query(policy, stmt);

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				console.info("Record: " + record.toString());
			}
		}
		finally {
			rs.close();
		}
	}
	// Example 5: Get a value from an inner list from a map inside an outer list
	private void runQuery5(
			IAerospikeClient client,
			Parameters params,
			String binName
	) throws Exception {

		int begin = 20;
		int end = 30;

		console.info("Query Predicate: bin4, inner list index 0, from map key 'list' at outer list index 3 contains regex str.*2");

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.range(binName, begin, end));

		QueryPolicy policy = client.copyQueryPolicyDefault();
		policy.filterExp = Exp.build(
				Exp.regexCompare("str.*2", RegexFlag.ICASE | RegexFlag.NEWLINE,
						ListExp.getByIndex(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(0),
						MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST, Exp.val("list"),
						ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.MAP, Exp.val(3), Exp.listBin("bin4"))))));

		RecordSet rs = client.query(policy, stmt);

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				console.info("Record: " + record.toString());
			}
		}
		finally {
			rs.close();
		}
	}

}
