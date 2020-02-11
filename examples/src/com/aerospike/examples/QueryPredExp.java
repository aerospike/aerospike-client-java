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

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryPredExp extends Example {

	public QueryPredExp(Console console) {
		super(console);
	}

	/**
	 * Perform secondary index query with a predicate filter.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}

		String indexName = "predidx";
		String binName = params.getBinName("idxbin");
		int size = 50;

		createIndex(client, params, indexName, binName);
		writeRecords(client, params, binName, size);
		runQuery1(client, params, binName);
		runQuery2(client, params, binName);
		runQuery3(client, params, binName);
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
		AerospikeClient client,
		Parameters params,
		String binName,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

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
			client.put(params.writePolicy, key, bin1, bin2, bin3);
		}
	}

	private void runQuery1(
		AerospikeClient client,
		Parameters params,
		String binName
	) throws Exception {

		int begin = 10;
		int end = 40;

		console.info("Query Predicate: (bin2 > 126 && bin2 <= 140) or (bin2 = 360)");

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);

		// Filter applied on query itself.  Filter can only reference an indexed bin.
		stmt.setFilter(Filter.range(binName, begin, end));

		// Predicates are applied on query results on server side.
		// Predicates can reference any bin.
		stmt.setPredExp(
			PredExp.integerBin("bin2"),
			PredExp.integerValue(126),
			PredExp.integerGreater(),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(140),
			PredExp.integerLessEq(),
			PredExp.and(2),
			PredExp.integerBin("bin2"),
			PredExp.integerValue(360),
			PredExp.integerEqual(),
			PredExp.or(2)
			);

		RecordSet rs = client.query(null, stmt);

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
		AerospikeClient client,
		Parameters params,
		String binName
	) throws Exception {

		int begin = 10;
		int end = 40;

		console.info("Query Predicate: Record updated on 2017-01-15");
		Calendar beginTime = new GregorianCalendar(2017, 0, 15);
		Calendar endTime = new GregorianCalendar(2017, 0, 16);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setPredExp(
			PredExp.recLastUpdate(),
			PredExp.integerValue(beginTime),
			PredExp.integerGreaterEq(),
			PredExp.recLastUpdate(),
			PredExp.integerValue(endTime),
			PredExp.integerLess(),
			PredExp.and(2)
			);

		RecordSet rs = client.query(null, stmt);

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
		AerospikeClient client,
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
		stmt.setPredExp(
			PredExp.stringBin("bin3"),
			PredExp.stringValue("prefix.*suffix"),
			PredExp.stringRegex(RegexFlag.ICASE | RegexFlag.NEWLINE)
			);

		RecordSet rs = client.query(null, stmt);

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
