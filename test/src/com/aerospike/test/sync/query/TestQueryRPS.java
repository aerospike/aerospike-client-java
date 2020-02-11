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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryRPS extends TestSync {
	private static final String indexName = "qeindex1";
	private static final String keyPrefix = "qekey";
	private static final String binName1 = args.getBinName("qebin1");
	private static final String binName2 = args.getBinName("qebin2");
	private static final String binName3 = args.getBinName("qebin3");
	private static final int records_per_node = 1000;
	private static final int rps = 1000;
	private static final int expected_duration = 1000 * records_per_node / rps;

	private static int n_records = 0;

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask0 = client.register(null,
				TestQuerySum.class.getClassLoader(), "udf/sum_example.lua",
				"sum_example.lua", Language.LUA);
		RegisterTask rtask1 = client.register(null,
				TestQueryExecute.class.getClassLoader(),
				"udf/record_example.lua", "record_example.lua", Language.LUA);

		rtask0.waitTillComplete();
		rtask1.waitTillComplete();

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask itask = client.createIndex(policy, args.namespace,
					args.set, indexName, binName1, IndexType.NUMERIC);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		n_records = records_per_node * client.getNodes().length;

		client.truncate(null, args.namespace, args.set, null);

		for (int i = 1; i <= n_records; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			client.put(null, key, new Bin(binName1, i), new Bin(binName2, i));
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	void checkRuntime(Node n, Statement stmt) {
		String trid = Long.toUnsignedString(stmt.getTaskId());
		String job_info = Info.request(n, "jobs:");
		String s = "run-time=";

		assert job_info.contains(trid);

		int start = job_info.indexOf(trid) + trid.length();
		int runStart = job_info.indexOf(s, start) + s.length();

		job_info = job_info.substring(runStart,
				job_info.indexOf(':', runStart));

		int duration = Integer.parseInt(job_info);

//		System.out.println(n.getName() + " - " + String.valueOf(duration) +
//				" - " + String.valueOf(expected_duration));
//		System.out.println(trid);
//		System.out.println(start);
//		System.out.println(Info.request(n, "jobs:"));

		assert (duration > expected_duration - 500 &&
				duration < expected_duration + 500);
	}

	void drainRecords(RecordSet rs) {
		try {
			while (rs.next()) {
			}
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void scan() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setRecordsPerSecond(rps);

		RecordSet rs = client.query(null, stmt);

		drainRecords(rs);

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Test
	public void bgScanWithOps() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
		stmt.setRecordsPerSecond(rps);

		ExecuteTask task = client.execute(null, stmt, Operation.put(
				new Bin(binName3, 1)));

		task.waitTillComplete();

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Test
	public void bgScanWithUDF() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
		stmt.setRecordsPerSecond(rps);

		ExecuteTask task = client.execute(null, stmt, "record_example",
				"processRecord", Value.get(binName2), Value.get(binName2),
				Value.get(100));

		task.waitTillComplete();

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Test
	public void scanAggregation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName1);
		stmt.setAggregateFunction(TestQuerySum.class.getClassLoader(),
				"udf/sum_example.lua", "sum_example", "sum_single_bin",
				Value.get(binName1));
		stmt.setRecordsPerSecond(rps);

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			while (rs.next()) {
			}
		}
		finally {
			rs.close();
		}

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Ignore
	@Test
	public void query() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setRecordsPerSecond(rps);

		RecordSet rs = client.query(null, stmt);

		drainRecords(rs);

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Ignore
	@Test
	public void bgQueryWithOps() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setRecordsPerSecond(rps);

		ExecuteTask task = client.execute(null, stmt, Operation.put(
				new Bin(binName3, 1)));

		task.waitTillComplete();

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Ignore
	@Test
	public void bgQueryWithUDF() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setRecordsPerSecond(rps);

		ExecuteTask task = client.execute(null, stmt, "record_example",
				"processRecord", Value.get(binName2), Value.get(binName2),
				Value.get(100));

		task.waitTillComplete();

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}

	@Ignore
	@Test
	public void queryAggregation() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName1);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setAggregateFunction(TestQuerySum.class.getClassLoader(),
				"udf/sum_example.lua", "sum_example", "sum_single_bin",
				Value.get(binName1));
		stmt.setRecordsPerSecond(rps);

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			while (rs.next()) {
			}
		}
		finally {
			rs.close();
		}

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt);
		}
	}
}