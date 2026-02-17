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
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Version;
import com.aerospike.test.sync.TestSync;

public class TestQueryRPS extends TestSync {
	private static final String indexName = "rpsindex";
	private static final String keyPrefix = "rpskey";
	private static final String binName1 = "rpsbin1";
	private static final String binName2 = "rpsbin2";
	private static final String binName3 = "rpsbin3";
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

		try {
			IndexTask itask = client.createIndex(args.indexPolicy, args.namespace,
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

	private void checkRuntime(Node n, Statement stmt, long id) {
		String taskId = Long.toUnsignedString(id);
		String module = (stmt.getFilter() == null) ? "scan" : "query";
		String command;
		Version serverVersion = n.getServerVersion();

		String cmd1 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "query-show:id=" + taskId : "query-show:trid=" + taskId;
		String cmd2 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? module + "-show:id=" + taskId : module + "-show:trid=" + taskId;
		String cmd3 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "jobs:module=" + module + ";cmd=get-job;id=" + taskId : "jobs:module=" + module + ";cmd=get-job;trid=" + taskId;

		if (n.hasPartitionQuery()) {
			// query-show works for both scan and query.
			command = cmd1;
		}
		else if (n.hasQueryShow()) {
			// scan-show and query-show are separate.
			command = cmd2;
		}
		else {
			// old job monitor syntax.
			command = cmd3;
		}

		String job_info = Info.request(n, command);
		String s = "run-time=";
		int runStart = job_info.indexOf(s) + s.length();

		job_info = job_info.substring(runStart,
				job_info.indexOf(':', runStart));

		int duration = Integer.parseInt(job_info);

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
		long taskId = new RandomShift().nextLong();

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setRecordsPerSecond(rps);
		stmt.setTaskId(taskId);

		RecordSet rs = client.query(null, stmt);

		drainRecords(rs);

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt, taskId);
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
			checkRuntime(n, stmt, task.getTaskId());
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
			checkRuntime(n, stmt, task.getTaskId());
		}
	}

	@Test
	public void scanAggregation() {
		long taskId = new RandomShift().nextLong();

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName1);
		stmt.setAggregateFunction(TestQuerySum.class.getClassLoader(),
				"udf/sum_example.lua", "sum_example", "sum_single_bin",
				Value.get(binName1));
		stmt.setRecordsPerSecond(rps);
		stmt.setTaskId(taskId);

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			while (rs.next()) {
			}
		}
		finally {
			rs.close();
		}

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt, taskId);
		}
	}

	@Ignore
	@Test
	public void query() {
		long taskId = new RandomShift().nextLong();

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setRecordsPerSecond(rps);
		stmt.setTaskId(taskId);

		RecordSet rs = client.query(null, stmt);

		drainRecords(rs);

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt, taskId);
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
			checkRuntime(n, stmt, task.getTaskId());
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
			checkRuntime(n, stmt, task.getTaskId());
		}
	}

	@Ignore
	@Test
	public void queryAggregation() {
		long taskId = new RandomShift().nextLong();

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName1);
		stmt.setFilter(Filter.range(binName1, 0, n_records));
		stmt.setAggregateFunction(TestQuerySum.class.getClassLoader(),
				"udf/sum_example.lua", "sum_example", "sum_single_bin",
				Value.get(binName1));
		stmt.setRecordsPerSecond(rps);
		stmt.setTaskId(taskId);

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			while (rs.next()) {
			}
		}
		finally {
			rs.close();
		}

		for (Node n : client.getNodes()) {
			checkRuntime(n, stmt, taskId);
		}
	}
}