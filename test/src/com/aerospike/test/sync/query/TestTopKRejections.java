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
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Assume;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.Statement;
import com.aerospike.test.sync.TestSync;

/**
 * Top-K requests the client refuses before sending anything: policy settings Top-K can't honor,
 * and query or execute calls that don't support it.
 */
public class TestTopKRejections extends TestSync {
	private static final String SET = "topKRejections";
	private static final String BIN = "rank";
	private static final String UNSUPPORTED_CALL = "Top-K is only supported by synchronous partition queries";
	private static final String MAX_RECORDS = "Top-K is incompatible with maxRecords";
	private static final String BACKGROUND = "orderBy/topK is only valid on foreground (SELECT-style) " +
		"queries; background (UPDATE-style) queries do not produce a result stream";
	private static final String AGGREGATE =
		"orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage";

	@Test
	public void includeBinDataFalseIsRejected() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.includeBinData = false;

		assertRejected("Top-K is incompatible with includeBinData=false", () -> client.query(policy, topK()));
	}

	@Test
	public void policyMaxRecordsIsRejected() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.maxRecords = 10;

		assertRejected(MAX_RECORDS, () -> client.query(policy, topK()));
	}

	@Test
	public void statementMaxRecordsIsRejected() {
		Statement statement = topK();
		statement.setMaxRecords(10);

		assertRejected(MAX_RECORDS, () -> client.query(null, statement));
	}

	@Test
	public void backgroundExecuteIsRejected() {
		assertRejected(BACKGROUND,
			() -> client.execute(null, topK(), Operation.put(new Bin(BIN, 1))));
		assertRejected(BACKGROUND,
			() -> client.execute(null, topK(), "record_example", "processRecord"));
	}

	@Test
	public void aggregationIsRejected() {
		assertRejected(AGGREGATE,
			() -> client.queryAggregate(null, topK(), "sum_example", "sum_single_bin"));
		assertRejected(AGGREGATE,
			() -> client.queryAggregateNode(null, topK(), client.getNodes()[0]));
	}

	@Test
	public void queryListenerIsRejected() {
		assertRejected(UNSUPPORTED_CALL, () -> client.query(null, topK(), (key, record) -> {}));
		assertRejected(UNSUPPORTED_CALL,
			() -> client.query(null, topK(), PartitionFilter.all(), (key, record) -> {}));
	}

	private static void requireCapableCluster() {
		Assume.assumeTrue("Server-side Top-K requires server version 8.2.1 or later on every node",
			client.getCluster().hasQueryOrderBy);
	}

	private static Statement topK() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		statement.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		statement.setTopK(3);
		return statement;
	}

	private static void assertRejected(String message, ThrowingRunnable call) {
		AerospikeException ae = assertThrows(AerospikeException.InvalidArgument.class, call);
		assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
		assertEquals(message, ae.getBaseMessage());
	}
}
