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
package com.aerospike.client.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;

/**
 * Top-K must not retry a TIMEOUT; QUERY_TIMEOUT and FAIL_FORBIDDEN must not be
 * retried for any query or scan. No server required.
 */
public class PartitionTrackerRetryTest {

	@Test
	public void topKTimeoutIsNotRetried() {
		PartitionTracker tracker = topKTracker();
		assertFalse(tracker.shouldRetry(emptyNodePartitions(), timeout()));
	}

	@Test
	public void plainQueryTimeoutIsRetried() {
		PartitionTracker tracker = plainQueryTracker();
		assertTrue(tracker.shouldRetry(emptyNodePartitions(), timeout()));
	}

	@Test
	public void scanTimeoutIsRetried() {
		PartitionTracker tracker = new PartitionTracker(new ScanPolicy(), 1);
		assertTrue(tracker.shouldRetry(emptyNodePartitions(), timeout()));
	}

	@Test
	public void queryTimeoutFromServerLimitIsNotRetried() {
		AerospikeException ae = new AerospikeException(ResultCode.QUERY_TIMEOUT);

		assertFalse(topKTracker().shouldRetry(emptyNodePartitions(), ae));
		assertFalse(plainQueryTracker().shouldRetry(emptyNodePartitions(), ae));
		assertFalse(new PartitionTracker(new ScanPolicy(), 1).shouldRetry(emptyNodePartitions(), ae));
	}

	@Test
	public void forbiddenFromTopKConcurrencyCapIsNotRetried() {
		AerospikeException ae = new AerospikeException(ResultCode.FAIL_FORBIDDEN);

		assertFalse(topKTracker().shouldRetry(emptyNodePartitions(), ae));
		assertFalse(plainQueryTracker().shouldRetry(emptyNodePartitions(), ae));
	}

	private static PartitionTracker topKTracker() {
		Statement stmt = new Statement();
		stmt.setNamespace("test");
		stmt.setOrderBy("rank", BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(10);
		return new PartitionTracker(new QueryPolicy(), stmt, 1);
	}

	private static PartitionTracker plainQueryTracker() {
		Statement stmt = new Statement();
		stmt.setNamespace("test");
		return new PartitionTracker(new QueryPolicy(), stmt, 1);
	}

	private static NodePartitions emptyNodePartitions() {
		return new NodePartitions(null, 1);
	}

	private static AerospikeException timeout() {
		return new AerospikeException.Timeout(new QueryPolicy(), true);
	}
}
