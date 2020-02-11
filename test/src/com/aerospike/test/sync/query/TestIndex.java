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

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestIndex extends TestSync {
	private static final String indexName = "testindex";
	private static final String binName = "testbin";

	@Test
	public void createDrop() {
		Policy policy = new Policy();
		policy.setTimeout(0);

		IndexTask task;

		// Drop index if it already exists.
		try {
			task = client.dropIndex(policy, args.namespace, args.set, indexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
		task.waitTillComplete();

		task = client.dropIndex(policy, args.namespace, args.set, indexName);
		task.waitTillComplete();

		task = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
		task.waitTillComplete();

		task = client.dropIndex(policy, args.namespace, args.set, indexName);
		task.waitTillComplete();
	}
}
