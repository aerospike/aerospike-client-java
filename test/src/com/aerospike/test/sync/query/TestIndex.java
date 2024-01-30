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

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestIndex extends TestSync {
	private static final String indexName = "testindex";
	private static final String binName = "testbin";

	@Test
	public void createDrop() {
		IndexTask task;

		// Drop index if it already exists.
		try {
			task = client.dropIndex(args.indexPolicy, args.namespace, args.set, indexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
		task.waitTillComplete();

		task = client.dropIndex(args.indexPolicy, args.namespace, args.set, indexName);
		task.waitTillComplete();

		// Ensure all nodes have dropped the index.
		Node[] nodes = client.getNodes();
		String cmd = IndexTask.buildStatusCommand(args.namespace, indexName);

		for (Node node : nodes) {
			String response = Info.request(node, cmd);
			assertEquals(response, "FAIL:201:no-index");
		}
	}

	@Test
	public void ctxRestore() {
		CTX[] ctx1 = new CTX[] {
			CTX.listIndex(-1),
			CTX.mapKey(Value.get("key1")),
			CTX.listValue(Value.get(937))
		};

		String base64 = CTX.toBase64(ctx1);
		CTX[] ctx2 = CTX.fromBase64(base64);

		assertEquals(ctx1.length, ctx2.length);

		for (int i = 0; i < ctx1.length; i++) {
			CTX item1 = ctx1[i];
			CTX item2 = ctx2[i];

			assertEquals(item1.id, item2.id);

			Object obj1 = item1.value.getObject();
			Object obj2 = item2.value.getObject();

			if (obj1 instanceof Integer && obj2 instanceof Long) {
				// fromBase64() converts integers to long, so consider these equivalent.
				assertEquals((long)(Integer)obj1, (long)(Long)obj2);
			}
			else {
				assertEquals(obj1, obj2);
			}
		}
	}
}
