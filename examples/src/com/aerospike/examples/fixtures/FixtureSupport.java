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
package com.aerospike.examples.fixtures;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.examples.Parameters;

final class FixtureSupport {
	private FixtureSupport() {
	}

	static Key key(Parameters params, Object userKey) {
		return key(params, params.set(), userKey);
	}

	static Key key(Parameters params, String setName, Object userKey) {
		return new Key(params.namespace(), setName, Value.get(userKey));
	}

	static void deleteKeys(IAerospikeClient client, Parameters params, Object... userKeys) {
		deleteKeys(client, params, params.set(), userKeys);
	}

	static void deleteKeys(IAerospikeClient client, Parameters params, String setName, Object... userKeys) {
		for (Object userKey : userKeys) {
			client.delete(params.writePolicy(), key(params, setName, userKey));
		}
	}

	static void deleteKeyRange(
		IAerospikeClient client,
		Parameters params,
		String keyPrefix,
		int begin,
		int end
	) {
		deleteKeyRange(client, params, params.set(), keyPrefix, begin, end);
	}

	static void deleteKeyRange(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String keyPrefix,
		int begin,
		int end
	) {
		for (int i = begin; i <= end; i++) {
			client.delete(params.writePolicy(), key(params, setName, keyPrefix + i));
		}
	}

	static void deleteNumericKeyRange(
		IAerospikeClient client,
		Parameters params,
		String setName,
		int begin,
		int end
	) {
		for (int i = begin; i <= end; i++) {
			client.delete(params.writePolicy(), key(params, setName, i));
		}
	}

	static void seedIntegerRange(
		IAerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) {
		seedIntegerRange(client, params, params.set(), keyPrefix, binName, size);
	}

	static void seedIntegerRange(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String keyPrefix,
		String binName,
		int size
	) {
		for (int i = 1; i <= size; i++) {
			client.put(params.writePolicy(), key(params, setName, keyPrefix + i), new com.aerospike.client.Bin(binName, i));
		}
	}

	static void seedNumericKeys(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String binName,
		int begin,
		int end
	) {
		for (int i = begin; i <= end; i++) {
			client.put(params.writePolicy(), key(params, setName, i), new com.aerospike.client.Bin(binName, i));
		}
	}

	static void createIndexIfMissing(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String indexName,
		String binName,
		IndexType indexType
	) throws Exception {
		Policy policy = new Policy();
		policy.socketTimeout = 0;

		try {
			IndexTask task = client.createIndex(policy, params.namespace(), setName, indexName, binName, indexType);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	static void createIndexIfMissing(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX... ctx
	) throws Exception {
		Policy policy = new Policy();
		policy.socketTimeout = 0;

		try {
			IndexTask task = client.createIndex(
				policy,
				params.namespace(),
				setName,
				indexName,
				binName,
				indexType,
				indexCollectionType,
				ctx);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	static void dropIndexIfExists(IAerospikeClient client, Parameters params, String indexName) throws Exception {
		dropIndexIfExists(client, params, params.set(), indexName);
	}

	static void dropIndexIfExists(IAerospikeClient client, Parameters params, String setName, String indexName) throws Exception {
		try {
			IndexTask task = client.dropIndex(params.readPolicy(), params.namespace(), setName, indexName);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}
	}

	static void registerLua(IAerospikeClient client, Parameters params, String packagePath, String serverFile) throws Exception {
		RegisterTask task = client.register(params.readPolicy(), packagePath, serverFile, Language.LUA);
		task.waitTillComplete();
	}
}
