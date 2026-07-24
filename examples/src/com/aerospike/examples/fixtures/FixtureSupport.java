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

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Version;
import com.aerospike.examples.ExampleSkipException;
import com.aerospike.examples.Parameters;

final class FixtureSupport {
	static final class ServerCapabilities {
		final boolean enterprise;
		final boolean hasTtl;
		final boolean strongConsistency;
		final Version serverVersion;

		private ServerCapabilities(
			boolean enterprise,
			boolean hasTtl,
			boolean strongConsistency,
			Version serverVersion
		) {
			this.enterprise = enterprise;
			this.hasTtl = hasTtl;
			this.strongConsistency = strongConsistency;
			this.serverVersion = serverVersion;
		}
	}

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

	static ServerCapabilities capabilities(IAerospikeClient client, Parameters params) {
		Partitions partitions = client.getCluster().partitionMap.get(params.namespace());
		boolean strongConsistency = partitions != null && partitions.scMode;
		Node node = client.getNodes()[0];
		Version serverVersion = node.getServerVersion();
		String editionFilter = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "release" : "edition";
		String namespaceFilter = "namespace/" + params.namespace();
		Map<String,String> info = Info.request(null, node, editionFilter, namespaceFilter);

		String editionToken = info.get(editionFilter);

		if (editionToken == null) {
			throw new IllegalStateException("Failed to get server edition for " + params.namespace());
		}

		String namespaceTokens = info.get(namespaceFilter);

		if (namespaceTokens == null) {
			throw new IllegalStateException("Failed to get namespace info for " + params.namespace());
		}

		int nsup = parseInt(namespaceTokens, "nsup-period");
		boolean hasTtl = nsup == 0 ? parseBoolean(namespaceTokens, "allow-ttl-without-nsup") : true;
		boolean enterprise = editionToken.equals("Aerospike Enterprise Edition") || editionToken.contains("Enterprise");
		return new ServerCapabilities(enterprise, hasTtl, strongConsistency, serverVersion);
	}

	static void requireTtl(ServerCapabilities capabilities, String exampleName) throws ExampleSkipException {
		if (! capabilities.hasTtl) {
			throw new ExampleSkipException(exampleName + " requires TTL support in the target namespace");
		}
	}

	static void requireTransactions(ServerCapabilities capabilities, String exampleName) throws ExampleSkipException {
		if (! capabilities.enterprise) {
			throw new ExampleSkipException(exampleName + " requires Aerospike Enterprise Edition");
		}

		if (! capabilities.strongConsistency) {
			throw new ExampleSkipException(exampleName + " requires a strong-consistency namespace");
		}
	}

	static void requireServerAtLeast(
		ServerCapabilities capabilities,
		int major,
		int minor,
		int patch,
		String featureName
	) throws ExampleSkipException {
		Version minimum = new Version(major, minor, patch, 0);

		if (! capabilities.serverVersion.isGreaterOrEqual(minimum)) {
			throw new ExampleSkipException(String.format(
				"%s requires server version %d.%d.%d or later",
				featureName,
				major,
				minor,
				patch));
		}
	}

	private static int parseInt(String namespaceTokens, String name) {
		return Integer.parseInt(parseString(namespaceTokens, name));
	}

	private static boolean parseBoolean(String namespaceTokens, String name) {
		return Boolean.parseBoolean(parseString(namespaceTokens, name));
	}

	private static String parseString(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			throw new IllegalStateException("Failed to find namespace config token: " + name);
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}
		return namespaceTokens.substring(begin, end);
	}
}
