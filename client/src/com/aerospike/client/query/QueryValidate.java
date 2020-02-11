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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;

public final class QueryValidate {

	public static long validateBegin(Node node, String namespace) {
		if (! node.hasClusterStable()) {
			return 0;
		}

		// Fail when cluster is in migration.
		String result = Info.request(node, "cluster-stable:namespace=" + namespace);

		try {
			return Long.parseLong(result, 16);
		}
		catch (Exception e) {
			// Yes, even scans return QUERY_ABORTED.
			throw new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result);
		}
	}

	public static void validate(Node node, String namespace, long expectedKey) {
		if (expectedKey == 0 || ! node.hasClusterStable()) {
			return;
		}

		// Fail when cluster is in migration.
		long clusterKey = validateBegin(node, namespace);

		if (clusterKey != expectedKey) {
			throw new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + expectedKey + ' ' + clusterKey);
		}
	}
}
