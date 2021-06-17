/*
 * Copyright 2012-2021 Aerospike, Inc.
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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryRecordCommand extends MultiCommand {

	private final Statement statement;
	private final RecordSet recordSet;

	public QueryRecordCommand(
		Cluster cluster,
		Node node,
		QueryPolicy policy,
		Statement statement,
		RecordSet recordSet,
		long clusterKey,
		boolean first
	) {
		super(cluster, policy, node, statement.namespace, clusterKey, first);
		this.statement = statement;
		this.recordSet = recordSet;
		deserializeKeys = true;
	}

	@Override
	protected final void writeBuffer() {
		setQuery(policy, statement, false, null);
	}

	@Override
	protected void parseRow(Key key) {
		Record record = parseRecord();

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (! recordSet.put(new KeyRecord(key, record))) {
			stop();
			throw new AerospikeException.QueryTerminated();
		}
	}
}
