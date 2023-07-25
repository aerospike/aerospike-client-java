/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.LatencyType;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryAggregateCommand extends MultiCommand {

	private final Statement statement;
	private final long taskId;
	private final LuaInstance instance;
	private final BlockingQueue<LuaValue> inputQueue;

	public QueryAggregateCommand(
		Cluster cluster,
		Node node,
		QueryPolicy policy,
		Statement statement,
		long taskId,
		LuaInstance instance,
		BlockingQueue<LuaValue> inputQueue,
		long clusterKey,
		boolean first
	) {
		super(cluster, policy, node, statement.namespace, clusterKey, first);
		this.statement = statement;
		this.taskId = taskId;
		this.instance = instance;
		this.inputQueue = inputQueue;
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.QUERY;
	}

	@Override
	protected final void writeBuffer() throws AerospikeException {
		setQuery(cluster, policy, statement, taskId, false, null);
	}

	@Override
	protected boolean parseRow() {
		skipKey(fieldCount);

		if (resultCode != 0) {
			// Aggregation scans (with null query filter) will return KEY_NOT_FOUND_ERROR
			// when the set does not exist on the target node.
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				// Non-fatal error.
				return false;
			}
			throw new AerospikeException(resultCode);
		}

		if (opCount != 1) {
			throw new AerospikeException("Query aggregate expected exactly one bin.  Received " + opCount);
		}

		// Parse aggregateValue.
		int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
		dataOffset += 5;
		byte particleType = dataBuffer[dataOffset];
		dataOffset += 2;
		byte nameSize = dataBuffer[dataOffset++];

		String name = Buffer.utf8ToString(dataBuffer, dataOffset, nameSize);
		dataOffset += nameSize;

		int particleBytesSize = opSize - (4 + nameSize);

		if (! name.equals("SUCCESS")) {
			if (name.equals("FAILURE")) {
				Object value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
				throw new AerospikeException(ResultCode.QUERY_GENERIC, value != null ? value.toString() : null);
			}
			else {
				throw new AerospikeException(ResultCode.PARSE_ERROR, "Query aggregate expected bin name SUCCESS.  Received " + name);
			}
		}

		LuaValue aggregateValue = instance.getLuaValue(particleType, dataBuffer, dataOffset, particleBytesSize);
		dataOffset += particleBytesSize;

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (aggregateValue != null) {
			try {
				inputQueue.put(aggregateValue);
			}
			catch (InterruptedException ie) {
			}
		}
		return true;
	}
}
