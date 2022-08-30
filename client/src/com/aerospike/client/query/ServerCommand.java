/*
 * Copyright 2012-2022 Aerospike, Inc.
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
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.WritePolicy;

public final class ServerCommand extends MultiCommand {
	private final Statement statement;
	private final long taskId;

	public ServerCommand(Cluster cluster, Node node, WritePolicy writePolicy, Statement statement, long taskId) {
		super(cluster, writePolicy, node, false);
		this.statement = statement;
		this.taskId = taskId;
	}

	@Override
	protected boolean isWrite() {
		return true;
	}

	@Override
	protected final void writeBuffer() {
		setQuery(cluster, policy, statement, taskId, true, null);
	}

	@Override
	protected boolean parseRow() {
		skipKey(fieldCount);

		// Server commands (Query/Execute UDF) should only send back a return code.
		if (resultCode != 0) {
			// Background scans (with null query filter) return KEY_NOT_FOUND_ERROR
			// when the set does not exist on the target node.
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				// Non-fatal error.
				return false;
			}
			throw new AerospikeException(resultCode);
		}

		if (opCount > 0) {
			throw new AerospikeException.Parse("Unexpectedly received bins on background query!");
		}

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}
		return true;
	}
}
