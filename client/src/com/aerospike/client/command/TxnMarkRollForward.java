/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.command;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.WritePolicy;

public final class TxnMarkRollForward extends SyncWriteCommand {

	public TxnMarkRollForward(Cluster cluster, WritePolicy writePolicy, Key key) {
		super(cluster, writePolicy, key);
	}

	@Override
	protected void writeBuffer() {
		setTxnMarkRollForward(key);
	}

	@Override
	protected void parseResult(Node node, Connection conn) throws IOException {
		int resultCode = parseHeader(node, conn);

		// MRT_COMMITTED is considered a success because it means a previous attempt already
		// succeeded in notifying the server that the transaction will be rolled forward.
		if (resultCode == ResultCode.OK || resultCode == ResultCode.MRT_COMMITTED) {
			return;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	protected void onInDoubt() {
	}
}
