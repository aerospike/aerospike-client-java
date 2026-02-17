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
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;

public final class TxnAddKeys extends SyncWriteCommand {
	private final OperateArgs args;
	private final Txn txn;

	public TxnAddKeys(Cluster cluster, Key key, OperateArgs args, Txn txn) {
		super(cluster, args.writePolicy, key);
		this.args = args;
		this.txn = txn;
	}

	@Override
	protected void writeBuffer() {
		setTxnAddKeys(args.writePolicy, key, args);
	}

	@Override
	protected void parseResult(Node node, Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);
		rp.parseTranDeadline(txn);
		if (node.areMetricsEnabled()) {
			node.addBytesIn(namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			return;
		}

		throw new AerospikeException(rp.resultCode);
	}
}
