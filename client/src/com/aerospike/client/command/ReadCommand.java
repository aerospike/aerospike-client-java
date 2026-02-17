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
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

public class ReadCommand extends SyncReadCommand {
	private final String[] binNames;
	private final boolean isOperation;
	private Record record;

	public ReadCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, policy, key);
		this.binNames = null;
		this.isOperation = false;
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames) {
		super(cluster, policy, key);
		this.binNames = binNames;
		this.isOperation = false;
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, boolean isOperation) {
		super(cluster, policy, key);
		this.binNames = null;
		this.isOperation = isOperation;
	}

	@Override
	protected void writeBuffer() {
		setRead(policy, key, binNames);
	}

	@Override
	protected void parseResult(Node node, Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);
		rp.parseFields(policy.txn, key, false);
		if (node.areMetricsEnabled()) {
			node.addBytesIn(namespace, rp.bytesIn);
		}
		if (rp.resultCode == ResultCode.OK) {
			this.record = rp.parseRecord(isOperation);
			return;
		}

		if (rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			return;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			return;
		}

		throw new AerospikeException(rp.resultCode);
	}

	public Record getRecord() {
		return record;
	}
}
