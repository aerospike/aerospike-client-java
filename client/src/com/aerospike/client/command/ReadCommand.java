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
package com.aerospike.client.command;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.Policy;

public class ReadCommand extends SyncCommand {
	protected final Key key;
	protected final Partition partition;
	private final String[] binNames;
	private final boolean isOperation;
	private Record record;

	public ReadCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, policy);
		this.key = key;
		this.binNames = null;
		this.partition = Partition.read(cluster, policy, key);
		this.isOperation = false;
		cluster.addTran();
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, String[] binNames) {
		super(cluster, policy);
		this.key = key;
		this.binNames = binNames;
		this.partition = Partition.read(cluster, policy, key);
		this.isOperation = false;
		cluster.addTran();
	}

	public ReadCommand(Cluster cluster, Policy policy, Key key, Partition partition, boolean isOperation) {
		super(cluster, policy);
		this.key = key;
		this.binNames = null;
		this.partition = partition;
		this.isOperation = isOperation;
		cluster.addTran();
	}

	@Override
	protected Node getNode() {
		return partition.getNodeRead(cluster);
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.READ;
	}

	@Override
	protected void writeBuffer() {
		setRead(policy, key, binNames);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);

		if (rp.resultCode == ResultCode.OK) {
			this.record = rp.parseRecord(isOperation);
			return;
		}

		if (rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			handleNotFound(rp.resultCode);
			return;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			return;
		}

		if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
			this.record = rp.parseRecord(isOperation);
			handleUdfError(rp.resultCode);
			return;
		}

		throw new AerospikeException(rp.resultCode);
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryRead(timeout);
		return true;
	}

	protected void handleNotFound(int resultCode) {
		// Do nothing in default case. Record will be null.
	}

	private void handleUdfError(int resultCode) {
		String ret = (String)record.bins.get("FAILURE");

		if (ret == null) {
			throw new AerospikeException(resultCode);
		}

		String message;
		int code;

		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Throwable e) {
			// Use generic exception if parse error occurs.
			throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	public Record getRecord() {
		return record;
	}
}
