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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.WritePolicy;
import java.io.IOException;

public final class ExecuteCommand extends SyncWriteCommand {
	private final String packageName;
	private final String functionName;
	private final Value[] args;
	private Record record;

	public ExecuteCommand(
		Cluster cluster,
		WritePolicy writePolicy,
		Key key,
		String packageName,
		String functionName,
		Value[] args
	) {
		super(cluster, writePolicy, key);
		this.packageName = packageName;
		this.functionName = functionName;
		this.args = args;
	}

	@Override
	protected void writeBuffer() {
		setUdf(writePolicy, key, packageName, functionName, args);
	}

	@Override
	protected void parseResult(Node node, Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);
		rp.parseFields(policy.txn, key, true);

		if (node.areMetricsEnabled()) {
			node.addBytesIn(namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			record = rp.parseRecord(false);
			return;
		}

		if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
			record = rp.parseRecord(false);
			handleUdfError(rp.resultCode);
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
