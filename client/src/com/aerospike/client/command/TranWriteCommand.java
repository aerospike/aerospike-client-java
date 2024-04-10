/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.tran.Tran;
import com.aerospike.client.tran.TranOp;
import java.io.IOException;

public final class TranWriteCommand extends SyncCommand {
	private final WritePolicy writePolicy;
	private final Key key;
	private final Tran tran;
	private final TranOp tranOp;
	private final Partition partition;

	public TranWriteCommand(Cluster cluster, WritePolicy writePolicy, Key key, Tran tran, TranOp tranOp) {
		super(cluster, writePolicy);
		this.writePolicy = writePolicy;
		this.key = key;
		this.tran = tran;
		this.tranOp = tranOp;
		this.partition = Partition.write(cluster, writePolicy, key);
		cluster.addTran();
	}

	@Override
	protected boolean isWrite() {
		return true;
	}

	@Override
	protected Node getNode() {
		return partition.getNodeWrite(cluster);
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.WRITE;
	}

	@Override
	protected void writeBuffer() {
		setTranWrite(writePolicy, key, tran, tranOp);
	}

	@Override
	protected void parseResult(Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);
		Record record = rp.parseRecord(false);

		// TODO: Is this necessary?
		if (record.version != null) {
			tran.addRead(key, record.version);
			tran.removeWrite(key);
		}
		else {
			if (rp.resultCode == ResultCode.OK) {
				tran.removeRead(key);
			}
			else {
				tran.removeWrite(key);
			}
		}

		int resultCode = rp.resultCode;

		if (resultCode == ResultCode.OK) {
			return;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryWrite(timeout);
		return true;
	}
}
