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
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.WritePolicy;
import java.io.IOException;

public abstract class SyncWriteCommand extends SyncCommand {
	final WritePolicy writePolicy;
	final Key key;
	final Partition partition;

	public SyncWriteCommand(Cluster cluster, WritePolicy writePolicy, Key key) {
		super(cluster, writePolicy);
		this.writePolicy = writePolicy;
		this.key = key;
		System.out.println(Thread.currentThread().threadId() + " - in SyncWriteCommand ctor, key.ns=" + key.namespace);
		// System.out.println("in SyncWriteCommand ctor, wp.tran.ns=" + writePolicy.tran.getNamespace());
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
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryWrite(timeout);
		return true;
	}

	protected int parseHeader(Connection conn) throws IOException {
		RecordParser rp = new RecordParser(conn, dataBuffer);
		rp.parseFields(policy.tran, key, true);

		if (rp.opCount > 0) {
			throw new AerospikeException("Unexpected write response opCount: " + rp.opCount + ',' + rp.resultCode);
		}
		return rp.resultCode;
	}
}
