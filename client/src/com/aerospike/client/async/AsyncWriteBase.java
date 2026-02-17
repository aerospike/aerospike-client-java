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
package com.aerospike.client.async;

import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.WritePolicy;

public abstract class AsyncWriteBase extends AsyncCommand {
	final WritePolicy writePolicy;
	final Key key;
	final Partition partition;

	public AsyncWriteBase(Cluster cluster, WritePolicy writePolicy, Key key) {
		super(writePolicy, true, key.namespace);
		this.writePolicy = writePolicy;
		this.key = key;
		this.partition = Partition.write(cluster, writePolicy, key);
		cluster.addCommandCount();
	}

	@Override
	boolean isWrite() {
		return true;
	}

	@Override
	Node getNode(Cluster cluster) {
		return partition.getNodeWrite(cluster);
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.WRITE;
	}

	@Override
	boolean prepareRetry(boolean timeout) {
		partition.prepareRetryWrite(timeout);
		return true;
	}

	@Override
	void onInDoubt() {
		if (writePolicy.txn != null) {
			writePolicy.txn.onWriteInDoubt(key);
		}
	}

	protected int parseHeader() {
		RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);
		rp.parseFields(policy.txn, key, true);
		return rp.resultCode;
	}
}
