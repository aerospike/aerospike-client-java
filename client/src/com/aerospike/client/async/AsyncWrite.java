/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncWrite extends AsyncCommand {
	private final WriteListener listener;
	private final WritePolicy writePolicy;
	private final Key key;
	private final Partition partition;
	private final Bin[] bins;
	private final Operation.Type operation;

	public AsyncWrite(
		Cluster cluster,
		WriteListener listener,
		WritePolicy writePolicy,
		Key key,
		Bin[] bins,
		Operation.Type operation
	) {
		super(writePolicy, true);
		this.listener = listener;
		this.writePolicy = writePolicy;
		this.key = key;
		this.partition = Partition.write(cluster, writePolicy, key);
		this.bins = bins;
		this.operation = operation;
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
	protected void writeBuffer() {
		setWrite(writePolicy, operation, key, bins);
	}

	@Override
	protected boolean parseResult() {
		validateHeaderSize();

		int resultCode = dataBuffer[5] & 0xFF;

		if (resultCode == 0) {
			return true;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			return true;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	boolean prepareRetry(boolean timeout) {
		partition.prepareRetryWrite(timeout);
		return true;
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
