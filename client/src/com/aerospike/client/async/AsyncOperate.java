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
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.listener.RecordListener;

public final class AsyncOperate extends AsyncRead {
	private final OperateArgs args;

	public AsyncOperate(RecordListener listener, Key key, OperateArgs args) {
		super(listener, args.writePolicy, key, args.partition, true);
		this.args = args;
	}

	@Override
	boolean isWrite() {
		return args.hasWrite;
	}

	@Override
	protected Node getNode(Cluster cluster) {
		return args.hasWrite ? partition.getNodeWrite(cluster) : partition.getNodeRead(cluster);
	}

	@Override
	protected void writeBuffer() {
		setOperate(args.writePolicy, key, args);
	}

	@Override
	protected void handleNotFound(int resultCode) {
		// Only throw not found exception for command with write operations.
		// Read-only command operations return a null record.
		if (args.hasWrite) {
			throw new AerospikeException(resultCode);
		}
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		if (args.hasWrite) {
			partition.prepareRetryWrite(timeout);
		}
		else {
			partition.prepareRetryRead(timeout);
		}
		return true;
	}
}
