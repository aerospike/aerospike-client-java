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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.listener.RecordListener;

public final class AsyncTxnAddKeys extends AsyncWriteBase {
	private final RecordListener listener;
	private final OperateArgs args;
	private final Txn txn;

	public AsyncTxnAddKeys(Cluster cluster, RecordListener listener, Key key, OperateArgs args, Txn txn) {
		super(cluster, args.writePolicy, key);
		this.listener = listener;
		this.args = args;
		this.txn = txn;
	}

	@Override
	protected void writeBuffer() {
		setTxnAddKeys(args.writePolicy, key, args);
	}

	@Override
	protected boolean parseResult() {
		RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);
		rp.parseTranDeadline(txn);

		if (rp.resultCode == ResultCode.OK) {
			return true;
		}

		throw new AerospikeException(rp.resultCode);
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, null);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
