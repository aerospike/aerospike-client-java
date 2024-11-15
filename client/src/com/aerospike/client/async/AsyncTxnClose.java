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
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncTxnClose extends AsyncWriteBase {
	private final Txn txn;
	private final DeleteListener listener;

	public AsyncTxnClose(
		Cluster cluster,
		Txn txn,
		DeleteListener listener,
		WritePolicy writePolicy,
		Key key
	) {
		super(cluster, writePolicy, key);
		this.txn = txn;
		this.listener = listener;
	}

	@Override
	protected void writeBuffer() {
		setTxnClose(txn, key);
	}

	@Override
	protected boolean parseResult() {
		int resultCode = parseHeader();

		if (resultCode == ResultCode.OK || resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			return true;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	void onInDoubt() {
	}

	@Override
	protected void onSuccess() {
		listener.onSuccess(key, true);
	}

	@Override
	protected void onFailure(AerospikeException e) {
		listener.onFailure(e);
	}
}
