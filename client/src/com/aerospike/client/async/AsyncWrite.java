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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncWrite extends AsyncWriteBase {
	private final WriteListener listener;
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
		super(cluster, writePolicy, key);
		this.listener = listener;
		this.bins = bins;
		this.operation = operation;
	}

	@Override
	protected void writeBuffer() {
		setWrite(writePolicy, operation, key, bins);
	}

	@Override
	protected boolean parseResult() {
		int resultCode = parseHeader();

		if (resultCode == ResultCode.OK) {
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
