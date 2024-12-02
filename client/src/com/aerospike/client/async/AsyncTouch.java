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
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncTouch extends AsyncWriteBase {
	private final WriteListener writeListener;
	private final ExistsListener existsListener;
	private boolean touched;

	public AsyncTouch(Cluster cluster, WriteListener listener, WritePolicy writePolicy, Key key) {
		super(cluster, writePolicy, key);
		this.writeListener = listener;
		this.existsListener = null;
	}

	public AsyncTouch(Cluster cluster, ExistsListener listener, WritePolicy writePolicy, Key key) {
		super(cluster, writePolicy, key);
		this.writeListener = null;
		this.existsListener = listener;
	}

	@Override
	protected void writeBuffer() {
		setTouch(writePolicy, key);
	}

	@Override
	protected boolean parseResult() {
		int resultCode = parseHeader();

		if (resultCode == ResultCode.OK) {
			touched = true;
			return true;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			if (existsListener == null) {
				throw new AerospikeException(resultCode);
			}
			touched = false;
			return true;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			touched = false;
			return true;
		}

		throw new AerospikeException(resultCode);
	}

	@Override
	protected void onSuccess() {
		if (writeListener != null) {
			writeListener.onSuccess(key);
		}
		else if (existsListener != null) {
			existsListener.onSuccess(key, touched);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (writeListener != null) {
			writeListener.onFailure(e);
		}
		else if (existsListener != null) {
			existsListener.onFailure(e);
		}
	}
}
