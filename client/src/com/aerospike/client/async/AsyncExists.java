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
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.policy.Policy;

public final class AsyncExists extends AsyncReadBase {
	private final ExistsListener listener;
	private boolean exists;

	public AsyncExists(Cluster cluster, ExistsListener listener, Policy policy, Key key) {
		super(cluster, policy, key);
		this.listener = listener;
	}

	@Override
	protected void writeBuffer() {
		setExists(policy, key);
	}

	@Override
	protected boolean parseResult() {
		RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);
		rp.parseFields(policy.txn, key, false);

		if (rp.resultCode == ResultCode.OK) {
			exists = true;
			return true;
		}

		if (rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			exists = false;
			return true;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			exists = true;
			return true;
		}

		throw new AerospikeException(rp.resultCode);
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, exists);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
