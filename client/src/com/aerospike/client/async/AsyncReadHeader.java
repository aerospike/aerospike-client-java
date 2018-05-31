/*
 * Copyright 2012-2018 Aerospike, Inc.
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
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;

public final class AsyncReadHeader extends AsyncCommand {
	private final RecordListener listener;
	private final Key key;
	private Record record;
	
	public AsyncReadHeader(RecordListener listener, Policy policy, Key key) {
		super(policy, new Partition(key), null, true);
		this.listener = listener;
		this.key = key;
	}

	@Override
	protected void writeBuffer() {
		setReadHeader(policy, key);
	}

	@Override
	protected boolean parseResult() {
		validateHeaderSize();
		
		int resultCode = dataBuffer[5] & 0xFF;

		if (resultCode == 0) {
			int generation = Buffer.bytesToInt(dataBuffer, 6);
			int expiration = Buffer.bytesToInt(dataBuffer, 10);
			record = new Record(null, generation, expiration);
		}
		else {
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				record = null;
			}
			else {
				throw new AerospikeException(resultCode);
			}
		}
		return true;
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
