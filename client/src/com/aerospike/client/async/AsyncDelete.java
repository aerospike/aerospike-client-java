/*
 * Copyright 2012-2017 Aerospike, Inc.
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
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncDelete extends AsyncCommand implements AsyncSingleCommand {
	private final DeleteListener listener;
	private final WritePolicy writePolicy;
	private final Key key;
	private boolean existed;
		
	public AsyncDelete(DeleteListener listener, WritePolicy writePolicy, Key key) {
		super(writePolicy, new Partition(key), null, false, false);
		this.listener = listener;
		this.writePolicy = writePolicy;
		this.key = key;
	}
	
	@Override
	protected void writeBuffer() {
		setDelete(writePolicy, key);
	}

	@Override
	public void parseResult() {
        if (resultCode == 0) {
        	existed = true;
        }
        else {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		existed = false;
        	}
        	else {
        		throw new AerospikeException(resultCode);
        	}
        }
	}

	@Override
	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, existed);
		}
	}

	@Override
	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
