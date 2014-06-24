/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import com.aerospike.client.Operation;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncOperate extends AsyncRead {
	private final WritePolicy policy;
	private final Operation[] operations;
	
	public AsyncOperate(AsyncCluster cluster, WritePolicy policy, RecordListener listener, Key key, Operation[] operations) {
		super(cluster, policy, listener, key, null);
		this.policy = (policy == null) ? new WritePolicy() : policy;
		this.operations = operations;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setOperate(policy, key, operations);
	}
}
