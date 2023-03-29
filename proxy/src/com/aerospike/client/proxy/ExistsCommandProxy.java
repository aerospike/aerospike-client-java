/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.proxy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;

public final class ExistsCommandProxy extends CommandProxy {
	private final ExistsListener listener;
	private final Key key;

	public ExistsCommandProxy(
		GrpcCallExecutor executor,
		ExistsListener listener,
		Policy policy,
		Key key
	) {
		super(KVSGrpc.getExistsStreamingMethod(), executor, policy);
		this.listener = listener;
		this.key = key;
	}

	@Override
	protected void writeCommand(Command command) {
		command.setExists(policy, key);
	}

	@Override
	protected void parseResult(Parser parser) {
		int resultCode = parser.parseResultCode();
		boolean exists;

		switch (resultCode) {
			case ResultCode.OK:
				exists = true;
				break;

			case ResultCode.KEY_NOT_FOUND_ERROR:
				exists = false;
				break;

			case ResultCode.FILTERED_OUT:
				if (policy.failOnFilteredOut) {
					throw new AerospikeException(resultCode);
				}
				exists = true;
				break;

			default:
				throw new AerospikeException(resultCode);
		}

		try {
			listener.onSuccess(key, exists);
		}
		catch (Throwable t) {
			logOnSuccessError(t);
		}
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}
}
