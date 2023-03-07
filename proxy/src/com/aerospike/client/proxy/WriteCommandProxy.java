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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;

public final class WriteCommandProxy extends CommandProxy {
	private final WriteListener listener;
	private final WritePolicy writePolicy;
	private final Key key;
	private final Bin[] bins;
	private final Operation.Type type;

	public WriteCommandProxy(
		GrpcCallExecutor executor,
		WriteListener listener,
		WritePolicy writePolicy,
		Key key,
		Bin[] bins,
		Operation.Type type
	) {
		super(KVSGrpc.getWriteStreamingMethod(), executor, writePolicy);
		this.listener = listener;
		this.writePolicy = writePolicy;
		this.key = key;
		this.bins = bins;
		this.type = type;
	}

	@Override
	void writeCommand(Command command) {
		command.setWrite(writePolicy, type, key, bins);
	}

	@Override
	void parseResult(Parser parser) {
		int resultCode = parser.parseResultCode();

		switch (resultCode) {
			case ResultCode.OK:
				break;

			case ResultCode.FILTERED_OUT:
				if (policy.failOnFilteredOut) {
					throw new AerospikeException(resultCode);
				}
				break;

			default:
				throw new AerospikeException(resultCode);
		}

		try {
			listener.onSuccess(key);
		}
		catch (Throwable t) {
			logOnSuccessError(t);
		}
	}

	@Override
	void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}
}
