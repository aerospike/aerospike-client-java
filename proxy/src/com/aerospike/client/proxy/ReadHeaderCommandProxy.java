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
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.KVSGrpc;

public final class ReadHeaderCommandProxy extends SingleCommandProxy {
	private final RecordListener listener;
	private final Key key;

	public ReadHeaderCommandProxy(
		GrpcCallExecutor executor,
		RecordListener listener,
		Policy policy,
		Key key
	) {
		super(KVSGrpc.getGetHeaderStreamingMethod(), executor, policy);
		this.listener = listener;
		this.key = key;
	}

	@Override
	void writeCommand(Command command) {
		command.setReadHeader(policy, key);
	}

	@Override
	void parseResult(Parser parser) {
		Record record = null;
		int resultCode = parser.parseHeader();

		switch (resultCode) {
			case ResultCode.OK:
				record = new Record(null, parser.generation, parser.expiration);
				break;

			case ResultCode.KEY_NOT_FOUND_ERROR:
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
			listener.onSuccess(key, record);
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
