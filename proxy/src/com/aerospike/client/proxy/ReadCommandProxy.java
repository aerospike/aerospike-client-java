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
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;

public class ReadCommandProxy extends AbstractCommand {
	private final RecordListener listener;
	final Key key;
	private final String[] binNames;
	private final boolean isOperation;

	public ReadCommandProxy(
		GrpcCallExecutor grpcCallExecutor,
		RecordListener listener,
		Policy policy,
		Key key,
		String[] binNames
	) {
		super(grpcCallExecutor, policy);
		this.listener = listener;
		this.key = key;
		this.binNames = binNames;
		this.isOperation = false;
	}

	public ReadCommandProxy(
		GrpcCallExecutor grpcCallExecutor,
		RecordListener listener,
		Policy policy,
		Key key,
		boolean isOperation
	) {
		super(grpcCallExecutor, policy);
		this.listener = listener;
		this.key = key;
		this.binNames = null;
		this.isOperation = isOperation;
	}

	@Override
	void writePayload() {
        serde.setRead(policy, key, binNames);
	}

	@Override
	protected void parseResult(Parser parser) {
		int resultCode = parser.parseHeader();

		if (resultCode == 0) {
			if (parser.opCount == 0) {
				// Bin data was not returned.
				Record record = new Record(null, parser.generation, parser.expiration);
				listener.onSuccess(key, record);
				return;
			}
			parser.skipKey();
			Record record = parser.parseRecord(isOperation);
			listener.onSuccess(key, record);
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			handleNotFound(resultCode);
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			listener.onSuccess(key, null);
			return;
		}

		if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
			parser.skipKey();
			Record record = parser.parseRecord(isOperation);
			handleUdfError(record, resultCode);
			return;
		}

		throw new AerospikeException(resultCode);
    }

	protected void handleNotFound(int resultCode) {
		// Do nothing in default case. Record will be null.
	}

	private void handleUdfError(Record record, int resultCode) {
		String ret = (String)record.bins.get("FAILURE");

		if (ret == null) {
			throw new AerospikeException(resultCode);
		}

		String message;
		int code;

		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Exception e) {
			// Use generic exception if parse error occurs.
			throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	@Override
    void allAttemptsFailed(AerospikeException exception) {
        listener.onFailure(exception);
    }
}