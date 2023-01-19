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

public class ReadHeaderCommandProxy extends AbstractCommand {
    private final Key key;
    private final RecordListener recordListener;

    public ReadHeaderCommandProxy(GrpcCallExecutor grpcCallExecutor, Policy policy, Key key, RecordListener recordListener) {
        super(grpcCallExecutor, policy);
        this.key = key;
        this.recordListener = recordListener;
    }

	@Override
	void writePayload() {
		serde.setReadHeader(policy, key);
	}

	@Override
	protected void parseResult(Parser parser) {
		parser.validateHeaderSize();

		int resultCode = parser.parseHeader();

		if (resultCode == 0) {
			Record record = new Record(null, parser.generation, parser.expiration);
			recordListener.onSuccess(key, record);
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			recordListener.onSuccess(key, null);
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
			recordListener.onSuccess(key, null);
			return;
		}

		throw new AerospikeException(resultCode);
	}

    @Override
    void allAttemptsFailed(AerospikeException exception) {
        recordListener.onFailure(exception);
    }
}