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
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;

public class WriteCommandProxy extends AbstractCommand {
    private final WriteListener writeListener;
    private final WritePolicy writePolicy;
    private final Key key;
    private final Bin[] bins;

    public WriteCommandProxy(
    	GrpcCallExecutor grpcCallExecutor,
    	WriteListener writeListener,
    	WritePolicy writePolicy,
    	Key key,
    	Bin[] bins
    ) {
        super(grpcCallExecutor, writePolicy);
        this.writeListener = writeListener;
        this.writePolicy = writePolicy;
        this.key = key;
        this.bins = bins;
    }

	@Override
	void writePayload() {
        serde.setWrite(writePolicy, Operation.Type.WRITE, key, bins);
	}

	@Override
	protected void parseResult(Parser parser) {
		parser.validateHeaderSize();

		int resultCode = parser.parseResultCode();

		if (resultCode == 0) {
            writeListener.onSuccess(key);
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
            writeListener.onSuccess(key);
			return;
		}

		throw new AerospikeException(resultCode);
    }

    @Override
    void allAttemptsFailed(AerospikeException exception) {
        writeListener.onFailure(exception);
    }
}
