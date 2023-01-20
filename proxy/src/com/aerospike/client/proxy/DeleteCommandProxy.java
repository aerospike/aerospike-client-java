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
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;

public final class DeleteCommandProxy extends AbstractCommand {
    private final DeleteListener listener;
    private final WritePolicy writePolicy;
    private final Key key;

    public DeleteCommandProxy(
    	GrpcCallExecutor grpcCallExecutor,
    	DeleteListener listener,
    	WritePolicy writePolicy,
    	Key key
    ) {
        super(grpcCallExecutor, writePolicy);
        this.listener = listener;
        this.writePolicy = writePolicy;
        this.key = key;
    }

	@Override
	void writePayload() {
        serde.setDelete(writePolicy, key);
	}

	@Override
	protected void parseResult(Parser parser) {
		parser.validateHeaderSize();

		int resultCode = parser.parseResultCode();

		if (resultCode == 0) {
            listener.onSuccess(key, true);
			return;
		}

		if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            listener.onSuccess(key, false);
			return;
		}

		if (resultCode == ResultCode.FILTERED_OUT) {
			if (policy.failOnFilteredOut) {
				throw new AerospikeException(resultCode);
			}
            listener.onSuccess(key, true);
			return;
		}

		throw new AerospikeException(resultCode);
    }

    @Override
    void allAttemptsFailed(AerospikeException exception) {
        listener.onFailure(exception);
    }
}
