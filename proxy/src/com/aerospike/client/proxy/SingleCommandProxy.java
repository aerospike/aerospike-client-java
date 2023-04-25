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
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;

public abstract class SingleCommandProxy extends CommandProxy {

	public SingleCommandProxy(
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor,
		GrpcCallExecutor executor,
		Policy policy
	) {
		super(methodDescriptor, executor, policy);
	}

	void onResponse(Kvs.AerospikeResponsePayload response) {
		// Check response status for client errors (negative error codes).
		// Server errors are checked in response payload in Parser.
		int status = response.getStatus();

		if (status != 0) {
			notifyFailure(new AerospikeException(status));
			return;
		}

		byte[] bytes = response.getPayload().toByteArray();
		Parser parser = new Parser(bytes);
		parser.parseProto();
		parseResult(parser);
	}

	@Override
	boolean isSingle() {
		return true;
	}

	abstract void parseResult(Parser parser);
}
