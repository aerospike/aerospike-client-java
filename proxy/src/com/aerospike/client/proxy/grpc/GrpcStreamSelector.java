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
package com.aerospike.client.proxy.grpc;

import java.util.List;

import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;

/**
 * A selector of streams within a channel to execute Aerospike proxy gRPC calls.
 */
public interface GrpcStreamSelector {
	/**
	 * Select a stream for the gRPC method. All streams created by the
	 * selector should be close when the selector is closed.
	 *
	 * @param streams          streams to select from.
	 * @param methodDescriptor the method description of the request.
	 * @return the selected stream, <code>null</code> when no stream is
	 * selected.
	 */
	GrpcStream select(
		List<GrpcStream> streams,
		MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor
	);
}
