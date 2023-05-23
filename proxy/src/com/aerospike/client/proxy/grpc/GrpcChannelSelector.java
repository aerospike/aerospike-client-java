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

/**
 * A selector of channels to execute Aerospike proxy gRPC calls.
 */
public interface GrpcChannelSelector {
	/**
	 * Select a channel for the gRPC method.
	 *
	 * @param channels channels to select from.
	 * @param call     the streaming call to be executed.
	 * @return the selected channel.
	 */
	GrpcChannelExecutor select(List<GrpcChannelExecutor> channels, GrpcStreamingCall call);
}
