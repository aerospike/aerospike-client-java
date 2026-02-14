/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.listener;

import com.aerospike.client.AbortStatus;

/**
 * Async callback for transaction abort; receives {@link com.aerospike.client.AbortStatus} on success.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#abort(com.aerospike.client.async.EventLoop, AbortListener, com.aerospike.client.Txn)}. Abort does not report failure via a separate callback.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.abort(loop, new AbortListener() {
 *   public void onSuccess(AbortStatus status) { }
 * }, txn);
 * }</pre>
 *
 * @see com.aerospike.client.AbortStatus
 * @see com.aerospike.client.IAerospikeClient#abort
 * @see com.aerospike.client.Txn
 */
public interface AbortListener {
	/**
	 * Called when the abort completes successfully.
	 * @param status abort status (must not be null)
	 */
	void onSuccess(AbortStatus status);
}
