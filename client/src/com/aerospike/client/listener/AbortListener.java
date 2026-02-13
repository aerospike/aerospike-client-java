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
 * Asynchronous result notifications for transaction aborts.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#abort(com.aerospike.client.async.EventLoop, AbortListener, com.aerospike.client.Txn) abort(EventLoop, AbortListener, ...)}.
 * <p>Implement and pass to async abort to receive AbortStatus on success.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.abort(eventLoop, new AbortListener() {
 *   public void onSuccess(AbortStatus status) { // done
 *   }
 * }, txn);
 * }</pre>
 */
public interface AbortListener {
	/**
	 * This method is called when the abort succeeded or will succeed.
	 */
	void onSuccess(AbortStatus status);
}
