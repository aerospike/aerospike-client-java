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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.CommitStatus;

/**
 * Asynchronous result notifications for transaction commits.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#commit(com.aerospike.client.async.EventLoop, CommitListener, com.aerospike.client.policy.CommitPolicy, com.aerospike.client.Txn) commit(EventLoop, CommitListener, ...)}.
 * <p>Implement and pass to async commit to receive CommitStatus on success or Commit exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.commit(eventLoop, new CommitListener() {
 *   public void onSuccess(CommitStatus status) { // use status
 *   }
 *   public void onFailure(AerospikeException.Commit ae) { ae.printStackTrace(); }
 * }, null, txn);
 * }</pre>
 */
public interface CommitListener {
	/**
	 * This method is called when the records are verified and the commit succeeded or will succeed.
	 */
	void onSuccess(CommitStatus status);

	/**
	 * This method is called when the commit fails.
	 */
	void onFailure(AerospikeException.Commit ae);
}
