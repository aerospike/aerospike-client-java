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
 * Async callback for transaction commit; receives {@link com.aerospike.client.CommitStatus} or exception.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#commit}
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.commit(loop, new CommitListener() {
 *   public void onSuccess(CommitStatus status) { }
 *   public void onFailure(AerospikeException.Commit e) { }
 * }, new CommitPolicy(), txn);
 * }</pre>
 *
 * @see com.aerospike.client.CommitStatus
 * @see com.aerospike.client.IAerospikeClient#commit
 * @see com.aerospike.client.Txn
 */
public interface CommitListener {
	/**
	 * Called when the commit completes (verified or will succeed).
	 * @param status commit status (must not be null)
	 */
	void onSuccess(CommitStatus status);

	/**
	 * Called when the commit fails.
	 * @param ae commit exception cause of failure wrapped into Aerospike exception
	 */
	void onFailure(AerospikeException.Commit ae);
}
