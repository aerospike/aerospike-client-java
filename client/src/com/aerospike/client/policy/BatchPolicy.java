/*
 * Copyright 2012-2022 Aerospike, Inc.
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
package com.aerospike.client.policy;

/**
 * Batch parent policy.
 */
public final class BatchPolicy extends Policy {
	/**
	 * Maximum number of concurrent synchronous batch request threads to server nodes at any point in time.
	 * If there are 16 nodes requested and maxConcurrentThreads is 8, then batch requests will be
	 * made for 8 nodes in parallel threads. When maxConcurrentThreads is set to 8, but there are 4 nodes, 
	 * we will only have 4 threads. For larger batch transactions that would benefit from multiple parallel threads,
	 * a possible starting point could be the number of CPU cores available on the client host. The ideal value would depend on 
	 * a variety of attributes, including the size and nature of the batch transaction, as well as, the overall
	 * system's performance. 
	 * <p>
	 * Values:
	 * <ul>
	 * <li>
	 * 1: Issue batch requests sequentially.  This mode has a performance advantage for small
	 * to medium sized batch sizes because requests can be issued in the main transaction thread.
	 * This is the default.
	 * </li>
	 * <li>
	 * 0: Issue all batch requests in parallel threads.  This mode has a performance
	 * advantage for extremely large batch sizes because each node can process the request
	 * immediately.  The downside is extra threads will need to be created (or taken from
	 * a thread pool).
	 * </li>
	 * <li>
	 * &gt; 0: Issue up to maxConcurrentThreads batch requests in parallel threads.  When a request
	 * completes, a new request will be issued until all requests are complete.  This mode
	 * prevents too many parallel threads being created for large cluster implementations.
	 * The downside is extra threads will still need to be created (or taken from a thread pool).
	 * </li>
	 * </ul>
	 * Asynchronous batch requests ignore this field and always issue all node requests in parallel.
	 * <p>
	 * Default: 1
	 */
	public int maxConcurrentThreads = 1;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for in-memory
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * <p>
	 * For batch transactions with smaller sized records (&lt;= 1K per record), inline
	 * processing will be significantly faster on in-memory namespaces.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: true
	 */
	public boolean allowInline = true;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for SSD
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * Server versions &lt; 6.0 ignore this field.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: false
	 */
	public boolean allowInlineSSD = false;

	/**
	 * Should all batch keys be attempted regardless of errors. This field is used on both
	 * the client and server. The client handles node specific errors and the server handles
	 * key specific errors.
	 * <p>
	 * If true, every batch key is attempted regardless of previous key specific errors.
	 * Node specific errors such as timeouts stop keys to that node, but keys directed at
	 * other nodes will continue to be processed.
	 * <p>
	 * If false, the server will stop the batch to its node on most key specific errors.
	 * The exceptions are {@link com.aerospike.client.ResultCode#KEY_NOT_FOUND_ERROR} and
	 * {@link com.aerospike.client.ResultCode#FILTERED_OUT} which never stop the batch.
	 * The client will stop the entire batch on node specific errors for sync commands
	 * that are run in sequence (maxConcurrentThreads == 1). The client will not stop
	 * the entire batch for async commands or sync commands run in parallel.
	 * <p>
	 * Server versions &lt; 6.0 do not support this field and treat this value as false
	 * for key specific errors.
	 * <p>
	 * Default: true
	 */
	public boolean respondAllKeys = true;

	/**
	 * This method is deprecated and will eventually be removed.
	 * The set name is now always sent for every distinct namespace/set in the batch.
	 * <p>
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is necessary for batch writes and batch reads when authentication is enabled and
	 * security roles are defined on a per set basis.
	 * <p>
	 * Default: false
	 */
	@Deprecated
	public boolean sendSetName;

	/**
	 * Copy batch policy from another batch policy.
	 */
	public BatchPolicy(BatchPolicy other) {
		super(other);
		this.maxConcurrentThreads = other.maxConcurrentThreads;
		this.allowInline = other.allowInline;
		this.allowInlineSSD = other.allowInlineSSD;
		this.respondAllKeys = other.respondAllKeys;
		this.sendSetName = other.sendSetName;
	}

	/**
	 * Copy batch policy from another policy.
	 */
	public BatchPolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public BatchPolicy() {
	}

	/**
	 * Default batch read policy.
	 */
	public static BatchPolicy ReadDefault() {
		return new BatchPolicy();
	}

	/**
	 * Default batch write policy.
	 */
	public static BatchPolicy WriteDefault() {
		BatchPolicy policy = new BatchPolicy();
		policy.maxRetries = 0;
		return policy;
	}
}
