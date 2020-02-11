/*
 * Copyright 2012-2020 Aerospike, Inc.
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
 * Configuration variables for multi-record get and exist requests.
 */
public final class BatchPolicy extends Policy {
	/**
	 * Maximum number of concurrent synchronous batch request threads to server nodes at any point in time.
	 * If there are 16 node/namespace combinations requested and maxConcurrentThreads is 8,
	 * then batch requests will be made for 8 node/namespace combinations in parallel threads.
	 * When a request completes, a new request will be issued until all 16 requests are complete.
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
	 * > 0: Issue up to maxConcurrentThreads batch requests in parallel threads.  When a request
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
	 * Allow batch to be processed immediately in the server's receiving thread when the server
	 * deems it to be appropriate.  If false, the batch will always be processed in separate
	 * transaction threads.  This field is only relevant for the new batch index protocol.
	 * <p>
	 * For batch exists or batch reads of smaller sized records (<= 1K per record), inline
	 * processing will be significantly faster on "in memory" namespaces.  The server disables
	 * inline processing on disk based namespaces regardless of this policy field.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: true
	 */
	public boolean allowInline = true;

	/**
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is only necessary when authentication is enabled and security roles are defined
	 * on a per set basis.
	 * <p>
	 * Default: false
	 */
	public boolean sendSetName;

	/**
	 * Copy batch policy from another batch policy.
	 */
	public BatchPolicy(BatchPolicy other) {
		super(other);
		this.maxConcurrentThreads = other.maxConcurrentThreads;
		this.allowInline = other.allowInline;
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
}
