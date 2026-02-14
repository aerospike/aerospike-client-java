/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.io.Closeable;

/**
 * Group of event loops used for asynchronous Aerospike operations.
 * <p>
 * Set on {@link com.aerospike.client.policy.ClientPolicy#eventLoops} when constructing the client, or obtain from {@link com.aerospike.client.AerospikeClient#getEventLoops()}. Use {@link #next()} or {@link #get(int)} to get an event loop for async API calls.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy policy = new ClientPolicy();
 * policy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(policy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * }</pre>
 *
 * @see EventLoop
 * @see NioEventLoops
 * @see NettyEventLoops
 * @see com.aerospike.client.policy.ClientPolicy#eventLoops
 */
public interface EventLoops extends Closeable {
	/** @return array of all event loops in this group (must not be null) */
	public EventLoop[] getArray();

	/** @return number of event loops in this group */
	public int getSize();

	/**
	 * @param index index in the event loop array (0 to size-1)
	 * @return the event loop at the given index
	 */
	public EventLoop get(int index);

	/**
	 * Returns the next event loop in round-robin order; implementations may use a non-atomic counter for performance.
	 * @return next event loop (must not be null)
	 */
	public EventLoop next();

	/** Closes this event loop group and releases resources. */
	public void close();
}
