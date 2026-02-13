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
 * Aerospike event loops interface. Supplies event loops for async operations; pass to
 * {@link com.aerospike.client.AerospikeClient}
 * or use with {@link EventLoop#execute(Runnable)}.
 * <p>
 * Implementations: {@link NioEventLoops}, {@link NettyEventLoops}.
 * <p>Create NioEventLoops or NettyEventLoops and pass to AerospikeClient for async usage.</p>
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(2);
 * IAerospikeClient client = new AerospikeClient(new ClientPolicy(), new Host("localhost", 3000), eventLoops);
 * EventLoop loop = eventLoops.next();
 * client.put(loop, writeListener, null, key, bins);
 * eventLoops.close();
 * }</pre>
 *
 * @see NioEventLoops
 * @see NettyEventLoops
 */
public interface EventLoops extends Closeable {
	/**
	 * Return array of Aerospike event loops.
	 */
	public EventLoop[] getArray();

	/**
	 * Return number of event loops in this group.
	 */
	public int getSize();

	/**
	 * Return Aerospike event loop given array index..
	 */
	public EventLoop get(int index);

	/**
	 * Return next Aerospike event loop in round-robin fashion.
	 * Implementations might not use an atomic sequence counter.
	 * Non-atomic counters improve performance, but might result
	 * in a slightly imperfect round-robin distribution.
	 */
	public EventLoop next();

	/**
	 * Close event loops.
	 */
	public void close();
}
