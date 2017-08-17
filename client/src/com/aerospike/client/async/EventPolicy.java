/*
 * Copyright 2012-2017 Aerospike, Inc.
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

/**
 * Asynchronous event loop configuration.
 */
public final class EventPolicy {
	/**
	 * Minimum command timeout in milliseconds that will be specified for this event loop group.
	 * If command timeouts are less than minTimeout, the actual command timeout will be minTimeout.
	 * The absolute minimum timeout value is 5ms.
	 * <p>
	 * minTimeout is used to specify the tick duration for HashedWheelTimer in each event loop.
	 * minTimeout is also used to specify the direct NIO event loop selector timeout.
	 * <p>
	 * Default: 100ms
	 */
	public int minTimeout = 100;

	/**
	 * The number of ticks per wheel for HashedWheelTimer in each event loop.
	 * <p>
	 * Default: 256
	 */
	public int ticksPerWheel = 256;

	/**
	 * Expected number of concurrent asynchronous commands in each event loop that are active at
	 * any point in time.  This value is used as each event loop's timeout queue and ByteBuffer
	 * pool initial capacity.  This value is not a fixed limit and the queues will dynamically
	 * resize when necessary.
	 * <p>
	 * The real event loop limiting factor is the maximum number of async connections allowed 
	 * per node (defined in {@link com.aerospike.client.policy.ClientPolicy#maxConnsPerNode}).
	 * <p>
	 * Default: 256
	 */
	public int commandsPerEventLoop = 256;
}
