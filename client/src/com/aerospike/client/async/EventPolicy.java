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
package com.aerospike.client.async;

/**
 * Asynchronous event loop configuration.
 */
public final class EventPolicy {
	/**
	 * Maximum number of async commands that can be processed in each event loop at any point in
	 * time. Each executing async command requires a socket connection.  Consuming too
	 * many sockets can negatively affect application reliability and performance.  If the user does
	 * not limit async command count in their application, this field should be used to enforce a
	 * limit internally in the client.
	 * <p>
	 * If this limit is reached, the next async command will be placed on the event loop's delay
	 * queue for later execution.  If this limit is zero, all async commands will be executed
	 * immediately and the delay queue will not be used.
	 * <p>
	 * If defined, a reasonable value is 40.  The optimal value will depend on cpu count, cpu speed,
	 * network bandwidth and the number of event loops employed.
	 * <p>
	 * Default: 0 (execute all async commands immediately)
	 */
	public int maxCommandsInProcess;

	/**
	 * Maximum number of async commands that can be stored in each event loop's delay queue for
	 * later execution.  Queued commands consume memory, but they do not consume sockets. This
	 * limit should be defined when it's possible that the application executes so many async
	 * commands that memory could be exhausted.
	 * <p>
	 * If this limit is reached, the next async command will be rejected with exception
	 * {@link com.aerospike.client.AerospikeException.AsyncQueueFull}.
	 * If this limit is zero, all async commands will be accepted into the delay queue.
	 * <p>
	 * The optimal value will depend on your application's magnitude of command bursts and the
	 * amount of memory available to store commands.
	 * <p>
	 * Default: 0 (no delay queue limit)
	 */
	public int maxCommandsInQueue;

	/**
	 * Initial capacity of each event loop's delay queue.  The delay queue can resize beyond this
	 * initial capacity.
	 * <p>
	 * Default: 256 (if delay queue is used)
	 */
	public int queueInitialCapacity = 256;

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
