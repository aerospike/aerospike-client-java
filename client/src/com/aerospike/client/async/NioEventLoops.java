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

import java.io.IOException;
import java.nio.channels.spi.SelectorProvider;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

/**
 * Asynchronous event loops.
 */
public final class NioEventLoops implements EventLoops {

	final NioEventLoop[] eventLoops;
    private int eventIter;

	/**
	 * Create direct NIO event loops, one per CPU core.
	 */
	public NioEventLoops() throws AerospikeException {
		this(0);
	}

	/**
	 * Create direct NIO event loops.
	 *
	 * @param size		number of event loops to create
	 */
	public NioEventLoops(int size) throws AerospikeException {
		this(new EventPolicy(), size);
	}

	/**
	 * Create direct NIO event loops.
	 *
	 * @param policy	event loop policy
	 * @param size		number of event loops to create
	 */
	public NioEventLoops(EventPolicy policy, int size) throws AerospikeException {
		this(policy, size, false, "aerospike-nio-event-loop");
	}

	/**
	 * Create direct NIO event loops.
	 *
	 * @param policy	event loop policy
	 * @param size		number of event loops to create
	 * @param daemon	true if the associated threads should run as a daemons
	 * @param poolName	event loop thread pool name
	 */
	public NioEventLoops(EventPolicy policy, int size, boolean daemon, String poolName) throws AerospikeException {
		if (policy.minTimeout < 5) {
			throw new AerospikeException("Invalid minTimeout " + policy.minTimeout + ". Must be at least 5ms.");
		}

		if (size <= 0) {
			// Default to all available CPU cores.
			size = Runtime.getRuntime().availableProcessors();

			if (size <= 0) {
				size = 1;
			}
		}
		eventLoops = new NioEventLoop[size];

		SelectorProvider provider = SelectorProvider.provider();

		for (int i = 0; i < eventLoops.length; i++) {
			try {
				eventLoops[i] = new NioEventLoop(policy, provider, i, daemon, poolName);
			}
			catch (IOException ioe) {
                for (int j = 0; j < i; j++) {
                	eventLoops[j].close();
                }
				throw new AerospikeException("Failed to construct event loop: " + Util.getErrorMessage(ioe));
			}
		}

		for (NioEventLoop eventLoop : eventLoops) {
			eventLoop.thread.start();
		}
	}

	/**
	 * Initialize TLS context. For internal use only.
	 */
	public void initTlsContext(TlsPolicy policy) {
		throw new AerospikeException("TLS not supported in direct NIO event loops");
	}

	/**
     * Return array of event loops.
     */
	@Override
	public NioEventLoop[] getArray() {
		return eventLoops;
	}

    /**
     * Return number of event loops in this group.
     */
	@Override
	public int getSize() {
		return eventLoops.length;
	}

	/**
	 * Return event loop given array index.
	 */
	@Override
	public NioEventLoop get(int index) {
		return eventLoops[index];
	}

	/**
	 * Return next event loop in round-robin fashion.
	 */
	@Override
	public NioEventLoop next() {
		int iter = eventIter++; // Not atomic by design
		iter = iter % eventLoops.length;

		if (iter < 0) {
			iter += eventLoops.length;
		}
        return eventLoops[iter];
	}

	/**
	 * Close all event loops.
	 */
	@Override
	public void close() {
		// Send close signal to all event loops.
		for (NioEventLoop eventLoop : eventLoops) {
			eventLoop.execute(new Runnable() {
				public void run() {
		 			throw new NioEventLoop.CloseException();
				}
			});
		}

		// Join with event loop threads.
		for (NioEventLoop eventLoop : eventLoops) {
			try {
				eventLoop.thread.join();
			}
			catch (Exception e) {
			}
		}
		/*
		for (NioEventLoop el : eventLoops) {
			el.timer.printRemaining();
		}
		*/
	}
}
