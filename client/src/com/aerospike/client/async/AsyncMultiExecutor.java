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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;

public abstract class AsyncMultiExecutor {

	private final EventLoop eventLoop;
	private final Cluster cluster;
	private final AtomicInteger completedCount = new AtomicInteger();
    private final AtomicBoolean done = new AtomicBoolean();
	private AsyncMultiCommand[] commands;
	private int maxConcurrent;
	
	public AsyncMultiExecutor(EventLoop eventLoop, Cluster cluster) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
	}
	
	public void execute(AsyncMultiCommand[] commands, int maxConcurrent) {	
		this.commands = commands;
		this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.length) ? commands.length : maxConcurrent;
		
		for (int i = 0; i < this.maxConcurrent; i++) {
			eventLoop.execute(cluster, commands[i]);
		}
	}
	
	protected final void childSuccess() {
		int finished = completedCount.incrementAndGet();

		if (finished < commands.length) {
			int nextThread = finished + maxConcurrent - 1;

			// Determine if a new command needs to be started.
			if (nextThread < commands.length && ! done.get()) {
				// Start new command.
				eventLoop.execute(cluster, commands[nextThread]);
			}
		}
		else {
			// All commands complete. Notify success if an exception has not already occurred.
			if (done.compareAndSet(false, true)) {
				onSuccess();
			}
		}
	}
	
	protected final void childFailure(AerospikeException ae) {
		// There is no need to stop commands if all commands have already completed.
		if (done.compareAndSet(false, true)) {    	
			// Send stop signal to all commands.
			for (AsyncMultiCommand command : commands) {
				command.stop();
			}
			onFailure(ae);
		}
	}
	
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
