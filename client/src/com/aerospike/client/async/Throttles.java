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
 * Use multiple throttles that enforce a limit on the maximum number of commands.
 * Useful in sync scan/async touch situations where scan thread needs to slow down
 * sending commands to event loops, so the event loop queues do not get overloaded.
 *
 * Warning: Do not wait for slots directly from event loop threads.  Deadlock
 * may occur in that situation.
 */
public final class Throttles {
	private final Throttle[] throttles;

	/**
	 * Construct throttles.
	 */
	public Throttles(int size, int maxCommandsPerEventLoop) {
		throttles = new Throttle[size];

		for (int i = 0; i < throttles.length; i++) {
			throttles[i] = new Throttle(maxCommandsPerEventLoop);
		}
	}

	/**
	 * Wait for a throttle's command slot(s).
	 *
	 * @param index		array index
	 * @param count		count of commands to reserve
	 * @return			if command should be processed
	 */
	public boolean waitForSlot(int index, int count) {
		return throttles[index].waitForSlot(count);
	}

	/**
	 * Recover slot(s) from commands that have completed.
	 *
	 * @param index		array index
	 * @param count		count of commands to reclaim
	 */
	public void addSlot(int index, int count) {
		throttles[index].addSlot(count);
	}

	/**
	 * Get current number of commands that could be run for a throttle.
	 */
	public int getAvailable(int index) {
		return throttles[index].getAvailable();
	}
}
