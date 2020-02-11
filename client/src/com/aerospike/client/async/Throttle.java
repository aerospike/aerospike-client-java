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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Limit the number of commands allowed at any point in time.
 */
public final class Throttle {
	private final ReentrantLock lock;
    private final Condition avail;
	private int available;

	/**
	 * Construct throttle with max number of commands.
	 */
	public Throttle(int capacity) {
		lock = new ReentrantLock(false);
		avail = lock.newCondition();
		available = capacity;
	}

	/**
	 * Wait for command slot(s).
	 *
	 * @param count		count of commands to reserve
	 * @return			if command should be processed
	 */
	public boolean waitForSlot(int count) {
		try {
	        final ReentrantLock lock = this.lock;
	        lock.lockInterruptibly();
	        try {
	            while (available < count) {
	            	avail.await();
	            }
	            available -= count;
				return true;
	        }
	        finally {
	            lock.unlock();
	        }
		}
		catch (InterruptedException ie) {
			return false;
		}
	}

	/**
	 * Recover slot(s) from commands that have completed.
	 *
	 * @param count		count of commands to reclaim
	 */
	public void addSlot(int count) {
		lock.lock();
		try {
			available += count;
			avail.signal();
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Get current number of commands that could be run.
	 */
	public int getAvailable() {
		return available;
	}
}
