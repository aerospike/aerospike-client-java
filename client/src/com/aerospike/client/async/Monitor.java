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
 * Monitor that coordinates wait/notify between different threads.
 */
public final class Monitor {
	private boolean completed;

	/**
	 * Wait in current thread for other thread to complete.
	 */
	public synchronized void waitTillComplete() {
		while (! completed) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	/**
	 * Notify waiting thread that this thread is complete.
	 */
	public synchronized void notifyComplete() {
		completed = true;
		super.notify();
	}
}
