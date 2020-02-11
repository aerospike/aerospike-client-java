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
package com.aerospike.client.cluster;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread pool factory that generates daemon threads.
 * Daemon threads automatically terminate when the program terminates.
 */
public class ThreadDaemonFactory implements ThreadFactory {
	private final AtomicInteger sequence = new AtomicInteger();

	/**
	 * Create new daemon thread with Aerospike prefix and sequence number.
	 */
	public final Thread newThread(Runnable runnable) {
		Thread thread = new Thread(runnable, "Aerospike-" + sequence.incrementAndGet());
		thread.setDaemon(true);
		return thread;
	}
}
