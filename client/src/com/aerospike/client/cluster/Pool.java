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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Concurrent bounded LIFO stack of connections.
 * <p>
 * The standard library concurrent stack, ConcurrentLinkedDequeue, will not suffice
 * because it's not bounded and it's size() method is too expensive.
 */
public final class Pool {

	private final Connection[] conns;
	private int head;
	private int tail;
	private int size;
	private final ReentrantLock lock;
	final AtomicInteger total;  // total connections: inUse + inPool

	public Pool(int capacity) {
		conns = new Connection[capacity];
		lock = new ReentrantLock(false);
		total = new AtomicInteger();
	}

	public int capacity() {
		return conns.length;
	}

	/**
	 * Insert connection at head of stack.
	 */
	public boolean offer(Connection conn) {
		if (conn == null) {
			throw new NullPointerException();
		}

		final ReentrantLock lock = this.lock;
		lock.lock();

		try {
			if (size == conns.length) {
				return false;
			}

			final Connection[] conns = this.conns;
			conns[head] = conn;

			if (++head == conns.length) {
				head = 0;
			}
			size++;
			return true;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Pop connection from head of stack.
	 */
	public Connection poll() {
		final ReentrantLock lock = this.lock;
		lock.lock();

		try {
			if (size == 0) {
				return null;
			}

			if (head == 0) {
				head = conns.length - 1;
			}
			else {
				head--;
			}
			size--;

			final Connection[] conns = this.conns;
			final Connection conn = conns[head];
			conns[head] = null;
			return conn;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Close connections that are idle for more than maxSocketIdle.
	 */
	public void closeIdle(Node node) {
		while (true) {
			// Lock on each iteration to give fairness to other
			// threads polling for connections.
			Connection conn;
			final ReentrantLock lock = this.lock;
			lock.lock();

			try {
				if (size == 0) {
					return;
				}

				// The oldest connection is at tail.
				final Connection[] conns = this.conns;
				conn = conns[tail];

				if (conn.isValid()) {
					return;
				}

				conns[tail] = null;

				if (++tail == conns.length) {
					tail = 0;
				}
				size--;
			}
			finally {
				lock.unlock();
			}

			// Close connection outside of lock.
			total.getAndDecrement();
			conn.close(node);
		}
	}

	/**
	 * Return item count.
	 */
	public int size() {
		final ReentrantLock lock = this.lock;
		lock.lock();

		try {
			return size;
		}
		finally {
			lock.unlock();
		}
	}
}
