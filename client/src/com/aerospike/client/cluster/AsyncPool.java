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
package com.aerospike.client.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.AsyncConnection;
import com.aerospike.client.async.AsyncConnectorExecutor;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.NettyConnection;

/**
 * Non-threadsafe bounded LIFO stack of async connections.
 * All modifications must be done in event loop threads.
 */
public final class AsyncPool {
	private final ArrayDeque<AsyncConnection> queue;
	private final int minSize;
	private final int maxSize;
	private final int index;
	private int total;
	private int opened;
	private int closed;
	private int removeClosed;

	public AsyncPool(int minSize, int maxSize, int eventLoopIndex) {
		this.queue = new ArrayDeque<AsyncConnection>(maxSize);
		this.minSize = minSize;
		this.maxSize = maxSize;
		this.index = eventLoopIndex;
	}

	public AsyncConnection getConnection(Node node, ByteBuffer byteBuffer) {
		Cluster cluster = node.cluster;
		AsyncConnection conn;

		while ((conn = queue.pollFirst()) != null) {
			if (! cluster.isConnCurrentTran(conn.getLastUsed())) {
				closeIdleConnection(conn);
				continue;
			}

			if (! conn.isValid(byteBuffer)) {
				closeConnection(node, conn);
				continue;
			}
			return conn;
		}

		if (reserve()) {
			return null;
		}

		throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
			"Max async conns reached: " + this + ',' + index + ',' + total +
			',' + queue.size() + ',' + maxSize);
	}

	public boolean putConnection(AsyncConnection conn) {
		if (conn == null) {
			if (Log.warnEnabled()) {
				Log.warn("Async conn is null: " + this + ',' + index);
			}
			return false;
		}

		if (queue.size() < maxSize) {
			queue.addFirst(conn);
			return true;
		}

		// This should not happen since connection slots are reserved in advance
		// and total connections should never exceed maxSize. If it does happen,
		// it's highly likely that total count was decremented twice for the same
		// transaction, causing the connection balancer to create more connections
		// than necessary. Attempt to correct situation by not decrementing total
		// when this excess connection is closed.
		conn.close();
		//connectionClosed();

		if (Log.warnEnabled()) {
			Log.warn("Async conn pool is full: " + this + ',' + index + ',' + total +
					 ',' + queue.size() + ',' + maxSize);
		}
		return false;
	}

	public void balance(EventLoop eventLoop, Node node) {
		if (removeClosed > 0) {
			removeClosed = 0;
			removeClosedAll();
		}

		int excess = total - minSize;

		if (excess > 0) {
			closeIdleConnections(node, excess);
		}
		else if (excess < 0 && node.errorCountWithinLimit()) {
			// Create connection requests sequentially because they will be done in the
			// background and there is no immediate need for them to complete.
			new AsyncConnectorExecutor(eventLoop, node.cluster, node, -excess, 1, null, null);
		}
	}

	public void removeClosedAll() {
		// Remove all closed netty connections in the queue.
		NettyConnection first = (NettyConnection)queue.pollFirst();
		NettyConnection conn = first;

		do {
			if (conn == null) {
				break;
			}

			if (conn.isOpen()) {
				queue.addLast(conn);
			}
			else {
				connectionClosed();
			}

			conn = (NettyConnection)queue.pollFirst();
		} while (conn != first);
	}

	public void removeClosedTail() {
		// Remove closed netty connections starting from tail of queue
		// until an open connection is reached or the queue is empty.
		removeClosed++;

		while (true) {
			NettyConnection conn = (NettyConnection)queue.peekLast();

			if (conn == null || conn.isOpen()) {
				break;
			}

			// Pop connection from queue and update statistics.
			queue.pollLast();
			connectionClosed();
			removeClosed--;
		}
	}

	private void closeIdleConnections(Node node, int count) {
		// Oldest connection is at end of queue.
		Cluster cluster = node.cluster;

		while (count > 0) {
			AsyncConnection conn = queue.peekLast();

			if (conn == null || cluster.isConnCurrentTrim(conn.getLastUsed())) {
				break;
			}

			// Pop connection from queue.
			queue.pollLast();
			closeIdleConnection(conn);
			count--;
		}
	}

	private void closeIdleConnection(AsyncConnection conn) {
		connectionClosed();
		conn.close();
	}

	public void closeConnections() {
		// Only called when closing node, so updating connection statistics is not necessary.
		AsyncConnection conn;

		while ((conn = queue.pollFirst()) != null) {
			conn.close();
		}
	}

	public void closeConnection(Node node, AsyncConnection conn) {
		connectionClosed();
		conn.close();
		node.incrErrorCount();
	}

	public boolean reserve() {
		if (total >= maxSize || queue.size() >= maxSize) {
			return false;
		}
		total++;
		return true;
	}

	public void release(Node node) {
		total--;
		node.incrErrorCount();
	}

	public void connectionOpened() {
		opened++;
	}

	private void connectionClosed() {
		total--;
		closed++;
	}

	public ConnectionStats getConnectionStats() {
		int inPool = queue.size();
		return new ConnectionStats(total - inPool, inPool, opened, closed);
	}

	public int getMinSize() {
		return minSize;
	}
}
