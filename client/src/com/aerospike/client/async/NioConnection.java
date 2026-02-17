/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

/**
 * Asynchronous socket channel connection wrapper.
 */
public final class NioConnection extends AsyncConnection implements Closeable {
	private final SocketChannel socketChannel;
	private SelectionKey key;
	private long bytesIn;

	public NioConnection(InetSocketAddress address) {
		try {
			socketChannel = SocketChannel.open();
		}
		catch (Throwable e) {
			throw new AerospikeException.Connection("SocketChannel open error: " + e.getMessage());
		}

		try {
			socketChannel.configureBlocking(false);
			Socket socket = socketChannel.socket();
			socket.setTcpNoDelay(true);

			// These options are useful when the connection pool is poorly bounded or there are a large
			// amount of network errors.  Since these conditions are not the normal use case and
			// the options could theoretically result in latent data being sent to new commands, leave
			// them out for now.
			// socket.setReuseAddress(true);
			// socket.setSoLinger(true, 0);

			socketChannel.connect(address);
		}
		catch (Throwable e) {
			close();
			throw new AerospikeException.Connection("SocketChannel init error: " + e.getMessage());
		}
	}

	public void registerConnect(NioEventLoop eventLoop, INioCommand command) {
		try {
			key = socketChannel.register(eventLoop.selector, SelectionKey.OP_CONNECT, command);
		}
		catch (ClosedChannelException e) {
			throw new AerospikeException.Connection("SocketChannel register error: " + e.getMessage());
		}
	}

	public void finishConnect() throws IOException {
		socketChannel.finishConnect();
	}

	public void attach(INioCommand command) {
		key.attach(command);
		bytesIn = 0;
	}

	public void registerWrite() {
		key.interestOps(SelectionKey.OP_WRITE);
	}

	public boolean write(ByteBuffer byteBuffer) throws IOException {
		socketChannel.write(byteBuffer);
		return ! byteBuffer.hasRemaining();
	}

	public void registerRead() {
		key.interestOps(SelectionKey.OP_READ);
	}

	/**
	 * Read till byteBuffer limit reached or received would-block.
	 */
	public boolean read(ByteBuffer byteBuffer) throws IOException {
		while (byteBuffer.hasRemaining()) {
			int len = socketChannel.read(byteBuffer);

			if (len == 0) {
				// Got would-block.
				return false;
			}

			if (len < 0) {
				// Server has shutdown socket.
				throw new EOFException();
			}

			bytesIn += len;
		}
		return true;
	}

	/**
	 * Validate connection in a command.  Return true if socket is connected and
	 * has no data in it's buffer.  Return false, if not connected, socket read error
	 * or has data in it's buffer.
	 */
	@Override
	public boolean isValid(ByteBuffer byteBuffer) {
		// Do not use socketChannel.isOpen() or socketChannel.isConnected() because
		// they do not take server actions on socket into account.
		byteBuffer.position(0);
		byteBuffer.limit(1);

		try {
			// Perform non-blocking read.
			// Expect socket buffer to be empty.
			return socketChannel.read(byteBuffer) == 0;
		}
		catch (Throwable e) {
			return false;
		}
	}

	public void unregister() {
		key.interestOps(0);
		key.attach(null);
	}

	public void addBytesIn(Node node, String namespace) {
		if (node.areMetricsEnabled()) {
			node.addBytesIn(namespace, bytesIn);
		}
		bytesIn = 0;
	}

	public long resetBytesIn() {
		long tmp = bytesIn;
		bytesIn = 0;
		return tmp;
	}

	/**
	 * Close socket channel.
	 */
	@Override
	public void close() {
		if (key != null) {
			key.cancel();
		}

		try {
			socketChannel.close();
		}
		catch (Throwable e) {
			if (Log.debugEnabled()) {
				Log.debug("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}
}
