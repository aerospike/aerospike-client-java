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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

/**
 * Create nio connection and place in connection pool.
 * Used for min connections functionality.
 */
public final class NioConnector extends AsyncConnector implements INioCommand {

	private final NioEventLoop eventLoop;
	private ByteBuffer byteBuffer;
	private NioConnection conn;

	public NioConnector(NioEventLoop eventLoop, Cluster cluster, Node node, AsyncConnector.Listener listener) {
		super(eventLoop, cluster, node, listener);
		this.eventLoop = eventLoop;
	}

	@Override
	public void createConnection() {
		state = AsyncCommand.CONNECT;
		conn = new NioConnection(node.getAddress());
		node.connectionOpened(eventLoop.index);
		conn.registerConnect(eventLoop, this);
	}

	@Override
	public void processEvent(SelectionKey key) {
		try {
			int ops = key.readyOps();

			if ((ops & SelectionKey.OP_READ) != 0) {
				read();
			}
			else if ((ops & SelectionKey.OP_WRITE) != 0) {
				write();
			}
			else if ((ops & SelectionKey.OP_CONNECT) != 0) {
				finishConnect();
			}
		}
		catch (AerospikeException ae) {
			fail(ae);
		}
		catch (Throwable e) {
			fail(new AerospikeException(e));
		}
	}

	protected final void finishConnect() throws IOException {
		conn.finishConnect();

		if (cluster.authEnabled) {
			byte[] token = node.getSessionToken();

			if (token != null) {
				writeAuth(token);
				return;
			}
		}

		finish();
	}

	private final void writeAuth(byte[] token) throws IOException {
		state = AsyncCommand.AUTH_WRITE;

		byte[] dataBuffer = new byte[256];
		AdminCommand admin = new AdminCommand(dataBuffer);
		int dataOffset = admin.setAuthenticate(cluster, token);

		byteBuffer = eventLoop.getByteBuffer();
		byteBuffer.clear();
		byteBuffer.put(dataBuffer, 0, dataOffset);
		byteBuffer.flip();

		if (conn.write(byteBuffer)) {
			if (node.areMetricsEnabled()) {
				node.addBytesOut(null, byteBuffer.position());
			}
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.AUTH_READ_HEADER;
			conn.registerRead();
		}
		else {
			state = AsyncCommand.AUTH_WRITE;
			conn.registerWrite();
		}
	}

	protected final void write() throws IOException {
		if (conn.write(byteBuffer)) {
			if (node.areMetricsEnabled()) {
				node.addBytesOut(null, byteBuffer.position());
			}
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.AUTH_READ_HEADER;
			conn.registerRead();
		}
	}

	protected final void read() throws IOException {
		if (! conn.read(byteBuffer)) {
			return;
		}

		switch (state) {
		case AsyncCommand.AUTH_READ_HEADER:
			readAuthHeader();
			if (! conn.read(byteBuffer)) {
				return;
			}
			// Fall through to AUTH_READ_BODY

		case AsyncCommand.AUTH_READ_BODY:
			readAuthBody();
			finish();
			break;
		}
	}

	private final void readAuthHeader() {
		byteBuffer.position(0);

		int receiveSize = ((int)(byteBuffer.getLong() & 0xFFFFFFFFFFFFL));

		if (receiveSize < 2 || receiveSize > byteBuffer.capacity()) {
			throw new AerospikeException.Parse("Invalid auth receive size: " + receiveSize);
		}
		byteBuffer.clear();
		byteBuffer.limit(receiveSize);
		state = AsyncCommand.AUTH_READ_BODY;
	}

	private final void readAuthBody() {
		int resultCode = byteBuffer.get(1) & 0xFF;

		if (resultCode != 0 && resultCode != ResultCode.SECURITY_NOT_ENABLED) {
			// Authentication failed. Session token probably expired.
			// Signal tend thread to perform node login, so future
			// commands do not fail.
			node.signalLogin();

			// This is a rare event because the client tracks session
			// expiration and will relogin before session expiration.
			// Do not try to login on same socket because login can take
			// a long time and thousands of simultaneous logins could
			// overwhelm server.
			throw new AerospikeException(resultCode);
		}
	}

	private final void finish() {
		conn.unregister();
		conn.addBytesIn(node, null);
		conn.updateLastUsed();
		putByteBuffer();
		success();
	}

	private final void putByteBuffer() {
		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
			byteBuffer = null;
		}
	}

	@Override
	final boolean addConnection() {
		boolean ret = node.putAsyncConnection(conn, eventLoop.index);
		conn = null;
		return ret;
	}

	@Override
	final void closeConnection() {
		putByteBuffer();

		if (conn != null) {
			conn.addBytesIn(node, null);
			node.closeAsyncConnection(conn, eventLoop.index);
			conn = null;
		}
		else {
			node.decrAsyncConnection(eventLoop.index);
		}
	}
}
