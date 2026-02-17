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
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.util.Util;

public final class NioRecover implements INioCommand, TimerTask {
	//private static final AtomicInteger Counter = new AtomicInteger();

	private final NioEventLoop eventLoop;
	private final EventState eventState;
	private final Node node;
	private final NioConnection conn;
	private final HashedWheelTimeout timeoutTask;
	private final ByteBuffer byteBuffer;
	//private final int tranId;
	private int offset;
	private int length;
	private int state;
	private final boolean isSingle;
	private final boolean checkReturnCode;
	private boolean isLastGroup;

	public NioRecover(NioCommand cmd) {
		AsyncCommand a = cmd.command;
		this.eventLoop = cmd.eventLoop;
		this.eventState = cmd.eventState;
		this.node = cmd.node;
		this.conn = cmd.conn;
		this.byteBuffer = cmd.byteBuffer;

		switch (cmd.state) {
		case AsyncCommand.AUTH_READ_HEADER:
			this.offset = byteBuffer.position();
			this.length = byteBuffer.limit();
			this.state = AsyncCommand.COMMAND_READ_HEADER;
			this.isSingle = true;
			this.checkReturnCode = true;
			break;

		case AsyncCommand.AUTH_READ_BODY:
			this.offset = byteBuffer.position();
			this.length = byteBuffer.limit();
			this.state = AsyncCommand.COMMAND_READ_BODY;
			this.isSingle = true;
			this.checkReturnCode = true;
			break;

		case AsyncCommand.COMMAND_READ_BODY:
			if (a.isSingle) {
				this.offset = byteBuffer.position();
				this.length = byteBuffer.limit();
			}
			else {
				// Multi-record detail handles offsets differently.
				this.offset = a.dataOffset;
				this.length = a.receiveSize;

				if (a.dataOffset >= 4) {
					// Warning: The following code assumes multi-record responses always end with a separate proto
					// that only contains one header with the info3 last group bit.  This is always true for batch
					// and scan, but query does not conform.  Therefore, connection recovery for queries will
					// likely fail.
					byte info3 = a.dataBuffer[3];

					if ((info3 & Command.INFO3_LAST) != 0) {
						isLastGroup = true;
					}
				}
			}
			this.state = cmd.state;
			this.isSingle = a.isSingle;
			this.checkReturnCode = false;
			break;

		default:
			this.offset = byteBuffer.position();
			this.length = byteBuffer.limit();
			this.state = cmd.state;
			this.isSingle = a.isSingle;
			this.checkReturnCode = false;
			break;
		}

		//tranId = Counter.getAndIncrement();
		//System.out.println("" + tranId + " timeout:" + a.isSingle + ',' + cmd.state + ',' + offset + ',' + length);

		conn.attach(this);
		timeoutTask = new HashedWheelTimeout(this);
		eventLoop.timer.addTimeout(timeoutTask, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(a.policy.timeoutDelay));
	}

	@Override
	public final void timeout() {
		//System.out.println("" + tranId + " timeout expired. close connection");

		// Command has been delayed long enough.
		// User has already been notified.
		// timeoutTask has already been removed, so avoid cancel.
		abort(false);
	}

	@Override
	public final void processEvent(SelectionKey key) {
		try {
			int ops = key.readyOps();

			if ((ops & SelectionKey.OP_READ) != 0) {
				switch (state) {
				case AsyncCommand.COMMAND_READ_HEADER:
					if (isSingle) {
						drainSingleHeader();
					}
					else {
						if (drainMultiHeader()) {
							drainMultiBody();
						}
					}
					break;

				case AsyncCommand.COMMAND_READ_BODY:
					if (isSingle) {
						drainSingleBody();
					}
					else {
						drainMultiBody();
					}
					break;
				}
			}
		}
		catch (Throwable e) {
			//System.out.println("" + tranId + " socket error:");
			//e.printStackTrace();
			abort(true);
		}
	}

	private final void drainSingleHeader() throws IOException {
		if (! conn.read(byteBuffer)) {
			return;
		}

		byteBuffer.position(0);
		length = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));
		byteBuffer.clear();

		if (length < byteBuffer.capacity()) {
			byteBuffer.limit(length);
		}

		offset = 0;
		state = AsyncCommand.COMMAND_READ_BODY;
		drainSingleBody();
	}

	private final void drainSingleBody() throws IOException {
		while (true) {
			if (! conn.read(byteBuffer)) {
				return;
			}

			if (checkReturnCode) {
				int resultCode = byteBuffer.get(1) & 0xFF;

				if (resultCode != 0) {
					// Authentication failed.
					//System.out.println("" + tranId + " invalid user/password:");
					abort(false);
					return;
				}
			}

			if (resetBuffer()) {
				continue;
			}

			recover();
			return;
		}
	}

	private final boolean drainMultiHeader() throws IOException {
		while (true) {
			if (! conn.read(byteBuffer)) {
				return false;
			}

			byteBuffer.position(0);
			long proto = byteBuffer.getLong();
			length = ((int)(proto & 0xFFFFFFFFFFFFL));

			if (length <= 0) {
				// Received zero length block. Read next header.
				byteBuffer.clear();
				byteBuffer.limit(8);
				continue;
			}

			boolean compressed = ((proto >> 48) & 0xff) == Command.MSG_TYPE_COMPRESSED;

			if (compressed) {
				// Do not recover connections with compressed data because that would
				// require saving large buffers with associated state and performing decompression
				// just to drain the connection.
				throw new AerospikeException("Recovering connections with compressed multi-record data is not supported");
			}
			break;
		}

		byteBuffer.clear();

		if (length < byteBuffer.capacity()) {
			byteBuffer.limit(length);
		}

		offset = 0;
		state = AsyncCommand.COMMAND_READ_BODY;
		return true;
	}

	private final void drainMultiBody() throws IOException {
		while (true) {
			if (! conn.read(byteBuffer)) {
				return;
			}

			// Only check for last group on the header of the
			// first record in the group.
			if (offset < 4) {
				byte info3 = byteBuffer.get(3);

				if ((info3 & Command.INFO3_LAST) != 0) {
					isLastGroup = true;
				}
			}

			if (resetBuffer()) {
				continue;
			}

			if (isLastGroup) {
				recover();
				return;
			}

			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.COMMAND_READ_HEADER;

			if (! drainMultiHeader()) {
				return;
			}
		}
	}

	private final boolean resetBuffer() {
		offset += byteBuffer.limit();

		if (offset < length) {
			byteBuffer.clear();

			int remaining = length - offset;

			if (remaining < byteBuffer.capacity()) {
				byteBuffer.limit(remaining);
			}
			return true;
		}
		return false;
	}

	private final void recover() {
		//System.out.println("" + tranId + " connection drained");
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		state = AsyncCommand.COMPLETE;

		try {
			conn.unregister();
			conn.addBytesIn(node, null);
			conn.updateLastUsed();
			node.putAsyncConnection(conn, eventLoop.index);
			close(true);
		}
		catch (Throwable e) {
			if (! eventState.closed) {
				Log.error("NioRecover recover failed: " + Util.getStackTrace(e));
			}
		}
	}

	private final void abort(boolean cancelTimeout) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		state = AsyncCommand.COMPLETE;

		try {
			conn.addBytesIn(node, null);
			node.closeAsyncConnection(conn, eventLoop.index);
			close(cancelTimeout);
		}
		catch (Throwable e) {
			if (! eventState.closed) {
				Log.error("NioRecover abort failed: " + Util.getStackTrace(e));
			}
		}
	}

	private final void close(boolean cancelTimeout) {
		if (cancelTimeout) {
			timeoutTask.cancel();
		}

		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
	}
}
