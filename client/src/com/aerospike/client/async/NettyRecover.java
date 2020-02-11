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

import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

public final class NettyRecover implements TimerTask {
	//private static final AtomicInteger Counter = new AtomicInteger();

	private final NettyEventLoop eventLoop;
	private final Node node;
	private final EventState eventState;
	private final NettyConnection conn;
	private final HashedWheelTimeout timeoutTask;
	private final byte[] dataBuffer;
	//private int tranId;
	private int offset;
	private int length;
	private int state;
	private final boolean isSingle;
	private final boolean checkReturnCode;
	private boolean isLastGroup;

	public NettyRecover(NettyCommand cmd) {
		AsyncCommand a = cmd.command;
		this.eventLoop = cmd.eventLoop;
		this.node = cmd.node;
		this.eventState = cmd.eventState;
		this.conn = cmd.conn;
		this.dataBuffer = a.dataBuffer;  // take ownership of dataBuffer.
		this.offset = a.dataOffset;
		this.length = a.receiveSize;

		//tranId = Counter.getAndIncrement();
		//System.out.println("" + tranId + " timeout:" + a.isSingle + ',' + cmd.state + ',' + offset + ',' + length);

		if (cmd.state == AsyncCommand.AUTH_READ_HEADER) {
			this.state = AsyncCommand.COMMAND_READ_HEADER;
			this.isSingle = true;
			this.checkReturnCode = true;
		}
		else if (cmd.state == AsyncCommand.AUTH_READ_BODY) {
			this.state = AsyncCommand.COMMAND_READ_BODY;
			this.isSingle = true;
			this.checkReturnCode = true;

			if (offset >= 2) {
				if (dataBuffer[1] != 0) {
					// Authentication failed.
					//System.out.println("" + tranId + " invalid user/password:");
					timeoutTask = null;
					abort(false);
					return;
				}
			}
		}
		else {
			this.state = cmd.state;
			this.isSingle = a.isSingle;
			this.checkReturnCode = false;
		}

		// Do not check pending limit because connection may already have events.
		eventState.pending++;
		eventLoop.pending++;

		// Replace channel handler.
		ChannelPipeline p = conn.channel.pipeline();
		p.removeLast();
		p.addLast(new InboundHandler(this));

		timeoutTask = eventLoop.timer.addTimeout(this, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(a.policy.timeoutDelay));
	}

	@Override
	public final void timeout() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		//System.out.println("" + tranId + " timeout expired. close connection");

		// Transaction has been delayed long enough.
		// User has already been notified.
		// timeoutTask has already been removed, so avoid cancel.
		abort(false);
	}

	public void drain(ByteBuf byteBuffer) {
		try {
			switch (state) {
			case AsyncCommand.COMMAND_READ_HEADER:
				if (isSingle) {
					drainSingleHeader(byteBuffer);
				}
				else {
					drainMultiHeader(byteBuffer);
				}
				break;

			case AsyncCommand.COMMAND_READ_BODY:
				if (isSingle) {
					drainSingleBody(byteBuffer);
				}
				else {
					if (! drainMultiBody(byteBuffer)) {
						return;
					}
					drainMultiHeader(byteBuffer);
				}
				break;
			}
		}
		finally {
			byteBuffer.release();
		}
	}

	private final void drainSingleHeader(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();
		int dataSize = offset + readableBytes;

		if (dataSize < 8) {
			byteBuffer.readBytes(dataBuffer, offset, readableBytes);
			offset = dataSize;
			return;
		}

		dataSize = 8 - offset;
		byteBuffer.readBytes(dataBuffer, offset, dataSize);
		readableBytes -= dataSize;
		length = ((int)(Buffer.bytesToLong(dataBuffer, 0) & 0xFFFFFFFFFFFFL));

		state = AsyncCommand.COMMAND_READ_BODY;
		offset = 0;
		drainSingleBody(byteBuffer);
	}

	private final void drainSingleBody(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();

		if (checkReturnCode && offset < 2 && offset + readableBytes >= 2) {
			int len = 2 - offset;
			byteBuffer.readBytes(dataBuffer, 0, len);
			readableBytes -= len;
			offset += len;

			byte resultCode = dataBuffer[len - 1];

			if (resultCode != 0) {
				// Authentication failed.
				//System.out.println("" + tranId + " invalid user/password:");
				abort(false);
				return;
			}

			if (readableBytes <= 0) {
				return;
			}
		}

		byteBuffer.skipBytes(readableBytes);
		offset += readableBytes;

		if (offset >= length) {
			recover();
		}
	}

	private final void drainMultiHeader(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();
		int dataSize;

		do {
			dataSize = offset + readableBytes;

			if (dataSize < 8) {
				byteBuffer.readBytes(dataBuffer, offset, readableBytes);
				offset = dataSize;
				return;
			}

			dataSize = 8 - offset;
			byteBuffer.readBytes(dataBuffer, offset, dataSize);
			readableBytes -= dataSize;
			long proto = Buffer.bytesToLong(dataBuffer, 0);
			length = ((int)(proto & 0xFFFFFFFFFFFFL));

			if (length == 0) {
				// Read next header.
				offset = 0;
				continue;
			}

			boolean compressed = ((proto >> 48) & 0xff) == Command.MSG_TYPE_COMPRESSED;

			if (compressed) {
				// Do not recover connections with compressed data because that would
				// require saving large buffers with associated state and performing decompression
				// just to drain the connection.
				throw new AerospikeException("Recovering connections with compressed multi-record data is not supported");
			}

			state = AsyncCommand.COMMAND_READ_BODY;
			offset = 0;

			if (readableBytes <= 0) {
				return;
			}

			if (! drainMultiBody(byteBuffer)) {
				return;
			}

			readableBytes = byteBuffer.readableBytes();
		} while (true);
	}

	private final boolean drainMultiBody(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();

		if (offset < 4 && offset + readableBytes >= 4) {
			int len = 4 - offset;
			byteBuffer.readBytes(dataBuffer, 0, len);
			readableBytes -= len;
			offset += len;

			// Warning: The following code assumes multi-record responses always end with a separate proto
			// that only contains one header with the info3 last group bit.  This is always true for batch
			// and scan, but query does not conform.  Therefore, connection recovery for queries will
			// likely fail.
			byte info3 = dataBuffer[len - 1];

			if ((info3 & Command.INFO3_LAST) != 0) {
				isLastGroup = true;
			}

			if (readableBytes <= 0) {
				return false;
			}
		}

		int needBytes = length - offset;
		int dataSize = (readableBytes >= needBytes)? needBytes : readableBytes;

		byteBuffer.skipBytes(dataSize);
		offset += dataSize;

		if (offset < length) {
			return false;
		}

		if (isLastGroup) {
			recover();
			return false;
		}

		// Prepare for next group.
		state = AsyncCommand.COMMAND_READ_HEADER;
		offset = 0;
		return true;
	}

	private final void recover() {
		//System.out.println("" + tranId + " connection drained");

		// Assign normal InboundHandler to connection.
		ChannelPipeline p = conn.channel.pipeline();
		p.removeLast();
		p.addLast(new NettyCommand.InboundHandler());

		// Put connection into pool.
		conn.channel.config().setAutoRead(false);
		conn.updateLastUsed();
		node.putAsyncConnection(conn, eventLoop.index);

		// Close recovery command.
		close(true);
	}

	private final void abort(boolean cancelTimeout) {
		node.closeAsyncConnection(conn, eventLoop.index);
		close(cancelTimeout);
	}

	private final void close(boolean cancelTimeout) {
		if (cancelTimeout) {
			timeoutTask.cancel();
		}

		if (dataBuffer.length <= AsyncCommand.MAX_BUFFER_SIZE) {
			eventLoop.bufferQueue.addLast(dataBuffer);
		}
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
		eventLoop.pending--;
		eventLoop.tryDelayQueue();
	}

	private static class InboundHandler extends ChannelInboundHandlerAdapter {
		private final NettyRecover command;

		public InboundHandler(NettyRecover command) {
			this.command = command;
		}

	    @Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg) {
	    	command.drain((ByteBuf)msg);
	    }

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			//System.out.println("" + command.tranId + " socket error:");
			//cause.printStackTrace();
			command.abort(true);
		}
	}
}
