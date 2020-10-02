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

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSession;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

/**
 * Asynchronous command handler using netty.
 */
public final class NettyCommand implements Runnable, TimerTask {

	final NettyEventLoop eventLoop;
	final Cluster cluster;
	final AsyncCommand command;
	final EventState eventState;
	Node node;
	NettyConnection conn;
	HashedWheelTimeout timeoutTask;
	long totalDeadline;
	int state;
	int iteration;
	int commandSentCounter;
	final boolean hasTotalTimeout;
	boolean usingSocketTimeout;
	boolean eventReceived;
	boolean connectInProgress;

	public NettyCommand(NettyEventLoop loop, Cluster cluster, AsyncCommand command) {
		this.eventLoop = loop;
		this.cluster = cluster;
		this.eventState = cluster.eventState[loop.index];
		this.command = command;
		command.bufferQueue = loop.bufferQueue;
		hasTotalTimeout = command.totalTimeout > 0;

		if (eventLoop.eventLoop.inEventLoop() && eventState.errors < 5) {
			// We are already in event loop thread, so start processing.
			run();
		}
		else {
			if (hasTotalTimeout) {
				totalDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.totalTimeout);
			}
			state = AsyncCommand.REGISTERED;
			eventLoop.execute(this);
		}
	}

	// Batch retry constructor.
	public NettyCommand(NettyCommand other, AsyncCommand command, long deadline) {
		this.eventLoop = other.eventLoop;
		this.cluster = other.cluster;
		this.command = command;
		this.eventState = other.eventState;
		this.totalDeadline = other.totalDeadline;
		this.iteration = other.iteration;
		this.commandSentCounter = other.commandSentCounter;
		this.hasTotalTimeout = other.hasTotalTimeout;
		this.usingSocketTimeout = other.usingSocketTimeout;

		command.bufferQueue = eventLoop.bufferQueue;

		// We are already in event loop thread, so start processing now.
		if (eventState.pending++ == -1) {
			eventState.pending = -1;
			eventState.errors++;
			state = AsyncCommand.COMPLETE;
			notifyFailure(new AerospikeException("Cluster has been closed"));
			return;
		}

		if (deadline > 0) {
			timeoutTask = eventLoop.timer.addTimeout(this, deadline);
		}

		if (eventLoop.maxCommandsInProcess > 0) {
			// Delay queue takes precedence over new commands.
			eventLoop.executeFromDelayQueue();

			// Handle new command.
			if (eventLoop.pending >= eventLoop.maxCommandsInProcess) {
				// Pending queue full. Append new command to delay queue.
				if (eventLoop.maxCommandsInQueue > 0 && eventLoop.delayQueue.size() >= eventLoop.maxCommandsInQueue) {
					queueError(new AerospikeException.AsyncQueueFull());
					return;
				}
				eventLoop.delayQueue.addLast(this);
				state = AsyncCommand.DELAY_QUEUE;
				return;
			}
		}
		eventLoop.pending++;
		executeCommand();
	}

	@Override
	public void run() {
		if (eventState.pending++ == -1) {
			eventState.pending = -1;
			eventState.errors++;
			state = AsyncCommand.COMPLETE;
			notifyFailure(new AerospikeException("Cluster has been closed"));
			return;
		}

		long currentTime = 0;

		if (hasTotalTimeout) {
			currentTime = System.nanoTime();

			if (state == AsyncCommand.REGISTERED) {
				// Command was queued to event loop thread.
				if (currentTime >= totalDeadline) {
					// Command already timed out.
					queueError(new AerospikeException.Timeout(command.policy, true));
					return;
				}
			}
			else {
				totalDeadline = currentTime + TimeUnit.MILLISECONDS.toNanos(command.totalTimeout);
			}
		}

		if (eventLoop.maxCommandsInProcess > 0) {
			// Delay queue takes precedence over new commands.
			eventLoop.executeFromDelayQueue();

			// Handle new command.
			if (eventLoop.pending >= eventLoop.maxCommandsInProcess) {
				// Pending queue full. Append new command to delay queue.
				if (eventLoop.maxCommandsInQueue > 0 && eventLoop.delayQueue.size() >= eventLoop.maxCommandsInQueue) {
					queueError(new AerospikeException.AsyncQueueFull());
					return;
				}
				eventLoop.delayQueue.addLast(this);

				if (hasTotalTimeout) {
					timeoutTask = eventLoop.timer.addTimeout(this, totalDeadline);
				}
				state = AsyncCommand.DELAY_QUEUE;
				return;
			}
		}

		if (hasTotalTimeout) {
			long deadline;

			if (command.socketTimeout > 0) {
				deadline = currentTime + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

				if (deadline < totalDeadline) {
					usingSocketTimeout = true;
				}
				else {
					deadline = totalDeadline;
				}
			}
			else {
				deadline = totalDeadline;
			}
			timeoutTask = eventLoop.timer.addTimeout(this, deadline);
		}
		else if (command.socketTimeout > 0) {
			usingSocketTimeout = true;
			timeoutTask = eventLoop.timer.addTimeout(this, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout));
		}

		eventLoop.pending++;
		executeCommand();
	}

	private final void queueError(AerospikeException ae) {
		eventState.pending--;
		eventState.errors++;
		state = AsyncCommand.COMPLETE;
		notifyFailure(ae);
	}

	final void executeCommandFromDelayQueue() {
		if (command.socketTimeout > 0) {
			long socketDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

			if (hasTotalTimeout) {
				if (socketDeadline < totalDeadline) {
					// Transition from total timer to socket timer.
					timeoutTask.cancel();
					usingSocketTimeout = true;
					eventLoop.timer.restoreTimeout(timeoutTask, socketDeadline);
				}
			}
			else {
				usingSocketTimeout = true;
				timeoutTask = eventLoop.timer.addTimeout(this, socketDeadline);
			}
		}
		eventLoop.pending++;
		executeCommand();
	}

	private void executeCommand() {
		state = AsyncCommand.CHANNEL_INIT;
		iteration++;

		try {
			node = command.getNode(cluster);
			conn = (NettyConnection)node.getAsyncConnection(eventState.index, null);

			if (conn != null) {
				InboundHandler handler = (InboundHandler)conn.channel.pipeline().last();
				handler.command = this;
				writeCommand();
				return;
			}

			connectInProgress = true;
			final int itr = iteration;
			final InboundHandler handler = new InboundHandler();
			handler.command = this;

			Bootstrap b = new Bootstrap();
			b.group(eventLoop.eventLoop);

			if (eventLoop.parent.isEpoll) {
				b.channel(EpollSocketChannel.class);
			}
			else {
				b.channel(NioSocketChannel.class);
			}
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.AUTO_READ, false);

			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) {
					if (! (state == AsyncCommand.CHANNEL_INIT && itr == iteration)) {
						// State mismatch. Timeout probably occurred.
						// Connection count has already been decremented.
						// Just close channel.
						ch.close();
						connectInProgress = false;
						return;
					}

					state = AsyncCommand.CONNECT;
					conn = new NettyConnection(ch);
					node.connectionOpened(eventLoop.index);
					connectInProgress = false;
					ChannelPipeline p = ch.pipeline();

					if (eventLoop.parent.sslContext != null && !eventLoop.parent.tlsPolicy.forLoginOnly) {
						state = AsyncCommand.TLS_HANDSHAKE;
						//InetSocketAddress address = node.getAddress();
						//p.addLast(eventLoop.parent.sslContext.newHandler(ch.alloc(), address.getHostString(), address.getPort()));
						p.addLast(eventLoop.parent.sslContext.newHandler(ch.alloc()));
					}
					p.addLast(handler);
				}
			});
			b.connect(node.getAddress());
			eventState.errors = 0;
		}
		catch (AerospikeException.Connection ac) {
			eventState.errors++;
			onNetworkError(ac);
		}
		catch (Exception e) {
			// Fail without retry on unknown errors.
			eventState.errors++;
			fail();
			notifyFailure(new AerospikeException(e));
			eventLoop.tryDelayQueue();
		}
	}

	private void channelActive() {
		if (cluster.getUser() != null) {
			writeAuth();
		}
		else {
			writeCommand();
		}
	}

	private void writeAuth() {
		state = AsyncCommand.AUTH_WRITE;
		command.initBuffer();

		AdminCommand admin = new AdminCommand(command.dataBuffer);
		command.dataOffset = admin.setAuthenticate(cluster, node.getSessionToken());
		writeByteBuffer();
	}

	private void writeCommand() {
		state = AsyncCommand.COMMAND_WRITE;
		command.writeBuffer();
		writeByteBuffer();
	}

	private void writeByteBuffer() {
		ByteBuf byteBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(command.dataOffset);
		byteBuffer.clear();
		byteBuffer.writeBytes(command.dataBuffer, 0, command.dataOffset);

		ChannelFuture cf = conn.channel.writeAndFlush(byteBuffer);
		cf.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				if (state == AsyncCommand.COMMAND_WRITE) {
					state = AsyncCommand.COMMAND_READ_HEADER;
					commandSentCounter++;
				}
				else {
					state = AsyncCommand.AUTH_READ_HEADER;
				}
				command.dataOffset = 0;
				// Socket timeout applies only to read events.
				// Reset event received because we are switching from a write to a read state.
				// This handles case where write succeeds and read event does not occur.  If we didn't reset,
				// the socket timeout would go through two iterations (double the timeout) because a write
				// event occurred in the first timeout period.
				eventReceived = false;
				conn.channel.config().setAutoRead(true);
			}
		});
	}

	private void read(ByteBuf byteBuffer) {
		eventReceived = true;

		try {
			switch (state) {
			case AsyncCommand.AUTH_READ_HEADER:
				readAuthHeader(byteBuffer);
				break;

			case AsyncCommand.AUTH_READ_BODY:
				readAuthBody(byteBuffer);
				break;

			case AsyncCommand.COMMAND_READ_HEADER:
				if (command.isSingle) {
					readSingleHeader(byteBuffer);
				}
				else {
					readMultiHeader(byteBuffer);
				}
				break;

			case AsyncCommand.COMMAND_READ_BODY:
				if (command.isSingle) {
					readSingleBody(byteBuffer);
				}
				else {
					readMultiBody(byteBuffer);
				}
				break;
			}
		}
		finally {
			byteBuffer.release();
		}
	}

	private void readAuthHeader(ByteBuf byteBuffer) {
		int avail = byteBuffer.readableBytes();
		int offset = command.dataOffset + avail;

		if (offset < 8) {
			byteBuffer.readBytes(command.dataBuffer, command.dataOffset, avail);
			command.dataOffset = offset;
			return;
		}

		// Process authentication header.
		byteBuffer.readBytes(command.dataBuffer, command.dataOffset, 8 - command.dataOffset);
		command.receiveSize = ((int)(Buffer.bytesToLong(command.dataBuffer, 0) & 0xFFFFFFFFFFFFL));

		if (command.receiveSize < 2 || command.receiveSize > command.dataBuffer.length) {
			throw new AerospikeException.Parse("Invalid auth receive size: " + command.receiveSize);
		}

		state = AsyncCommand.AUTH_READ_BODY;
		offset -= 8;
		command.dataOffset = offset;

		if (offset > 0) {
			byteBuffer.readBytes(command.dataBuffer, 0, offset);

			if (offset >= command.receiveSize) {
				parseAuthBody();
			}
		}
	}

	private void readAuthBody(ByteBuf byteBuffer) {
		int avail = byteBuffer.readableBytes();
		int offset = command.dataOffset + avail;

		if (offset < command.receiveSize) {
			byteBuffer.readBytes(command.dataBuffer, command.dataOffset, avail);
			command.dataOffset = offset;
			return;
		}
		parseAuthBody();
	}

	private void parseAuthBody() {
		int resultCode = command.dataBuffer[1] & 0xFF;

		if (resultCode != 0 && resultCode != ResultCode.SECURITY_NOT_ENABLED) {
			// Authentication failed. Session token probably expired.
			// Signal tend thread to perform node login, so future
			// transactions do not fail.
			node.signalLogin();

			// This is a rare event because the client tracks session
			// expiration and will relogin before session expiration.
			// Do not try to login on same socket because login can take
			// a long time and thousands of simultaneous logins could
			// overwhelm server.
			throw new AerospikeException(resultCode);
		}
		writeCommand();
	}

	private void readSingleHeader(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();
		int dataSize = command.dataOffset + readableBytes;

		if (dataSize < 8) {
			byteBuffer.readBytes(command.dataBuffer, command.dataOffset, readableBytes);
			command.dataOffset = dataSize;
			return;
		}

		dataSize = 8 - command.dataOffset;
		byteBuffer.readBytes(command.dataBuffer, command.dataOffset, dataSize);
		readableBytes -= dataSize;

		int receiveSize = command.parseProto(Buffer.bytesToLong(command.dataBuffer, 0));

		command.sizeBuffer(receiveSize);
		state = AsyncCommand.COMMAND_READ_BODY;

		dataSize = (readableBytes >= receiveSize)? receiveSize : readableBytes;
		byteBuffer.readBytes(command.dataBuffer, 0, dataSize);
		command.dataOffset = dataSize;

		if (command.dataOffset >= receiveSize) {
			parseSingleBody();
		}
	}

	private void readSingleBody(ByteBuf byteBuffer) {
		int readableBytes = byteBuffer.readableBytes();
		int needBytes = command.receiveSize - command.dataOffset;
		int dataSize = (readableBytes >= needBytes)? needBytes : readableBytes;

		byteBuffer.readBytes(command.dataBuffer, command.dataOffset, dataSize);
		command.dataOffset += dataSize;

		if (command.dataOffset >= command.receiveSize) {
			parseSingleBody();
		}
	}

	private void parseSingleBody() {
		conn.updateLastUsed();
		command.parseCommandResult();
		finish();
	}

	private final void readMultiHeader(ByteBuf byteBuffer) {
		if (! command.valid) {
			throw new AerospikeException.QueryTerminated();
		}

		int readableBytes = byteBuffer.readableBytes();
		int dataSize;

		do {
			dataSize = command.dataOffset + readableBytes;

			if (dataSize < 8) {
				byteBuffer.readBytes(command.dataBuffer, command.dataOffset, readableBytes);
				command.dataOffset = dataSize;
				return;
			}

			dataSize = 8 - command.dataOffset;
			byteBuffer.readBytes(command.dataBuffer, command.dataOffset, dataSize);
			readableBytes -= dataSize;

			int receiveSize = command.parseProto(Buffer.bytesToLong(command.dataBuffer, 0));

			if (receiveSize == 0) {
				// Read next header.
				command.dataOffset = 0;
				continue;
			}

			command.sizeBuffer(receiveSize);
			state = AsyncCommand.COMMAND_READ_BODY;

			if (readableBytes <= 0) {
				return;
			}

			dataSize = (readableBytes >= receiveSize)? receiveSize : readableBytes;
			byteBuffer.readBytes(command.dataBuffer, 0, dataSize);
			readableBytes -= dataSize;
			command.dataOffset = dataSize;

			if (command.dataOffset < receiveSize) {
				return;
			}

			conn.updateLastUsed();

			if (command.parseCommandResult()) {
				finish();
				return;
			}

			// Prepare for next group.
			state = AsyncCommand.COMMAND_READ_HEADER;
			command.dataOffset = 0;
		} while (true);
	}

	private final void readMultiBody(ByteBuf byteBuffer) {
		if (! command.valid) {
			throw new AerospikeException.QueryTerminated();
		}

		int readableBytes = byteBuffer.readableBytes();
		int needBytes = command.receiveSize - command.dataOffset;
		int dataSize = (readableBytes >= needBytes)? needBytes : readableBytes;

		byteBuffer.readBytes(command.dataBuffer, command.dataOffset, dataSize);
		command.dataOffset += dataSize;

		if (command.dataOffset < command.receiveSize) {
			return;
		}

		conn.updateLastUsed();

		if (command.parseCommandResult()) {
			finish();
			return;
		}

		// Prepare for next group.
		state = AsyncCommand.COMMAND_READ_HEADER;
		command.dataOffset = 0;
		readMultiHeader(byteBuffer);
	}

	@Override
	public final void timeout() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		long currentTime = 0;

		if (hasTotalTimeout) {
			// Check total timeout.
			currentTime = System.nanoTime();

			if (currentTime >= totalDeadline) {
				totalTimeout();
				return;
			}

			if (usingSocketTimeout) {
				// Socket idle timeout is in effect.
				if (eventReceived) {
					// Event(s) received within socket timeout period.
					eventReceived = false;

					long deadline = currentTime + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

					if (deadline >= totalDeadline) {
						// Transition to total timeout.
						deadline = totalDeadline;
						usingSocketTimeout = false;
					}
					eventLoop.timer.restoreTimeout(timeoutTask, deadline);
					return;
				}
			}
		}
		else {
			// Check socket timeout.
			if (eventReceived) {
				// Event(s) received within socket timeout period.
				eventReceived = false;

				long socketDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);
				eventLoop.timer.restoreTimeout(timeoutTask, socketDeadline);
				return;
			}
		}

		// Check maxRetries.
		if (iteration > command.maxRetries) {
			totalTimeout();
			return;
		}

		// Recover connection when possible.
		recoverConnection();

		// Attempt retry.
		long timeout = TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

		if (hasTotalTimeout) {
			long remaining = totalDeadline - currentTime;

			if (remaining <= timeout) {
				// Transition to total timeout.
				timeout = remaining;
				usingSocketTimeout = false;
			}
		}
		else {
			currentTime = System.nanoTime();
		}

		long deadline = currentTime + timeout;

		if (! command.prepareRetry(true)) {
			// Batch may be retried in separate commands.
			if (command.retryBatch(this, deadline)) {
				// Batch retried in separate commands.  Complete this command.
				timeoutTask = null;
				close();
				return;
			}
		}

		eventLoop.timer.restoreTimeout(timeoutTask, deadline);
		executeCommand();
	}

	private final void totalTimeout() {
		AerospikeException ae = new AerospikeException.Timeout(command.policy, true);

		if (state == AsyncCommand.DELAY_QUEUE) {
			// Command timed out in delay queue.
			closeFromDelayQueue();
			notifyFailure(ae);
			return;
		}

		// Recover connection when possible.
		recoverConnection();

		// Perform timeout.
		timeoutTask = null;
		close();
		notifyFailure(ae);
		eventLoop.tryDelayQueue();
	}

	private final void recoverConnection() {
		if (command.policy.timeoutDelay > 0) {
			switch (state) {
			case AsyncCommand.CONNECT:
			case AsyncCommand.TLS_HANDSHAKE:
			case AsyncCommand.AUTH_READ_HEADER:
			case AsyncCommand.AUTH_READ_BODY:
			case AsyncCommand.COMMAND_READ_HEADER:
			case AsyncCommand.COMMAND_READ_BODY:
				try {
					// Create new command to drain connection.
					new NettyRecover(this);
					// NettyRecover took ownership of dataBuffer.
					command.dataBuffer = null;
					return;
				}
				catch (Exception e) {
					Log.warn("NettyRecover failed: " + Util.getErrorMessage(e));
				}
				break;

			default:
				break;
			}
		}

		// Abort connection recovery.
		closeConnection();
	}

	protected final void finish() {
		complete();

		try {
			command.onSuccess();
		}
		catch (Exception e) {
			Log.error("onSuccess() error: " + Util.getErrorMessage(e));
		}

		eventLoop.tryDelayQueue();
	}

	protected final void onNetworkError(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		closeConnection();
		retry(ae, true);
	}

	protected final void onServerTimeout() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		putConnection();

		AerospikeException ae = new AerospikeException.Timeout(command.policy, false);
		retry(ae, false);
	}

	private final void retry(final AerospikeException ae, boolean queueCommand) {
		// Check maxRetries.
		if (iteration > command.maxRetries) {
			// Fail command.
			close();
			notifyFailure(ae);
			eventLoop.tryDelayQueue();
			return;
		}

		long currentTime = 0;

		// Check total timeout.
		if (hasTotalTimeout) {
			currentTime = System.nanoTime();

			if (currentTime >= totalDeadline) {
				// Fail command.
				close();
				notifyFailure(ae);
				eventLoop.tryDelayQueue();
				return;
			}
		}

		long deadline = totalDeadline;

		// Attempt retry.
		if (usingSocketTimeout) {
			// Socket timeout in effect.
			timeoutTask.cancel();
			long timeout = TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

			if (hasTotalTimeout) {
				long remaining = totalDeadline - currentTime;

				if (remaining <= timeout) {
					// Transition to total timeout.
					timeout = remaining;
					usingSocketTimeout = false;
				}
			}
			else {
				currentTime = System.nanoTime();
			}

			deadline = currentTime + timeout;
		}

		if (queueCommand) {
			// Retry command at the end of the queue so other commands have a
			// chance to run first.
			final long d = deadline;
			eventLoop.execute(new Runnable() {
				@Override
				public void run() {
					if (state == AsyncCommand.COMPLETE) {
						return;
					}
					retry(ae, d);
				}
			});
		}
		else {
			// Retry command immediately.
			retry(ae, deadline);
		}
	}

	private final void retry(AerospikeException ae, long deadline) {
		if (! command.prepareRetry(ae.getResultCode() != ResultCode.SERVER_NOT_AVAILABLE)) {
			// Batch may be retried in separate commands.
			if (command.retryBatch(this, deadline)) {
				// Batch retried in separate commands.  Complete this command.
				close();
				return;
			}
		}

		if (usingSocketTimeout) {
			eventLoop.timer.restoreTimeout(timeoutTask, deadline);
		}
		executeCommand();
	}

	protected final void onApplicationError(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		if (ae.keepConnection()) {
			complete();
		}
		else {
			// Close socket to flush out possible garbage.
			fail();
		}

		notifyFailure(ae);
		eventLoop.tryDelayQueue();
	}

	private final void notifyFailure(AerospikeException ae) {
		try {
			ae.setNode(node);
			ae.setPolicy(command.policy);
			ae.setIteration(iteration);
			ae.setInDoubt(command.isWrite(), commandSentCounter);

			if (Log.debugEnabled()) {
				Command.LogPolicy(command.policy);
			}
			command.onFailure(ae);
		}
		catch (Exception e) {
			Log.error("onFailure() error: " + Util.getErrorMessage(e));
		}
	}

	private final void complete() {
		putConnection();
		close();
	}

	private final void putConnection() {
		conn.channel.config().setAutoRead(false);
		InboundHandler handler = (InboundHandler)conn.channel.pipeline().last();
		handler.command = null;
		node.putAsyncConnection(conn, eventState.index);
	}

	private final void fail() {
		closeConnection();
		close();
	}

	private final void closeConnection() {
		if (conn != null) {
			node.closeAsyncConnection(conn, eventState.index);
			conn = null;
		}
		else if (connectInProgress) {
			node.decrAsyncConnection(eventState.index);
			connectInProgress = false;
		}
	}

	private final void closeFromDelayQueue() {
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
	}

	private final void close() {
		if (timeoutTask != null) {
			timeoutTask.cancel();
		}
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
		eventLoop.pending--;
	}

	static class InboundHandler extends ChannelInboundHandlerAdapter {
		NettyCommand command;

		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			// Mark connection ready in regular (non TLS) mode.
			// Otherwise, wait for TLS handshake to complete.
			if (command.state == AsyncCommand.CONNECT) {
				command.channelActive();
			}
		}

	    @Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg) {
	    	command.read((ByteBuf)msg);
	    }

	    @Override
	    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (! (evt instanceof SslHandshakeCompletionEvent)) {
				return;
			}

			Throwable cause = ((SslHandshakeCompletionEvent)evt).cause();

			if (cause != null) {
				throw new AerospikeException("TLS connect failed: " + cause.getMessage(), cause);
			}

			TlsPolicy tlsPolicy = command.eventLoop.parent.tlsPolicy;

			String tlsName = command.node.getHost().tlsName;
			SSLSession session = ((SslHandler)ctx.pipeline().first()).engine().getSession();
			X509Certificate cert = (X509Certificate)session.getPeerCertificates()[0];

			Connection.validateServerCertificate(tlsPolicy, tlsName, cert);

			if (command.state == AsyncCommand.TLS_HANDSHAKE) {
				command.channelActive();
			}
	    }

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			if (command == null) {
				Log.error("Connection exception: " + Util.getErrorMessage(cause));
				return;
			}

			if (cause instanceof AerospikeException.Connection) {
	        	command.onNetworkError((AerospikeException.Connection)cause);
			}
			else if (cause instanceof AerospikeException) {
				AerospikeException ae = (AerospikeException)cause;

	        	if (ae.getResultCode() == ResultCode.TIMEOUT) {
	        		// Go through retry logic on server timeout
	        		command.onServerTimeout();
	        	}
	        	else {
		        	command.onApplicationError(ae);
	        	}
			}
			else if (cause instanceof IOException) {
	        	command.onNetworkError(new AerospikeException(cause));
			}
			else {
	        	command.onApplicationError(new AerospikeException(cause));
			}
		}
	}
}
