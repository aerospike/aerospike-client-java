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
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.util.Util;

public final class NioCommand implements INioCommand, Runnable, TimerTask {

	final NioEventLoop eventLoop;
	final Cluster cluster;
	final AsyncCommand command;
	final EventState eventState;
	final HashedWheelTimeout timeoutTask;
	TimeoutState timeoutState;
	Node node;
	NioConnection conn;
	ByteBuffer byteBuffer;
	long begin;
	long totalDeadline;
	long bytesOut;
	int state;
	int iteration;
	final boolean metricsEnabled;
	final boolean hasTotalTimeout;
	boolean usingSocketTimeout;
	boolean eventReceived;

	public NioCommand(NioEventLoop eventLoop, Cluster cluster, AsyncCommand command) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
		this.command = command;
		this.eventState = cluster.eventState[eventLoop.index];
		this.timeoutTask = new HashedWheelTimeout(this);
		command.bufferQueue = eventLoop.bufferQueue;
		this.metricsEnabled = cluster.metricsEnabled;
		this.hasTotalTimeout = command.totalTimeout > 0;

		if (eventLoop.thread == Thread.currentThread() && eventState.errors < 5) {
			// We are already in event loop thread, so start processing.
			run();
		}
		else {
			// Send command through queue so it can be executed in event loop thread.
			if (hasTotalTimeout) {
				totalDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.totalTimeout);
			}
			state = AsyncCommand.REGISTERED;
			eventLoop.execute(this);
		}
	}

	// Batch retry constructor.
	public NioCommand(NioCommand other, AsyncCommand command, long deadline) {
		this.eventLoop = other.eventLoop;
		this.cluster = other.cluster;
		this.command = command;
		this.eventState = other.eventState;
		this.timeoutTask = new HashedWheelTimeout(this);
		this.totalDeadline = other.totalDeadline;
		this.iteration = other.iteration;
		this.metricsEnabled = cluster.metricsEnabled;
		this.hasTotalTimeout = other.hasTotalTimeout;
		this.usingSocketTimeout = other.usingSocketTimeout;

		command.bufferQueue = eventLoop.bufferQueue;

		// We are already in event loop thread, so start processing now.
		if (eventState.closed) {
			queueError(new AerospikeException("Cluster has been closed"));
			return;
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

				if (deadline > 0) {
					eventLoop.timer.addTimeout(timeoutTask, deadline);
				}
				state = AsyncCommand.DELAY_QUEUE;
				return;
			}
		}
		eventState.pending++;
		eventLoop.pending++;
		executeCommand(deadline, TimeoutState.BATCH_RETRY);
	}

	@Override
	public void run() {
		if (eventState.closed) {
			queueError(new AerospikeException("Cluster has been closed"));
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
					eventLoop.timer.addTimeout(timeoutTask, totalDeadline);
				}
				state = AsyncCommand.DELAY_QUEUE;
				return;
			}
		}

		long deadline = totalDeadline;

		if (hasTotalTimeout) {
			if (command.socketTimeout > 0) {
				long socketDeadline = currentTime + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

				if (socketDeadline < totalDeadline) {
					usingSocketTimeout = true;
					deadline = socketDeadline;
				}
			}
		}
		else if (command.socketTimeout > 0) {
			usingSocketTimeout = true;
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);
		}

		eventState.pending++;
		eventLoop.pending++;
		executeCommand(deadline, TimeoutState.REGISTERED);
	}

	private final void queueError(AerospikeException ae) {
		eventState.errors++;
		state = AsyncCommand.COMPLETE;
		notifyFailure(ae);
	}

	final void executeCommandFromDelayQueue() {
		long deadline = totalDeadline;

		if (command.socketTimeout > 0) {
			long socketDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.socketTimeout);

			if (hasTotalTimeout) {
				if (socketDeadline < totalDeadline) {
					// Transition from total timer to socket timer.
					timeoutTask.cancel();
					usingSocketTimeout = true;
					deadline = socketDeadline;
				}
			}
			else {
				usingSocketTimeout = true;
				deadline = socketDeadline;
			}
		}
		eventState.pending++;
		eventLoop.pending++;
		executeCommand(deadline, TimeoutState.DELAY_QUEUE);
	}

	protected final void executeCommand(long deadline, int tstate) {
		state = AsyncCommand.CONNECT;
		bytesOut = 0;
		iteration++;

		try {
			node = command.getNode(cluster);
			node.validateErrorCount();

			if (metricsEnabled) {
				begin = System.nanoTime();
			}

			byteBuffer = eventLoop.getByteBuffer();
			conn = (NioConnection)node.getAsyncConnection(eventLoop.index, byteBuffer);

			if (conn != null) {
				setTimeoutTask(deadline, tstate);
				conn.attach(this);
				writeCommand();
				return;
			}

			try {
				if (command.policy.connectTimeout > 0) {
					timeoutState = new TimeoutState(deadline, tstate);
					deadline = timeoutState.start + TimeUnit.MILLISECONDS.toNanos(command.policy.connectTimeout);
					timeoutTask.cancel();
					eventLoop.timer.addTimeout(timeoutTask, deadline);
				}
				else {
					setTimeoutTask(deadline, tstate);
				}
				conn = new NioConnection(node.getAddress());
				node.connectionOpened(eventLoop.index);
			}
			catch (Throwable e) {
				node.decrAsyncConnection(eventLoop.index);
				throw e;
			}

			conn.registerConnect(eventLoop, this);
			eventState.errors = 0;
		}
		catch (AerospikeException.Connection ac) {
			eventState.errors++;
			onNetworkError(ac, true);
		}
		catch (AerospikeException.Backoff ab) {
			eventState.errors++;
			retry(ab, true);
		}
		catch (AerospikeException ae) {
			// Fail without retry on non-connection errors.
			eventState.errors++;
			fail();
			notifyFailure(ae);
			eventLoop.tryDelayQueue();
		}
		catch (IOException ioe) {
			eventState.errors++;
			onNetworkError(new AerospikeException.Connection(ioe), true);
		}
		catch (Throwable e) {
			// Fail without retry on unknown errors.
			eventState.errors++;
			fail();
			notifyFailure(new AerospikeException(e));
			eventLoop.tryDelayQueue();
		}
	}

	private final void setTimeoutTask(long deadline, int tstate) {
		if (deadline <= 0) {
			return;
		}

		switch(tstate) {
		case TimeoutState.REGISTERED:
		case TimeoutState.BATCH_RETRY:
		case TimeoutState.TIMEOUT:
			eventLoop.timer.addTimeout(timeoutTask, deadline);
			break;

		case TimeoutState.DELAY_QUEUE:
		case TimeoutState.RETRY:
			// Only set timeoutTask when not active.
			if (! timeoutTask.active()) {
				eventLoop.timer.addTimeout(timeoutTask, deadline);
			}
			break;

		default:
			break;
		}
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
		catch (AerospikeException.Connection ac) {
			onNetworkError(ac, false);
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.TIMEOUT) {
				onServerTimeout();
			}
			else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
				onDeviceOverload(ae);
			}
			else if (ae.getResultCode() == ResultCode.KEY_BUSY) {
				onKeyBusy(ae);
			}
			else {
				onApplicationError(ae);
			}
		}
		catch (IOException ioe) {
			onNetworkError(new AerospikeException.Connection(ioe), false);
		}
		catch (Throwable e) {
			onApplicationError(new AerospikeException(e));
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
		connectComplete();
	}

	private void connectComplete() throws IOException {
		if (metricsEnabled) {
			addLatency(LatencyType.CONN);
		}

		if (timeoutState != null) {
			restoreTimeout();
		}
		writeCommand();
	}

	private final void restoreTimeout() {
		// Switch from connectTimeout back to previous timeout.
		timeoutTask.cancel();

		long elapsed = System.nanoTime() - timeoutState.start;

		if (timeoutState.deadline > 0) {
			timeoutState.deadline += elapsed;
		}

		if (totalDeadline > 0) {
			totalDeadline += elapsed;
		}
		setTimeoutTask(timeoutState.deadline, timeoutState.state);
		timeoutState = null;
	}

	private final void writeAuth(byte[] token) throws IOException {
		state = AsyncCommand.AUTH_WRITE;
		command.initBuffer();

		AdminCommand admin = new AdminCommand(command.dataBuffer);
		command.dataOffset = admin.setAuthenticate(cluster, token);
		byteBuffer.clear();
		byteBuffer.put(command.dataBuffer, 0, command.dataOffset);
		byteBuffer.flip();
		command.putBuffer();

		if (conn.write(byteBuffer)) {
			bytesOut += command.dataOffset;
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.AUTH_READ_HEADER;
			// Socket timeout applies only to read events.
			// Reset event received because we are switching from a write to a read state.
			// This handles case where write succeeds and read event does not occur.  If we didn't reset,
			// the socket timeout would go through two iterations (double the timeout) because a write
			// event occurred in the first timeout period.
			eventReceived = false;
			conn.registerRead();
		}
		else {
			conn.registerWrite();
		}
	}

	private final void writeCommand() throws IOException {
		state = AsyncCommand.COMMAND_WRITE;
		command.writeBuffer();

		if (command.dataOffset > byteBuffer.capacity()) {
			byteBuffer = NioEventLoop.createByteBuffer(command.dataOffset);
		}

		byteBuffer.clear();
		byteBuffer.put(command.dataBuffer, 0, command.dataOffset);
		byteBuffer.flip();
		command.putBuffer();

		if (conn.write(byteBuffer)) {
			bytesOut += command.dataOffset;
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.COMMAND_READ_HEADER;
			command.commandSentCounter++;
			eventReceived = false;
			conn.registerRead();
		}
		else {
			conn.registerWrite();
		}
	}

	protected final void write() throws IOException {
		if (conn.write(byteBuffer)) {
			bytesOut += command.dataOffset;
			byteBuffer.clear();
			byteBuffer.limit(8);

			if (state == AsyncCommand.COMMAND_WRITE) {
				state = AsyncCommand.COMMAND_READ_HEADER;
				command.commandSentCounter++;
			}
			else {
				state = AsyncCommand.AUTH_READ_HEADER;
			}
			eventReceived = false;
			conn.registerRead();
		}
	}

	protected final void read() throws IOException {
		eventReceived = true;

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
			connectComplete();
			break;

		case AsyncCommand.COMMAND_READ_HEADER:
			if (command.isSingle) {
				readSingleHeader();
			}
			else {
				readMultiHeader();
			}
			break;

		case AsyncCommand.COMMAND_READ_BODY:
			if (command.isSingle) {
				readSingleBody();
			}
			else {
				readMultiBody();
			}
			break;
		}
	}

	private final void readAuthHeader() {
		byteBuffer.position(0);
		command.receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));

		if (command.receiveSize < 2 || command.receiveSize > byteBuffer.capacity()) {
			throw new AerospikeException.Parse("Invalid auth receive size: " + command.receiveSize);
		}
		byteBuffer.clear();
		byteBuffer.limit(command.receiveSize);
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

	private final void readSingleHeader() throws IOException {
		byteBuffer.position(0);

		int receiveSize = command.parseProto(byteBuffer.getLong());

		if (receiveSize <= byteBuffer.capacity()) {
			byteBuffer.clear();
		}
		else {
			byteBuffer = NioEventLoop.createByteBuffer(receiveSize);
		}
		byteBuffer.limit(receiveSize);
		state = AsyncCommand.COMMAND_READ_BODY;

		if (conn.read(byteBuffer)) {
			readSingleBody();
		}
	}

	private final void readSingleBody() {
		// Copy entire message to dataBuffer.
		command.sizeBuffer(command.receiveSize);
		byteBuffer.position(0);
		byteBuffer.get(command.dataBuffer, 0, command.receiveSize);
		conn.updateLastUsed();
		command.parseCommandResult();
		command.putBuffer();
		finish();
	}

	private final void readMultiHeader() throws IOException {
		if (! command.valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (! parseGroupHeader()) {
			return;
		}

		if (! conn.read(byteBuffer)) {
			return;
		}

		readMultiBody();
	}

	private final void readMultiBody() throws IOException {
		if (! command.valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (! parseGroupBody()) {
			return;
		}

		// In the interest of fairness, only one group of records should be read at a time.
		// There is, however, one exception.  The server returns the end code in a separate
		// group that only has one dummy record header.  Therefore, we continue to read
		// this small group in order to avoid having to wait one more async iteration just
		// to find out the batch/scan/query has already ended.
		if (! conn.read(byteBuffer)) {
			return;
		}

		if (! parseGroupHeader()) {
			return;
		}

		if (command.receiveSize == Command.MSG_REMAINING_HEADER_SIZE) {
			// We may be at end.  Read ahead and parse.
			if (! conn.read(byteBuffer)) {
				return;
			}
			parseGroupBody();
		}
	}

	private final boolean parseGroupHeader() {
		byteBuffer.position(0);

		int receiveSize = command.parseProto(byteBuffer.getLong());

		if (receiveSize <= 0) {
			// Received zero length block. Read next header.
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.COMMAND_READ_HEADER;
			return false;
		}

		command.sizeBuffer(receiveSize);
		command.dataOffset = 0;
		byteBuffer.clear();

		if (receiveSize < byteBuffer.capacity()) {
			byteBuffer.limit(receiveSize);
		}
		state = AsyncCommand.COMMAND_READ_BODY;
		return true;
	}

	private final boolean parseGroupBody() throws IOException {
		do {
			// Copy byteBuffer to byte[].
			byteBuffer.position(0);
			byteBuffer.get(command.dataBuffer, command.dataOffset, byteBuffer.limit());
			command.dataOffset += byteBuffer.limit();
			byteBuffer.clear();

			if (command.dataOffset >= command.receiveSize) {
				conn.updateLastUsed();

				if (command.parseCommandResult()) {
					finish();
					return false;
				}
				// Prepare for next group.
				byteBuffer.limit(8);
				command.dataOffset = 0;
				state = AsyncCommand.COMMAND_READ_HEADER;
				return true;
			}
			else {
				int remaining = command.receiveSize - command.dataOffset;

				if (remaining < byteBuffer.capacity()) {
					byteBuffer.limit(remaining);
				}

				if (! conn.read(byteBuffer)) {
					return false;
				}
			}
		} while (true);
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
					eventLoop.timer.addTimeout(timeoutTask, deadline);
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
				eventLoop.timer.addTimeout(timeoutTask, socketDeadline);
				return;
			}
		}

		// Check maxRetries.
		if (iteration > command.maxRetries) {
			totalTimeout();
			return;
		}

		// Increment node's timeout counter.
		node.addTimeout(command.namespace);

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
				close();
				return;
			}
		}

		cluster.addRetry();
		executeCommand(deadline, TimeoutState.TIMEOUT);
	}

	private final void totalTimeout() {
		AerospikeException ae = new AerospikeException.Timeout(command.policy, true);

		if (state == AsyncCommand.DELAY_QUEUE) {
			// Command timed out in delay queue.
			if (metricsEnabled) {
				cluster.addDelayQueueTimeout();
			}
			closeFromDelayQueue();
			notifyFailure(ae);
			return;
		}

		// Increment node's timeout counter.
		node.addTimeout(command.namespace);

		// Recover connection when possible.
		recoverConnection();

		// Perform timeout.
		close();
		notifyFailure(ae);
		eventLoop.tryDelayQueue();
	}

	private final void recoverConnection() {
		if (command.policy.timeoutDelay > 0 && (
			state == AsyncCommand.COMMAND_READ_HEADER || state == AsyncCommand.COMMAND_READ_BODY ||
			state == AsyncCommand.AUTH_READ_HEADER || state == AsyncCommand.AUTH_READ_BODY)) {
			// Create new command to drain connection with existing byteBuffer.
			new NioRecover(this);
			// NioRecover took ownership of connection.
			conn = null;
		}
		else {
			closeConnection();

			// Put byteBuffer back into pool.
			if (byteBuffer != null) {
				eventLoop.putByteBuffer(byteBuffer);
			}
		}
		byteBuffer = null;
	}

	protected final void finish() {
		complete();

		if (metricsEnabled) {
			LatencyType type = command.getLatencyType();

			if (type != LatencyType.NONE) {
				addLatency(type);
			}
		}

		try {
			command.onSuccess();
		}
		catch (Throwable e) {
			Log.error("onSuccess() error: " + Util.getErrorMessage(e));
		}

		eventLoop.tryDelayQueue();
	}

	private void addLatency(LatencyType type) {
		long elapsed = System.nanoTime() - begin;
		node.addLatency(command.namespace, type, elapsed);
	}

	protected final void onNetworkError(AerospikeException ae, boolean queueCommand) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		addError();
		closeConnection();
		retry(ae, queueCommand);
	}

	protected final void onServerTimeout() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		node.addTimeout(command.namespace);
		addBytesInOut();
		conn.unregister();
		node.putAsyncConnection(conn, eventLoop.index);

		AerospikeException ae = new AerospikeException.Timeout(command.policy, false);
		retry(ae, false);
	}

	protected final void onDeviceOverload(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		addError();
		addBytesInOut();
		conn.unregister();
		node.putAsyncConnection(conn, eventLoop.index);
		node.incrErrorRate();
		retry(ae, false);
	}

	private void onKeyBusy(AerospikeException ae) {
		node.addKeyBusy(command.namespace);
		onApplicationError(ae);
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
		command.onRetryException(node, iteration, ae);

		if (! command.prepareRetry(ae.getResultCode() != ResultCode.SERVER_NOT_AVAILABLE)) {
			// Batch may be retried in separate commands.
			if (command.retryBatch(this, deadline)) {
				// Batch retried in separate commands.  Complete this command.
				close();
				return;
			}
		}

		cluster.addRetry();
		executeCommand(deadline, TimeoutState.RETRY);
	}

	protected final void onApplicationError(AerospikeException ae) {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}

		addError();

		if (ae.keepConnection()) {
			// Put connection back in pool.
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
			command.onFinalException(node, iteration, ae);
		}
		catch (Throwable e) {
			Log.error("onFinalException() error: " + Util.getErrorMessage(e));
		}
	}

	private void addError() {
		// Some errors can occur before the node is assigned.
		if (node != null) {
			node.addError(command.namespace);
		}
	}

	private final void complete() {
		conn.unregister();
		addBytesInOut();
		node.putAsyncConnection(conn, eventLoop.index);
		close();
	}

	private final void fail() {
		closeConnection();
		close();
	}

	private final void closeConnection() {
		if (conn != null) {
			addBytesInOut();
			node.closeAsyncConnection(conn, eventLoop.index);
			conn = null;
		}
	}

	private final void addBytesInOut() {
		if (node.areMetricsEnabled()) {
			node.addBytesIn(command.namespace, conn.resetBytesIn());
			node.addBytesOut(command.namespace, bytesOut);
		}
		bytesOut = 0;
	}

	private final void closeFromDelayQueue() {
		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
	}

	private final void close() {
		timeoutTask.cancel();

		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
		eventLoop.pending--;
	}
}
