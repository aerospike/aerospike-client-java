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
import com.aerospike.client.util.Util;

public final class NioCommand implements INioCommand, Runnable, TimerTask {

	final NioEventLoop eventLoop;
	final Cluster cluster;
	final AsyncCommand command;
	final EventState eventState;
	Node node;
	NioConnection conn;
	ByteBuffer byteBuffer;
	HashedWheelTimeout timeoutTask;
	long totalDeadline;
	int state;
	int iteration;
	int commandSentCounter;
	final boolean hasTotalTimeout;
	boolean usingSocketTimeout;
	boolean eventReceived;

	public NioCommand(NioEventLoop eventLoop, Cluster cluster, AsyncCommand command) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
		this.eventState = cluster.eventState[eventLoop.index];
		this.command = command;
		command.bufferQueue = eventLoop.bufferQueue;
		hasTotalTimeout = command.totalTimeout > 0;

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

	protected final void executeCommand() {
		state = AsyncCommand.CONNECT;
		iteration++;

		try {
			node = command.getNode(cluster);
			byteBuffer = eventLoop.getByteBuffer();
			conn = (NioConnection)node.getAsyncConnection(eventLoop.index, byteBuffer);

			if (conn != null) {
				conn.attach(this);
				writeCommand();
				return;
			}

			try {
				conn = new NioConnection(node.getAddress(), cluster.maxSocketIdleNanos);
				node.connectionOpened(eventLoop.index);
			}
			catch (Exception e) {
				node.decrAsyncConnection(eventLoop.index);
				throw e;
			}

			state = (cluster.getUser() != null) ? AsyncCommand.AUTH_WRITE : AsyncCommand.COMMAND_WRITE;
			conn.registerConnect(this);
			eventState.errors = 0;
		}
		catch (AerospikeException.Connection ac) {
			eventState.errors++;
			onNetworkError(ac, true);
		}
		catch (IOException ioe) {
			eventState.errors++;
			onNetworkError(new AerospikeException.Connection(ioe), true);
		}
		catch (Exception e) {
			// Fail without retry on unknown errors.
			eventState.errors++;
			fail();
			notifyFailure(new AerospikeException(e));
			eventLoop.tryDelayQueue();
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
        		// Go through retry logic on server timeout
        		onServerTimeout();
        	}
        	else {
        		onApplicationError(ae);
        	}
        }
        catch (IOException ioe) {
        	onNetworkError(new AerospikeException.Connection(ioe), false);
        }
        catch (Exception e) {
			onApplicationError(new AerospikeException(e));
        }
    }

	protected final void finishConnect() throws IOException {
		conn.finishConnect();

		if (state == AsyncCommand.AUTH_WRITE) {
			writeAuth();
		}
		else {
			writeCommand();
		}
	}

	private final void writeAuth() throws IOException {
		command.initBuffer();

		AdminCommand admin = new AdminCommand(command.dataBuffer);
		command.dataOffset = admin.setAuthenticate(cluster, node.getSessionToken());
		byteBuffer.clear();
		byteBuffer.put(command.dataBuffer, 0, command.dataOffset);
		byteBuffer.flip();
		command.putBuffer();

		if (conn.write(byteBuffer)) {
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
			state = AsyncCommand.AUTH_WRITE;
			conn.registerWrite();
		}
	}

	private final void writeCommand() throws IOException {
		command.writeBuffer();

		if (command.dataOffset > byteBuffer.capacity()) {
			byteBuffer = NioEventLoop.createByteBuffer(command.dataOffset);
		}

		byteBuffer.clear();
		byteBuffer.put(command.dataBuffer, 0, command.dataOffset);
		byteBuffer.flip();
		command.putBuffer();

		if (conn.write(byteBuffer)) {
			byteBuffer.clear();
			byteBuffer.limit(8);
			state = AsyncCommand.COMMAND_READ_HEADER;
			commandSentCounter++;
			eventReceived = false;
			conn.registerRead();
		}
		else {
			state = AsyncCommand.COMMAND_WRITE;
			conn.registerWrite();
		}
	}

	protected final void write() throws IOException {
		if (conn.write(byteBuffer)) {
			byteBuffer.clear();
			byteBuffer.limit(8);

			if (state == AsyncCommand.COMMAND_WRITE) {
				state = AsyncCommand.COMMAND_READ_HEADER;
				commandSentCounter++;
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
			writeCommand();
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
			// transactions do not fail.
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
		if (command.policy.timeoutDelay > 0 && (
			state == AsyncCommand.COMMAND_READ_HEADER || state == AsyncCommand.COMMAND_READ_BODY ||
			state == AsyncCommand.AUTH_READ_HEADER || state == AsyncCommand.AUTH_READ_BODY)) {
			// Create new command to drain connection with existing byteBuffer.
			new NioRecover(this);
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

		try {
			command.onSuccess();
		}
		catch (Exception e) {
			Log.error("onSuccess() error: " + Util.getErrorMessage(e));
		}

		eventLoop.tryDelayQueue();
	}

	protected final void onNetworkError(AerospikeException ae, boolean queueCommand) {
		closeConnection();
		retry(ae, queueCommand);
	}

	protected final void onServerTimeout() {
		conn.unregister();
		node.putAsyncConnection(conn, eventLoop.index);

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
		conn.unregister();
		node.putAsyncConnection(conn, eventLoop.index);
		close();
	}

	private final void fail() {
		closeConnection();
		close();
	}

	private final void closeConnection() {
		if (conn != null) {
			node.closeAsyncConnection(conn, eventLoop.index);
			conn = null;
		}
	}

	private final void closeFromDelayQueue() {
		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
	}

	private final void close() {
		if (timeoutTask != null) {
			timeoutTask.cancel();
		}

		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
		command.putBuffer();
		state = AsyncCommand.COMPLETE;
		eventState.pending--;
		eventLoop.pending--;
	}
}
