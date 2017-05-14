/*
 * Copyright 2012-2017 Aerospike, Inc.
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
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.async.HashedWheelTimer.HashedWheelTimeout;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.util.Util;

public class NioCommand implements Runnable, TimerTask {

	final NioEventLoop eventLoop;
	final AsyncCommand command;
	final EventState eventState;
	Node node;
	NioConnection conn;
	ByteBuffer byteBuffer;
	HashedWheelTimeout timeoutTask;
	long deadline;
	int state;
	int iterations;
	int receiveSize;
	boolean timeoutDelay;
	boolean isClose;

	public NioCommand(NioEventLoop eventLoop, Cluster cluster, AsyncCommand command) {
		this.eventLoop = eventLoop;
		this.eventState = cluster.eventState[eventLoop.index];		
		this.command = command;
		command.cluster = cluster;
		command.bufferQueue = eventLoop.bufferQueue;
		
		int timeout = command.policy.timeout;
		deadline = (timeout > 0)? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) : 0;
		
		if (eventLoop == Thread.currentThread() && eventLoop.errors < 5) {
			// We are already in event loop thread, so start processing.
			run();		
		}
		else {
			// Send command through queue so it can be executed in event loop thread.
			state = AsyncCommand.REGISTERED;
			eventLoop.execute(this);
		}
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

		if (deadline > 0) {
			timeoutTask = eventLoop.timer.addTimeout(this, deadline);
		}

		executeCommand();
	}

	protected final void executeCommand() {
		try {
			node = command.getNode();
			byteBuffer = eventLoop.getByteBuffer();
			conn = (NioConnection)node.getAsyncConnection(eventLoop.index, byteBuffer);
			
			if (conn != null) {
				conn.attach(this);
				writeCommand();
				return;
			}
			
			try {
				conn = new NioConnection(node.getAddress());
			}
			catch (Exception e) {
				node.decrAsyncConnection(eventLoop.index);
				throw e;
			}
		
			state = (command.cluster.getUser() != null) ? AsyncCommand.AUTH_WRITE : AsyncCommand.COMMAND_WRITE;
			conn.registerConnect(this);
			eventLoop.errors = 0;
		}
		catch (AerospikeException.Connection ac) {
			eventLoop.errors++;
			onNetworkError(ac);
		}
		catch (IOException ioe) {
			eventLoop.errors++;
			onNetworkError(new AerospikeException(ioe));
		}
		catch (Exception e) {
			// Fail without retry on unknown errors.
			eventLoop.errors++;
			cleanup();
			notifyFailure(new AerospikeException(e));
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
		command.dataOffset = admin.setAuthenticate(command.cluster.getUser(), command.cluster.getPassword());
		byteBuffer.clear();
		byteBuffer.put(command.dataBuffer, 0, command.dataOffset);
		byteBuffer.flip();
		command.putBuffer();
		
		if (conn.write(byteBuffer)) {
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
			conn.registerRead();
		}
		else {
			state = AsyncCommand.COMMAND_WRITE;
			conn.registerWrite();
		}
	}

	protected final void write() throws IOException {
		if (conn.write(byteBuffer)) {
			state = (state == AsyncCommand.COMMAND_WRITE)? AsyncCommand.COMMAND_READ_HEADER : AsyncCommand.AUTH_READ_HEADER; 
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
			writeCommand();
			break;
			
		case AsyncCommand.COMMAND_READ_HEADER:
			if (command.single) {
				readSingleHeader();
			}
			else {
				readMultiHeader();
			}
			break;
			
		case AsyncCommand.COMMAND_READ_BODY:
			if (command.single) {
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
		receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));

		if (receiveSize < 2 || receiveSize > byteBuffer.capacity()) {
			throw new AerospikeException.Parse("Invalid auth receive size: " + receiveSize);
		}
		byteBuffer.clear();
		byteBuffer.limit(receiveSize);
		state = AsyncCommand.AUTH_READ_BODY;			
	}

	private final void readAuthBody() {
		int resultCode = byteBuffer.get(1) & 0xFF;
	
		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}
	}
	
	private final void readSingleHeader() throws IOException {
		byteBuffer.position(0);
		receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));
			        
		if (receiveSize < Command.MSG_REMAINING_HEADER_SIZE) {
			throw new AerospikeException.Parse("Invalid receive size: " + receiveSize);
		}

		if (receiveSize <= byteBuffer.capacity()) {
			byteBuffer.clear();
			byteBuffer.limit(receiveSize);
		}
		else {
			byteBuffer = NioEventLoop.createByteBuffer(receiveSize);
		}		
		state = AsyncCommand.COMMAND_READ_BODY;

		if (conn.read(byteBuffer)) {
			readSingleBody();
		}
	}

	private final void readSingleBody() {
		if (command.readAll) {
			// Copy entire message to dataBuffer.
			command.sizeBuffer(receiveSize);
			byteBuffer.position(0);
			byteBuffer.get(command.dataBuffer, 0, receiveSize);
			command.resultCode = command.dataBuffer[5] & 0xFF;
			((AsyncSingleCommand)command).parseResult();
			command.putBuffer();
		}
		else {
			command.resultCode = byteBuffer.get(5) & 0xFF;
			((AsyncSingleCommand)command).parseResult();
		}
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
		
		if (receiveSize == Command.MSG_REMAINING_HEADER_SIZE) {
			// We may be at end.  Read ahead and parse.
			if (! conn.read(byteBuffer)) {
				return;
			}
			parseGroupBody();
		}		
	}

	private final boolean parseGroupHeader() {
		byteBuffer.position(0);
		receiveSize = ((int) (byteBuffer.getLong() & 0xFFFFFFFFFFFFL));
			        
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
			
			if (command.dataOffset >= receiveSize) {
				if (((AsyncMultiCommand)command).parseGroup(receiveSize)) {
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
				int remaining = receiveSize - command.dataOffset;
					
				if (remaining < byteBuffer.capacity()) {
					byteBuffer.limit(remaining);
				}
				
				if (! conn.read(byteBuffer)) {
					return false;
				}
			}
		} while (true);
	}

	public final void timeout() {
		if (state == AsyncCommand.COMPLETE) {
			return;
		}
		
		// Command has timed out.
		iterations++;
		
		// Attempt retry.
		if (command.policy.retryOnTimeout && iterations <= command.policy.maxRetries) {
			closeConnection();
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.policy.timeout);
			timeoutTask = eventLoop.timer.addTimeout(this, deadline);
			executeCommand();
			return;
		}

		// Attempt timeout delay.
		if (command.policy.timeoutDelay > 0) {
			if (timeoutDelay) {
				// Transaction has been delayed long enough.
				// timeoutTask has already been removed, so set to null to avoid cancel.
				timeoutTask = null;
				cleanup();
			}
			else {
				// Notify user of timeout, but allow transaction to continue
				// in hope of reusing the socket.
				timeoutDelay = true;
				deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.policy.timeoutDelay);
				timeoutTask = eventLoop.timer.addTimeout(this, deadline);
				notifyFailure(new AerospikeException.Timeout(node, command.policy.timeout, iterations, 0, 0));
			}
			return;		
		}
		
		// Perform timeout.
		timeoutTask = null;
		cleanup();
		notifyFailure(new AerospikeException.Timeout(node, command.policy.timeout, iterations, 0, 0));
	}
	
	protected final void finish() {
		complete();
		
		if (timeoutDelay) {
			// User has already been notified.
			return;
		}

		try {
			command.onSuccess();
		}
		catch (Exception e) {
			Log.error("onSuccess() error: " + Util.getErrorMessage(e));
		}
	}

	protected final void onNetworkError(AerospikeException ae) {
		if (timeoutDelay) {
			// User has already been notified.
			cleanup();
			return;
		}
		
		// Attempt retry.
		if (iterations < command.policy.maxRetries && (command.policy.retryOnTimeout || deadline == 0 || System.nanoTime() < deadline)) {
			closeConnection();
			iterations++;
			
			if (command.policy.retryOnTimeout && deadline > 0) {
				deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.policy.timeout);
				timeoutTask.cancel();
				timeoutTask = eventLoop.timer.addTimeout(this, deadline);
			}
			executeCommand();
			return;
		}
		
		// Fail command.
		cleanup();
		notifyFailure(ae);		
	}
	
	protected final void onApplicationError(AerospikeException ae) {
		if (ae.keepConnection()) {
			// Put connection back in pool.
			complete();
		}
		else {
			// Close socket to flush out possible garbage.
			cleanup();
		}
		
		if (! timeoutDelay) {			
			notifyFailure(ae);
		}		
	}
	
	private final void notifyFailure(AerospikeException ae) {
		try {
			command.onFailure(ae);
		}
		catch (Exception e) {
			Log.error("onFailure() error: " + Util.getErrorMessage(e));		
		}
	}

	private final void complete() {		
		if (timeoutTask != null) {
			timeoutTask.cancel();
		}
		conn.unregister();
		node.putAsyncConnection(conn, eventLoop.index);
		eventLoop.putByteBuffer(byteBuffer);
		command.putBuffer();
		eventState.pending--;
		state = AsyncCommand.COMPLETE;
	}

	private final void cleanup() {
		if (timeoutTask != null) {
			timeoutTask.cancel();
		}
		closeConnection();
		
		if (byteBuffer != null) {
			eventLoop.putByteBuffer(byteBuffer);
		}
		command.putBuffer();
		eventState.pending--;
		state = AsyncCommand.COMPLETE;
	}

	private final void closeConnection() {
		if (conn != null) {
			node.closeAsyncConnection(conn, eventLoop.index);
			conn = null;
		}
	}
}
