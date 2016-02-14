/*
 * Copyright 2012-2016 Aerospike, Inc.
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
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.ThreadLocalData;

/**
 * Asynchronous command handler.
 */
public abstract class AsyncCommand extends Command implements Runnable {
	private static final int IN_PROGRESS = 0;
	private static final int TIMEOUT_DELAY = 1;
	private static final int COMPLETE = 2;

	protected AsyncConnection conn;
	protected ByteBuffer byteBuffer;
	protected final AsyncCluster cluster;
	protected AsyncNode node;
	private final AtomicInteger state = new AtomicInteger();
	private long limit;
	protected int timeout;
	protected boolean inAuthenticate;
	protected boolean inHeader = true;
	
	public AsyncCommand(AsyncCluster cluster) {
		this.cluster = cluster;
	}
	
	public void execute() {
		Policy policy = getPolicy();
		timeout = policy.timeout;
		
		if (timeout > 0) {	
			limit = System.currentTimeMillis() + timeout;
		}
		
		byteBuffer = cluster.getByteBuffer();
		
		try {
			node = getNode();
			conn = node.getAsyncConnection(byteBuffer);
			
			if (conn == null) {
				conn = new AsyncConnection(node.getAddress(), cluster);
			
				if (cluster.getUser() != null) {
					inAuthenticate = true;
					dataBuffer = ThreadLocalData.getBuffer();
					AdminCommand command = new AdminCommand(dataBuffer);
					dataOffset = command.setAuthenticate(cluster.getUser(), cluster.getPassword());
					byteBuffer.clear();
					byteBuffer.put(dataBuffer, 0, dataOffset);
					byteBuffer.flip();
					conn.execute(this);
					return;
				}
			}
			writeCommand();
			conn.execute(this);
		}
		catch (RuntimeException re) {
			close();
			throw re;
		}
	}
	
	protected void writeCommand() {	
		writeBuffer();
		
		if (dataOffset > byteBuffer.capacity()) {
			byteBuffer = ByteBuffer.allocateDirect(dataOffset);
		}
		
		byteBuffer.clear();
		byteBuffer.put(dataBuffer, 0, dataOffset);
		byteBuffer.flip();
	}

	protected void processAuthenticate() {	
		inAuthenticate = false;
		inHeader = true;
		
		int resultCode = byteBuffer.get(1) & 0xFF;
	
		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}
		writeCommand();
		conn.setWriteable();
	}

	protected final void write() throws IOException {
		conn.write(byteBuffer);
	}

	protected final boolean checkTimeout() {
		int status = state.get();
		
		if (status == COMPLETE) {
			return false;
		}
		
		if (limit > 0 && System.currentTimeMillis() > limit) {
			// Check if timeouts are allowed in the current state.
			// Do not timeout if the command is currently reading data in an offloaded task thread.
			// This means actual time elapsed can be significantly greater than the timeout because
			// we won't check again for another selector iteration.
			if (conn.allowTimeout()) {
				// Command has timed out in timeout queue thread.
				// At this point, we know another thread can't be modifying this command's state.
				Policy policy = getPolicy();
				
				if (policy.timeoutDelay > 0) {
					if (status == IN_PROGRESS) {
						if (state.compareAndSet(IN_PROGRESS, TIMEOUT_DELAY)) {
							// Notify user of timeout, but allow transaction to continue
							// in hope of reusing the socket. Reset timeout
							limit = System.currentTimeMillis() + policy.timeoutDelay;
							onFailure(new AerospikeException.Timeout(node, timeout, 0, 0, 0));
							return true;
						}											
					}
					else {
						if (state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {
							// Transaction has been delayed long enough.
							// We know the task has not been offloaded to another thread,
							// so we can close safely here.
							close();
						}
					}
				}
				else {
					// Ensure that command succeeds or fails, but not both.
					if (state.compareAndSet(IN_PROGRESS, COMPLETE)) {
						// We know the task has not been offloaded to another thread,
						// so we can close safely here.
						close();
						onFailure(new AerospikeException.Timeout(node, timeout, 0, 0, 0));
					}
				}
				return false;  // Do not put back on timeout queue.
			}
		}
		return true;
	}
	
	public void run() {
		try {
			read();
			
			if (state.get() != COMPLETE) {
				conn.setReadable();
			}
		}
        catch (AerospikeException.Connection ac) {
        	failOnNetworkError(ac);
        }
        catch (AerospikeException ae) {
			// Fail without retry on non-network errors.
			failOnApplicationError(ae);
        }
        catch (IOException ioe) {
        	failOnNetworkError(new AerospikeException(ioe));
        }
        catch (Exception e) {
			// Fail without retry on unknown errors.
			failOnApplicationError(new AerospikeException(e));
        }
	}
	
	protected final void finish() {
		// Finish could be called from a separate asyncTaskThreadPool thread.
		// Make sure SelectorManager thread has not already caused a transaction timeout.
		if (state.compareAndSet(IN_PROGRESS, COMPLETE)) {
			conn.unregister();
			node.putAsyncConnection(conn);
			cluster.putByteBuffer(byteBuffer);
			
			try {
				onSuccess();
			}
			catch (AerospikeException ae) {
				// The user's onSuccess() may have already been called which in turn generates this
				// exception.  It's important to call onFailure() anyhow because the user's code 
				// may be waiting for completion notification which wasn't yet called in
				// onSuccess().  This is the only case where both onSuccess() and onFailure()
				// gets called for the same command.
				onFailure(ae);
			}
			catch (Exception e) {
				onFailure(new AerospikeException(e));
			}
		}
		else if (state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {
			// User has already been notified of timeout.
			// Put connection back into pool and discard response.
			conn.unregister();
			node.putAsyncConnection(conn);
			cluster.putByteBuffer(byteBuffer);
		}
	}

	protected final void failOnNetworkError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (state.compareAndSet(IN_PROGRESS, COMPLETE)) {
			close();
			onFailure(ae);
		}
		else if (state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {			
			close();
		}
	}

	protected final void failOnApplicationError(AerospikeException ae) {
		// Ensure that command succeeds or fails exactly once.
		boolean notify = state.compareAndSet(IN_PROGRESS, COMPLETE);
		
		if (notify || state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {			
			if (ae.keepConnection()) {
				// Put connection back in pool.
				conn.unregister();
				node.putAsyncConnection(conn);
				cluster.putByteBuffer(byteBuffer);
			}
			else {
				// Close socket to flush out possible garbage.
				close();
			}
			
			if (notify) {			
				onFailure(ae);
			}
		}
	}

	private void close() {
		if (conn != null) {
			conn.close();
			conn = null;
		}
		cluster.putByteBuffer(byteBuffer);
	}

	protected abstract AsyncNode getNode() throws AerospikeException.InvalidNode;
	protected abstract void read() throws AerospikeException, IOException;
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
