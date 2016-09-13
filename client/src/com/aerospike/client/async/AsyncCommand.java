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
	protected AsyncNode node;
	protected final AsyncCluster cluster;
	protected final Policy policy;
	private final AtomicInteger state = new AtomicInteger();
	private long limit;
	private int iterations;
	protected boolean inAuthenticate;
	protected boolean inHeader = true;
	
	public AsyncCommand(AsyncCluster cluster, Policy policy) {
		this.cluster = cluster;
		this.policy = policy;	
	}

	public AsyncCommand(AsyncCommand other) {
		// Retry constructor.
		this.cluster = other.cluster;
		this.policy = other.policy;
		this.byteBuffer = other.byteBuffer;
		this.limit = other.limit;
		this.iterations = other.iterations + 1;
		this.sequence = other.sequence;
	}

	public final void execute() {
		if (policy.timeout > 0) {
			limit = System.currentTimeMillis() + policy.timeout;
		}
		byteBuffer = cluster.getByteBuffer();
		executeCommand();
	}

	private void executeCommand() {
		try {
			node = (AsyncNode)getNode();
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
		catch (AerospikeException.Connection aec) {
			// Attempt retry on failed connection.
			if (iterations < policy.maxRetries && (policy.retryOnTimeout || limit == 0 || System.currentTimeMillis() < limit)) {
				closeConnection();
				iterations++;
				
				if (policy.timeout > 0 && policy.retryOnTimeout) {
					limit = System.currentTimeMillis() + policy.timeout;
				}
				executeCommand();  // recursive call
			}
			else {
				cleanup();
				throw aec;
			}
		}
		catch (RuntimeException re) {
			cleanup();
			throw re;
		}
	}
	
	protected final void writeCommand() {	
		writeBuffer();
		
		if (dataOffset > byteBuffer.capacity()) {
			byteBuffer = ByteBuffer.allocateDirect(dataOffset);
		}
		
		byteBuffer.clear();
		byteBuffer.put(dataBuffer, 0, dataOffset);
		byteBuffer.flip();
	}

	protected final void processAuthenticate() {	
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
				if (policy.timeoutDelay > 0) {
					if (status == IN_PROGRESS) {
						if (state.compareAndSet(IN_PROGRESS, TIMEOUT_DELAY)) {
							// Notify user of timeout, but allow transaction to continue
							// in hope of reusing the socket.  Do not perform concurrent retry
							// because that would require a new byte buffer which could possibly
							// result in deadlock.
							limit = System.currentTimeMillis() + policy.timeoutDelay;
							onFailure(new AerospikeException.Timeout(node, policy.timeout, iterations + 1, 0, 0));
							return true;
						}											
					}
					else {
						if (state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {
							// Transaction has been delayed long enough.
							// We know the task has not been offloaded to another thread,
							// so we can close safely here.
							cleanup();
						}
					}
				}
				else {
					// Ensure that command succeeds or fails, but not both.
					if (state.compareAndSet(IN_PROGRESS, COMPLETE)) {
						// We know the task has not been offloaded to another thread,
						// so we can close safely here.
						// Attempt retry.
						if (iterations < policy.maxRetries && policy.retryOnTimeout) {
							AsyncCommand command = cloneCommand();

							if (command != null) {
								closeConnection();							
								command.limit = System.currentTimeMillis() + policy.timeout;
								
								try {
									command.executeCommand();
								}
								catch (Exception e) {
									// Command has already been cleaned up.
									// Notify user with original error.
									onFailure(new AerospikeException.Timeout(node, policy.timeout, iterations + 1, 0, 0));
								}
								return false;
							}							
						}					
						cleanup();
						onFailure(new AerospikeException.Timeout(node, policy.timeout, iterations + 1, 0, 0));
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
        	onNetworkError(ac);
        }
        catch (AerospikeException ae) {
			// Fail without retry on non-network errors.
			onApplicationError(ae);
        }
        catch (IOException ioe) {
        	onNetworkError(new AerospikeException(ioe));
        }
        catch (Exception e) {
			// Fail without retry on unknown errors.
			onApplicationError(new AerospikeException(e));
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

	protected final void onNetworkError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (state.compareAndSet(IN_PROGRESS, COMPLETE)) {			
			// Attempt retry.
			if (iterations < policy.maxRetries && (policy.retryOnTimeout || limit == 0 || System.currentTimeMillis() < limit)) {
				AsyncCommand command = cloneCommand();

				if (command != null) {
					closeConnection();
					
					if (policy.timeout > 0 && policy.retryOnTimeout) {
						command.limit = System.currentTimeMillis() + policy.timeout;
					}
					
					try {				
						command.executeCommand();
					}
					catch (Exception e) {
						// Command has already been cleaned up.
						// Notify user with original error.
						onFailure(ae);
					}
					return;
				}
			}
			cleanup();
			onFailure(ae);
		}
		else if (state.compareAndSet(TIMEOUT_DELAY, COMPLETE)) {			
			cleanup();
		}
	}

	protected final void onApplicationError(AerospikeException ae) {
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
				cleanup();
			}
			
			if (notify) {			
				onFailure(ae);
			}
		}
	}

	private void cleanup() {
		closeConnection();
		cluster.putByteBuffer(byteBuffer);
	}

	private void closeConnection() {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}

	protected abstract AsyncCommand cloneCommand();
	protected abstract void read() throws AerospikeException, IOException;
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
