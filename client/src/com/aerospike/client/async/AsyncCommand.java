/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

/**
 * Asynchronous command handler.
 */
public abstract class AsyncCommand extends Command implements Runnable {
	
	protected AsyncConnection conn;
	protected ByteBuffer byteBuffer;
	protected final AsyncCluster cluster;
	protected AsyncNode node;
	private final AtomicBoolean complete = new AtomicBoolean();
	private long limit;
	protected int timeout;
	private int iteration;
	protected boolean inHeader = true;
	
	public AsyncCommand(AsyncCluster cluster) {
		this.cluster = cluster;
	}
	
	public void execute() throws AerospikeException {
		Policy policy = getPolicy();
		timeout = policy.timeout;
		
		if (timeout > 0) {	
			limit = System.currentTimeMillis() + timeout;
		}
		
		byteBuffer = cluster.getByteBuffer();
		executeCommand();
	}
		
	public void executeCommand() throws AerospikeException {
		if (complete.get()) {
			return;
		}

		try {
			node = getNode();
			conn = node.getAsyncConnection();			
			writeBuffer();
				
			if (dataOffset > byteBuffer.capacity()) {
				byteBuffer = ByteBuffer.allocateDirect(dataOffset);
			}
			
			byteBuffer.clear();
			byteBuffer.put(dataBuffer, 0, dataOffset);
			byteBuffer.flip();
	
			conn.execute(this);
		}
		catch (AerospikeException.InvalidNode ai) {
			if (!retryOnInit()) {				
				throw ai;
			}
		}
		catch (AerospikeException.Connection ce) {
			// Socket connection error has occurred.
			if (!retryOnInit()) {				
				throw ce;
			}
		}
		catch (Exception e) {
			if (!failOnApplicationInit()) {
				throw new AerospikeException(e);
			}
		}
	}

	private boolean retryOnInit() throws AerospikeException {
		if (complete.get()) {
			return true;
		}

		Policy policy = getPolicy();
		
		if (++iteration > policy.maxRetries) {
			return failOnNetworkInit();
		}

		if (limit > 0 && System.currentTimeMillis() + policy.sleepBetweenRetries > limit) {
			// Might as well stop here because the transaction will
			// timeout after sleep completed.
			return failOnNetworkInit();
		}

		// Prepare for retry.
		resetConnection();

		if (policy.sleepBetweenRetries > 0) {
			Util.sleep(policy.sleepBetweenRetries);
		}

		// Retry command recursively.
		executeCommand();
		return true;
	}

	protected final void write() throws IOException {
		conn.write(byteBuffer);
	}

	protected final void retryAfterInit(AerospikeException ae) {
		if (complete.get()) {
			return;
		}

		Policy policy = getPolicy();

		if (++iteration > policy.maxRetries) {
			failOnNetworkError(ae);
			return;
		}

		if (limit > 0 && System.currentTimeMillis() + policy.sleepBetweenRetries > limit) {
			// Might as well stop here because the transaction will
			// timeout after sleep completed.
			failOnNetworkError(ae);
			return;
		}

		// Prepare for retry.
		resetConnection();

		if (policy.sleepBetweenRetries > 0) {
			Util.sleep(policy.sleepBetweenRetries);
		}

		try {
			// Retry command recursively.
			executeCommand();
		}
		catch (Exception e) {
			// Command has already been cleaned up.
			// Notify user of original exception.
			onFailure(ae);
		}
	}

	private void resetConnection() {
		if (node != null) {
			node.decreaseHealth();
		}

		if (limit > 0) {
			// A lock on reset is required when a client timeout is specified.
			synchronized (this) {
				if (conn != null) {
					conn.close();
					conn = null;
				}
			}
		}
		else {
			if (conn != null) {
				conn.close();
				conn = null;
			}
		}
	}

	protected final boolean checkTimeout() {
		if (complete.get()) {
			return false;
		}
		
		if (limit > 0 && System.currentTimeMillis() > limit) {
			// Command has timed out in timeout queue thread.
			// Ensure that command succeeds or fails, but not both.
			if (complete.compareAndSet(false, true)) {
				// Timeout thread may contend with retry thread.
				// Lock before closing.
				synchronized (this) {
					if (conn != null) {
						conn.close();
					}
				}
				failOnClientTimeout();
			}
			return false;  // Do not put back on timeout queue.
		}
		return true;
	}
	
	public void run() {
		try {
			read();
			
			if (! complete.get()) {
				conn.setReadable();
			}
		}
        catch (AerospikeException.Connection ac) {
        	retryAfterInit(ac);
        }
        catch (AerospikeException ae) {
			// Fail without retry on non-network errors.
			failOnApplicationError(ae);
        }
        catch (IOException ioe) {
        	retryAfterInit(new AerospikeException(ioe));
        }
        catch (Exception e) {
			// Fail without retry on unknown errors.
			failOnApplicationError(new AerospikeException(e));
        }
	}
	
	protected final void finish() {
		// Finish could be called from a separate asyncTaskThreadPool thread.
		// Make sure SelectorManager thread has not already caused a transaction timeout.
		if (complete.compareAndSet(false, true)) {			
			conn.unregister();
			conn.updateLastUsed();
			node.putAsyncConnection(conn);
			node.restoreHealth();
			cluster.putByteBuffer(byteBuffer);
			onSuccess();
		} 
	}

	private boolean failOnNetworkInit() {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			closeOnNetworkError();
			return false;
		}
		else {
			return true;
		}
	}

	private boolean failOnApplicationInit() {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			close();
			return false;
		}
		else {
			return true;
		}
	}

	private void failOnNetworkError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			closeOnNetworkError();
			onFailure(ae);
		}
	}

	protected final void failOnApplicationError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {
			if (ae.keepConnection()) {
				// Put connection back in pool.
				conn.unregister();
				conn.updateLastUsed();
				node.putAsyncConnection(conn);
				node.restoreHealth();
				cluster.putByteBuffer(byteBuffer);
			}
			else {
				// Close socket to flush out possible garbage.
				close();
			}
			onFailure(ae);
		}
	}

	private void failOnClientTimeout() {
		// Free up resources and notify.
		closeOnNetworkError();
		onFailure(new AerospikeException.Timeout());
	}

	private void closeOnNetworkError() {
		if (node != null) {
			node.decreaseHealth();
		}
		close();
	}

	private void close() {
		// Connection was probably already closed by timeout thread.
		// Check connected status before closing again.
		if (conn != null && conn.isConnected()) {
			conn.close();
		}
		cluster.putByteBuffer(byteBuffer);
	}

	protected abstract AsyncNode getNode() throws AerospikeException.InvalidNode;
	protected abstract void read() throws AerospikeException, IOException;
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
