/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
			failOnClientTimeout();
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
			failOnClientTimeout();
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
			failOnClientTimeout();
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
		else {
			failOnClientTimeout();
		}
	}

	private boolean failOnNetworkInit() {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			closeOnNetworkError();
			return false;
		}
		else {
			failOnClientTimeout();
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
			failOnClientTimeout();
			return true;
		}
	}

	private void failOnNetworkError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			closeOnNetworkError();
			onFailure(ae);
		}
		else {
			failOnClientTimeout();
		}
	}

	protected final void failOnApplicationError(AerospikeException ae) {
		// Ensure that command succeeds or fails, but not both.
		if (complete.compareAndSet(false, true)) {			
			close();
			onFailure(ae);
		}
		else {
			failOnClientTimeout();
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
