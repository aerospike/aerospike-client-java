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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

/**
 * Asynchronous command handler.
 */
public abstract class AsyncCommand implements Runnable {
	
	protected AsyncConnection conn;
	protected ByteBuffer byteBuffer;
	protected final AsyncCluster cluster;
	protected AsyncNode node;
	private long limit;
	protected int timeout;
	private boolean complete;
	
	public AsyncCommand(AsyncCluster cluster) {
		this.cluster = cluster;
	}
	
	public void execute(Policy policy, Command command) throws AerospikeException {
		if (policy == null) {
			policy = new Policy();
		}
		
		timeout = policy.timeout;
		
		if (timeout > 0) {		
			limit = System.currentTimeMillis() + timeout;
		}

		byteBuffer = cluster.getByteBuffer();

		int maxIterations = policy.maxRetries + 1;		
        int failedNodes = 0;
        int failedConns = 0;
        int i;

        // Execute command until successful, timed out or maximum iterations have been reached.
		for (i = 0; i < maxIterations; i++) {
			try {		
				node = getNode();
	    		conn = node.getAsyncConnection();
				
				int size = command.getSendOffset();
				
				if (size > byteBuffer.capacity()) {
					byteBuffer = ByteBuffer.allocateDirect(size);
				}
				
				byteBuffer.clear();
				byteBuffer.put(command.getSendBuffer(), 0, size);
				byteBuffer.flip();

				conn.execute(this);
				return;
			}
			catch (AerospikeException.InvalidNode ine) {
				// Node is currently inactive.  Retry.
				failedNodes++;
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Decrease health and retry.
				node.decreaseHealth(60);
				
				if (Log.debugEnabled()) {
					Log.debug("Node " + node + ": " + Util.getErrorMessage(ce));
				}
				failedConns++;
			}
			catch (Exception e) {
				cluster.putByteBuffer(byteBuffer);
				throw new AerospikeException(e);
			}

			// Check for client timeout.
			if (limit > 0 && System.currentTimeMillis() > limit) {
				break;
			}

			// Sleep before trying again.
			Util.sleep(policy.sleepBetweenRetries);
		}
		
		cluster.putByteBuffer(byteBuffer);
		
		if (Log.debugEnabled()) {
			Log.debug("Client timeout: timeout=" + policy.timeout + " iterations=" + i + 
				" failedNodes=" + failedNodes + " failedConns=" + failedConns);
		}
		throw new AerospikeException.Timeout();
	}
	
	protected final void write() throws IOException {
		conn.write(byteBuffer);
	}

	protected final boolean checkTimeout() {
		if (complete) {
			return false;
		}
		long current = System.currentTimeMillis();
		
		if (limit > 0 && current > limit) {
			// Command has timed out.
			if (Log.debugEnabled()) {
				int elapsed = ((int)(current - limit)) + timeout;
				Log.debug("Client timeout: timeout=" + timeout + " elapsed=" + elapsed);
			}
			node.decreaseHealth(20);
			fail(new AerospikeException.Timeout());
			return false;
		}
		return true;
	}
	
	public void run() {
		try {
			read();
			
			if (! complete) {
				conn.setReadable();
			}
		}
        catch (AerospikeException ae) {
        	failCommand(ae);
        }
        catch (IOException ioe) {
			failCommand(ioe);
        }
        catch (Exception e) {
			failCommand(new AerospikeException(e));
        }
	}
	
	protected final void finish() {
		complete = true;
		conn.unregister();
		conn.updateLastUsed();
		node.putAsyncConnection(conn);
		node.restoreHealth();
		cluster.putByteBuffer(byteBuffer);
		onSuccess();
	}
	
	protected final void failConnection(AerospikeException ae) {
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ae));
		}
		node.decreaseHealth(60);
		fail(ae);
	}

	protected final void failCommand(IOException ioe) {
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ioe));
		}
		// IO error means connection to server node is unhealthy.
		// Reflect this status.
		node.decreaseHealth(60);
		fail(new AerospikeException(ioe));
	}
	
	protected final void failCommand(AerospikeException ae) {		
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ae));
		}
		fail(ae);
	}
	
	private final void fail(AerospikeException ae) {		
		complete = true;
		
		if (conn != null) {
			conn.close();
		}
		cluster.putByteBuffer(byteBuffer);		
		onFailure(ae);
	}

	protected abstract AsyncNode getNode() throws AerospikeException.InvalidNode;
	protected abstract void read() throws AerospikeException, IOException;
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
