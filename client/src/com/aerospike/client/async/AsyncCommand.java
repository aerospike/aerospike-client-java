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
	private final AtomicBoolean complete = new AtomicBoolean();
	private long limit;
	protected int timeout;
	
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
		}
		catch (AerospikeException.InvalidNode ai) {
			cluster.putByteBuffer(byteBuffer);
			throw ai;
		}
		catch (AerospikeException.Connection ce) {
			// Socket connection error has occurred.
			node.decreaseHealth();
			cluster.putByteBuffer(byteBuffer);
			throw ce;
		}
		catch (Exception e) {
			if (conn != null) {
				node.putAsyncConnection(conn);
			}
			cluster.putByteBuffer(byteBuffer);
			throw new AerospikeException(e);
		}
	}
	
	protected final void write() throws IOException {
		conn.write(byteBuffer);
	}

	protected final boolean checkTimeout() {
		if (complete.get()) {
			return false;
		}
		long current = System.currentTimeMillis();
		
		if (limit > 0 && current > limit) {
			// Command has timed out.
			/*
			if (Log.debugEnabled()) {
				int elapsed = ((int)(current - limit)) + timeout;
				Log.debug("Client timeout: timeout=" + timeout + " elapsed=" + elapsed);
			}
			*/
			node.decreaseHealth();
			fail(new AerospikeException.Timeout());
			return false;
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
	
	protected final void failConnection(AerospikeException ae) {
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ae));
		}
		node.decreaseHealth();
		fail(ae);
	}

	protected final void failCommand(IOException ioe) {
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ioe));
		}
		// IO error means connection to server node is unhealthy.
		// Reflect this status.
		node.decreaseHealth();
		fail(new AerospikeException(ioe));
	}
	
	protected final void failCommand(AerospikeException ae) {		
		if (Log.debugEnabled()) {
			Log.debug("Node " + node + ": " + Util.getErrorMessage(ae));
		}
		fail(ae);
	}
	
	private final void fail(AerospikeException ae) {
		// Make sure transaction has not already succeeded before failing.
		if (complete.compareAndSet(false, true)) {			
			conn.close();
			cluster.putByteBuffer(byteBuffer);		
			onFailure(ae);
		}
	}

	protected abstract AsyncNode getNode() throws AerospikeException.InvalidNode;
	protected abstract void read() throws AerospikeException, IOException;
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
