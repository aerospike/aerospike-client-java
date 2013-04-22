/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {

	protected byte[] receiveBuffer;
		
	public final void execute(Policy policy) throws AerospikeException {
		if (policy == null) {
			policy = new Policy();
		}
		        
		int maxIterations = policy.maxRetries + 1;
		int remainingMillis = policy.timeout;
		long limit = System.currentTimeMillis() + remainingMillis;
        int failedNodes = 0;
        int failedConns = 0;
        int i;

        // Execute command until successful, timed out or maximum iterations have been reached.
		for (i = 0; i < maxIterations; i++) {
			Node node = null;
			try {		
				node = getNode();
				Connection conn = node.getConnection(remainingMillis);
				
				try {
					// Reset timeout in send buffer (destined for server) and socket.
					Buffer.intToBytes(remainingMillis, sendBuffer, 22);
					
					// Send command.
					send(conn);
					
					// Parse results.
					parseResult(conn.getInputStream());
					
					// Reflect healthy status.
					conn.updateLastUsed();
					node.restoreHealth();
					
					// Put connection back in pool.
					node.putConnection(conn);
					
					// Command has completed successfully.  Exit method.
					return;
				}
				catch (AerospikeException ae) {
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					throw ae;
				}
				catch (RuntimeException re) {
					// All runtime exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					throw re;
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					conn.close();
					
					if (Log.debugEnabled()) {
						Log.debug("Node " + node + ": " + Util.getErrorMessage(ioe));
					}
					// IO error means connection to server node is unhealthy.
					// Reflect this status.
					node.decreaseHealth();
				}
			}
			catch (AerospikeException.InvalidNode ine) {
				// Node is currently inactive.  Retry.
				failedNodes++;
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Decrease health and retry.
				node.decreaseHealth();
				
				if (Log.debugEnabled()) {
					Log.debug("Node " + node + ": " + Util.getErrorMessage(ce));
				}
				failedConns++;	
			}

			// Check for client timeout.
			if (policy.timeout > 0) {
				remainingMillis = (int)(limit - System.currentTimeMillis());

				if (remainingMillis <= 0) {
					break;
				}
			}
			// Sleep before trying again.
			Util.sleep(policy.sleepBetweenRetries);
		}
		
		if (Log.debugEnabled()) {
			Log.debug("Client timeout: timeout=" + policy.timeout + " iterations=" + i + 
				" failedNodes=" + failedNodes + " failedConns=" + failedConns);
		}
		throw new AerospikeException.Timeout();
	}
	
	private final void send(Connection conn) throws IOException {
		final OutputStream os = conn.getOutputStream();
		
		// Never write more than 8 KB at a time.  Apparently, the jni socket write does an extra 
		// malloc and free if buffer size > 8 KB.
		final int max = sendOffset;
		int pos = 0;
		int len;
		
		while (pos < max) {
			len = max - pos;
			
			if (len > 8192)
				len = 8192;
			
			os.write(sendBuffer, pos, len);
			pos += len;
		}
	}
	
	public static void readFully(InputStream is, byte[] buf, int length) throws IOException {
		int pos = 0;
	
		while (pos < length) {
			int count = is.read(buf, pos, length - pos);
		    
			if (count < 0)
		    	throw new EOFException();
			
			pos += count;
		}
	}
	
	protected abstract Node getNode() throws AerospikeException.InvalidNode;
	protected abstract void parseResult(InputStream is) throws AerospikeException, IOException;
}
