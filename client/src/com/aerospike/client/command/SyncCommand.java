/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.client.command;

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {

	public final void execute() throws AerospikeException {
		Policy policy = getPolicy();        
		int remainingMillis = policy.timeout;
		long limit = System.currentTimeMillis() + remainingMillis;
        int failedNodes = 0;
        int failedConns = 0;
        int iterations = 0;

        // Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			Node node = null;
			try {		
				node = getNode();
				Connection conn = node.getConnection(remainingMillis);
				
				try {
					// Set command buffer.
					writeBuffer();

					// Reset timeout in send buffer (destined for server) and socket.
					Buffer.intToBytes(remainingMillis, dataBuffer, 22);
					
					// Send command.
					conn.write(dataBuffer, dataOffset);
					
					// Parse results.
					parseResult(conn);
					
					// Reflect healthy status.
					conn.updateLastUsed();
					node.restoreHealth();
					
					// Put connection back in pool.
					node.putConnection(conn);
					
					// Command has completed successfully.  Exit method.
					return;
				}
				catch (AerospikeException ae) {
					if (ae.keepConnection()) {
						// Put connection back in pool.
						conn.updateLastUsed();
						node.restoreHealth();
						node.putConnection(conn);						
					}
					else {
						// Close socket to flush out possible garbage.  Do not put back in pool.
						conn.close();
					}
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

			if (++iterations > policy.maxRetries) {
				break;
			}
			
			// Check for client timeout.
			if (policy.timeout > 0) {
				remainingMillis = (int)(limit - System.currentTimeMillis() - policy.sleepBetweenRetries);
				
				if (remainingMillis <= 0) {
					break;
				}
			}
			
			if (policy.sleepBetweenRetries > 0) {
				// Sleep before trying again.
				Util.sleep(policy.sleepBetweenRetries);
			}
		}
		
		/*
		if (Log.debugEnabled()) {
			Log.debug("Client timeout: timeout=" + policy.timeout + " iterations=" + iterations + 
				" failedNodes=" + failedNodes + " failedConns=" + failedConns);
		}*/
		throw new AerospikeException.Timeout(policy.timeout, iterations, failedNodes, failedConns);
	}
		
	protected abstract Node getNode() throws AerospikeException.InvalidNode;
	protected abstract void parseResult(Connection conn) throws AerospikeException, IOException;
}
