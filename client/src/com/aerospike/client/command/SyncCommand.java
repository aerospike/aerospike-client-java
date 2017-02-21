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
package com.aerospike.client.command;

import java.io.IOException;
import java.net.SocketTimeoutException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {

	public final void execute() {
		Policy policy = getPolicy();        
		int remainingMillis = policy.timeout;
		long limit = System.currentTimeMillis() + remainingMillis;
		Node node = null;
		Exception exception = null;
        int failedNodes = 0;
        int failedConns = 0;
        int iterations = 0;

        // Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			try {		
				node = getNode();
				Connection conn = node.getConnection(remainingMillis);
				
				try {
					// Set command buffer.
					writeBuffer();

					// Check if timeout needs to be changed in send buffer.
					if (remainingMillis != policy.timeout) {
						// Reset timeout in send buffer (destined for server) and socket.
						Buffer.intToBytes(remainingMillis, dataBuffer, 22);
					}
					
					// Send command.
					conn.write(dataBuffer, dataOffset);
					
					// Parse results.
					parseResult(conn);
					
					// Put connection back in pool.
					node.putConnection(conn);
					
					// Command has completed successfully.  Exit method.
					return;
				}
				catch (AerospikeException ae) {
					if (ae.keepConnection()) {
						// Put connection back in pool.
						node.putConnection(conn);						
					}
					else {
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.closeConnection(conn);
					}
					throw ae;
				}
				catch (RuntimeException re) {
					// All runtime exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					node.closeConnection(conn);
					throw re;
				}
				catch (SocketTimeoutException ste) {
					// Full timeout has been reached.
					node.closeConnection(conn);
					exception = ste;
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					node.closeConnection(conn);
					exception = new AerospikeException(ioe);
				}
			}
			catch (AerospikeException.InvalidNode ine) {
				// Node is currently inactive.  Retry.
				exception = ine;
				failedNodes++;
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Retry.				
				exception = ce;
				failedConns++;
			}

			if (++iterations > policy.maxRetries) {
				break;
			}
			
			// Check for client timeout.
			if (policy.timeout > 0 && ! policy.retryOnTimeout) {
				// Timeout is absolute.  Stop if timeout has been reached.
				remainingMillis = (int)(limit - System.currentTimeMillis() - policy.sleepBetweenRetries);
				
				if (remainingMillis <= 0) {
					break;
				}
			}
			
			if (policy.sleepBetweenRetries > 0) {
				// Sleep before trying again.
				Util.sleep(policy.sleepBetweenRetries);
			}

			// Reset node reference and try again.
			node = null;
		}
		
		// Retries have been exhausted.  Throw last exception.
		if (exception instanceof SocketTimeoutException) {
			throw new AerospikeException.Timeout(node, policy.timeout, iterations, failedNodes, failedConns);
		}
		throw (RuntimeException)exception;
	}

	protected final void emptySocket(Connection conn) throws IOException
	{
		// There should not be any more bytes.
		// Empty the socket to be safe.
		long sz = Buffer.bytesToLong(dataBuffer, 0);
		int headerLength = dataBuffer[8];
		int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;

		// Read remaining message bytes.
		if (receiveSize > 0)
		{
			sizeBuffer(receiveSize);
			conn.readFully(dataBuffer, receiveSize);
		}
	}

	protected abstract Policy getPolicy();
	protected abstract void parseResult(Connection conn) throws AerospikeException, IOException;
}
