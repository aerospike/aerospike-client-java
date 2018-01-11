/*
 * Copyright 2012-2018 Aerospike, Inc.
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
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {
	// private static final AtomicLong TranCounter = new AtomicLong();

	public final void execute(Cluster cluster, Policy policy, Key key, Node node, boolean isRead) {
		//final long tranId = TranCounter.getAndIncrement();
		final Partition partition = (key != null)? new Partition(key) : null;
		AerospikeException exception = null;
		long deadline = 0;
		int socketTimeout = policy.socketTimeout;
		int totalTimeout = policy.totalTimeout;
		int iteration = 0;
		int commandSentCounter = 0;
		boolean isClientTimeout;

		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

			if (socketTimeout > totalTimeout) {
				socketTimeout = totalTimeout;
			}
		}

		// Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			try {
				if (partition != null) {
					// Single record command node retrieval.
					node = getNode(cluster, partition, policy.replica, isRead);
					
					//if (iteration > 0 && !isRead) {
					//	Log.info("Retry: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					//}
				}
				Connection conn = node.getConnection(socketTimeout);
				
				try {
					// Set command buffer.
					writeBuffer();

					// Check if total timeout needs to be changed in send buffer.
					if (totalTimeout != policy.totalTimeout) {
						// Reset timeout in send buffer (destined for server) and socket.
						Buffer.intToBytes(totalTimeout, dataBuffer, 22);
					}
					
					// Send command.
					conn.write(dataBuffer, dataOffset);
					commandSentCounter++;
					
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
					
					if (ae.getResultCode() == ResultCode.TIMEOUT) {
						// Go through retry logic on server timeout.
						// Log.info("Server timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						exception = new AerospikeException.Timeout(node, policy, iteration + 1, false);
						isClientTimeout = false;
						
						if (isRead) {
							super.sequence++;
						}
					}
					else {
						// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
						ae.setInDoubt(isRead, commandSentCounter);
						throw ae;
					}
				}
				catch (RuntimeException re) {
					// All runtime exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					// Log.info("Throw RuntimeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					throw re;
				}
				catch (SocketTimeoutException ste) {
					// Full timeout has been reached.
					// Log.info("Socket timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					isClientTimeout = true;

					if (isRead) {
						super.sequence++;
					}
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException(ioe);
					isClientTimeout = false;
					super.sequence++;
				}
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Retry.				
				// Log.info("Connection error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = ce;
				isClientTimeout = false;
				super.sequence++;
			}
			
			// Check maxRetries.
			if (++iteration > policy.maxRetries) {
				break;
			}

			if (policy.totalTimeout > 0) {
				// Check for total timeout.
				long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

				if (remaining <= 0) {
					break;
				}
				
				// Convert back to milliseconds for remaining check.
				remaining = TimeUnit.NANOSECONDS.toMillis(remaining);

				if (remaining < totalTimeout) {
					totalTimeout = (int)remaining;
					
					if (socketTimeout > totalTimeout) {
						socketTimeout = totalTimeout;
					}
				}				
			}

			if (!isClientTimeout && policy.sleepBetweenRetries > 0) {
				// Sleep before trying again.
				Util.sleep(policy.sleepBetweenRetries);
			}
		}

		// Retries have been exhausted.  Throw last exception.
		if (isClientTimeout) {
			// Log.info("SocketTimeoutException: " + tranId + ',' + sequence + ',' + iteration);
			exception = new AerospikeException.Timeout(node, policy, iteration, true);
		}
				
		// Log.info("Runtime exception: " + tranId + ',' + sequence + ',' + iteration + ',' + exception.getMessage());
		exception.setInDoubt(isRead, commandSentCounter);
		throw exception;
	}

	@Override
	protected final void sizeBuffer() {
		dataBuffer = ThreadLocalData.getBuffer();
		
		if (dataOffset > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(dataOffset);
		}
	}
	
	protected final void sizeBuffer(int size) {
		if (size > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(size);
		}
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

	protected abstract void writeBuffer();
	protected abstract void parseResult(Connection conn) throws AerospikeException, IOException;
}
