/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.ConnectionRecover;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {
	// private static final AtomicLong TranCounter = new AtomicLong();
	protected final Cluster cluster;
	protected final Policy policy;
	int iteration = 1;
	int commandSentCounter;
	long deadline;

	/**
	 * Default constructor.
	 */
	public SyncCommand(Cluster cluster, Policy policy) {
		super(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		this.cluster = cluster;
		this.policy = policy;
	}

	/**
	 * Scan/Query constructor.
	 */
	public SyncCommand(Cluster cluster, Policy policy, int socketTimeout, int totalTimeout) {
		super(socketTimeout, totalTimeout, 0);
		this.cluster = cluster;
		this.policy = policy;
	}

	public void execute() {
		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);
		}
		executeCommand();
	}

	public final void executeCommand() {
		//final long tranId = TranCounter.getAndIncrement();
		Node node;
		AerospikeException exception = null;
		boolean isClientTimeout;

		// Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			try {
				node = getNode();
			}
			catch (AerospikeException ae) {
				if (cluster.isActive()) {
					// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
					ae.setPolicy(policy);
					ae.setIteration(iteration);
					ae.setInDoubt(isWrite(), commandSentCounter);
					throw ae;
				}
				else {
					throw new AerospikeException("Cluster has been closed");
				}
			}

			try {
				node.validateErrorCount();
				Connection conn = node.getConnection(this, policy.connectTimeout, socketTimeout, policy.timeoutDelay);

				try {
					// Set command buffer.
					writeBuffer();

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
						// Retry on server timeout.
						// Log.info("Server timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						exception = new AerospikeException.Timeout(policy, false);
						isClientTimeout = false;
						node.incrErrorCount();
					}
					else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
						// Add to circuit breaker error count and retry.
						exception = ae;
						isClientTimeout = false;
						node.incrErrorCount();
					}
					else {
						throw ae;
					}
				}
				catch (Connection.ReadTimeout crt) {
					if (policy.timeoutDelay > 0) {
						cluster.recoverConnection(new ConnectionRecover(conn, node, policy.timeoutDelay, crt, isSingle()));
					}
					else {
						node.closeConnection(conn);
					}
					isClientTimeout = true;
				}
				catch (SocketTimeoutException ste) {
					// Full timeout has been reached.
					// Log.info("Socket timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					isClientTimeout = true;
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
				}
				catch (Throwable e) {
					// All remaining exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					// Log.info("Throw Throwable: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					throw e;
				}
			}
			catch (Connection.ReadTimeout crt) {
				// Connection already handled.
				isClientTimeout = true;
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Retry.
				// Log.info("Connection error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = ce;
				isClientTimeout = false;
			}
			catch (AerospikeException.Backoff be) {
				// Node is in backoff state. Retry, hopefully on another node.
				// Log.info("Backoff error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = be;
				isClientTimeout = false;
			}
			catch (AerospikeException ae) {
				// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
				ae.setNode(node);
				ae.setPolicy(policy);
				ae.setIteration(iteration);
				ae.setInDoubt(isWrite(), commandSentCounter);
				throw ae;
			}

			// Check maxRetries.
			if (iteration > maxRetries) {
				break;
			}

			if (totalTimeout > 0) {
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

			iteration++;

			if (! prepareRetry(isClientTimeout || exception.getResultCode() != ResultCode.SERVER_NOT_AVAILABLE)) {
				// Batch may be retried in separate commands.
				if (retryBatch(cluster, socketTimeout, totalTimeout, deadline, iteration, commandSentCounter)) {
					// Batch was retried in separate commands.  Complete this command.
					return;
				}
			}
		}

		// Retries have been exhausted.  Throw last exception.
		if (isClientTimeout) {
			// Log.info("SocketTimeoutException: " + tranId + ',' + sequence + ',' + iteration);
			exception = new AerospikeException.Timeout(policy, true);
		}

		// Log.info("Runtime exception: " + tranId + ',' + sequence + ',' + iteration + ',' + exception.getMessage());
		exception.setNode(node);
		exception.setPolicy(policy);
		exception.setIteration(iteration);
		exception.setInDoubt(isWrite(), commandSentCounter);
		throw exception;
	}

	public void resetDeadline(long startTime) {
		long elapsed = System.nanoTime() - startTime;
		deadline += elapsed;
	}

	@Override
	protected void sizeBuffer() {
		dataBuffer = ThreadLocalData.getBuffer();

		if (dataOffset > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(dataOffset);
		}
	}

	protected void sizeBuffer(int size) {
		if (size > dataBuffer.length) {
			dataBuffer = ThreadLocalData.resizeBuffer(size);
		}
	}

	protected boolean retryBatch(
		Cluster cluster,
		int socketTimeout,
		int totalTimeout,
		long deadline,
		int iteration,
		int commandSentCounter
	) {
		return false;
	}

	protected boolean isSingle() {
		return true;
	}

	protected boolean isWrite() {
		return false;
	}

	protected abstract Node getNode();
	protected abstract void writeBuffer();
	protected abstract void parseResult(Connection conn) throws AerospikeException, IOException;
	protected abstract boolean prepareRetry(boolean timeout);
}
