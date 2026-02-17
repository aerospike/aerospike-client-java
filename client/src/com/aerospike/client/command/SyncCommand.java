/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.ConnectionRecover;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public abstract class SyncCommand extends Command {
	// private static final AtomicLong TranCounter = new AtomicLong();
	protected final Cluster cluster;
	protected final Policy policy;
	ArrayList<AerospikeException> subExceptions;
	int iteration = 1;
	int commandSentCounter;
	long deadline;
	String namespace;

	/**
	 * Default constructor.
	 */
	public SyncCommand(Cluster cluster, Policy policy, String namespace) {
		super(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		this.cluster = cluster;
		this.policy = policy;
		this.namespace = namespace;
	}

	/**
	 * Scan/Query constructor.
	 */
	public SyncCommand(Cluster cluster, Policy policy, int socketTimeout, int totalTimeout, String namespace) {
		super(socketTimeout, totalTimeout, 0);
		this.cluster = cluster;
		this.policy = policy;
		this.namespace = namespace;
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
		long begin = 0;
		boolean metricsEnabled = cluster.metricsEnabled;
		LatencyType latencyType = metricsEnabled? getLatencyType() : LatencyType.NONE;
		boolean isClientTimeout;
		int retryInterval = policy.sleepBetweenRetries;
		double sleepMultiplier = policy.sleepMultiplier;

		// Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			try {
				node = getNode();
			}
			catch (AerospikeException ae) {
				if (cluster.isActive()) {
					// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
					prepareException(null, ae, subExceptions);
					throw ae;
				}
				else {
					throw new AerospikeException("Cluster has been closed");
				}
			}

			try {
				node.validateErrorCount();

				if (latencyType != LatencyType.NONE) {
					begin = System.nanoTime();
				}

				Connection conn = node.getConnection(this, policy.connectTimeout, socketTimeout, policy.timeoutDelay);

				try {
					// Set command buffer.
					writeBuffer();

					// Send command.
					conn.write(dataBuffer, dataOffset);
					commandSentCounter++;

					if (metricsEnabled) {
						node.addBytesOut(namespace, dataOffset);
					}

					// Parse results.
					parseResult(node, conn);

					// Put connection back in pool.
					node.putConnection(conn);

					if (latencyType != LatencyType.NONE) {
						long elapsed = System.nanoTime() - begin;
						node.addLatency(namespace, latencyType, elapsed);
					}

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
						node.incrErrorRate();
						node.addTimeout(namespace);
					}
					else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
						// Add to circuit breaker error count and retry.
						exception = ae;
						isClientTimeout = false;
						node.incrErrorRate();
						node.addError(namespace);
					}
					else if (ae.getResultCode() == ResultCode.KEY_BUSY) {
						exception = ae;
						isClientTimeout = false;
						node.incrErrorRate();
						node.addKeyBusy(namespace);
					}
					else {
						node.addError(namespace);
						prepareException(node, ae, subExceptions);
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
					exception = new AerospikeException.Timeout(policy, true);
					isClientTimeout = true;
					node.addTimeout(namespace);
				}
				catch (SocketTimeoutException ste) {
					// Full timeout has been reached.
					// Log.info("Socket timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException.Timeout(policy, true);
					isClientTimeout = true;
					node.addTimeout(namespace);
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
					node.addError(namespace);
				}
				catch (Throwable t) {
					// All remaining exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					// Log.info("Throw Throwable: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					node.addError(namespace);
					AerospikeException ae = new AerospikeException(t);
					prepareException(node, ae, subExceptions);
					throw ae;
				}
			}
			catch (Connection.ReadTimeout crt) {
				// Connection already handled.
				exception = new AerospikeException.Timeout(policy, true);
				isClientTimeout = true;
				node.addTimeout(namespace);
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Retry.
				// Log.info("Connection error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = ce;
				isClientTimeout = false;
				node.addError(namespace);
			}
			catch (AerospikeException.Backoff be) {
				// Node is in backoff state. Retry, hopefully on another node.
				// Log.info("Backoff error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = be;
				isClientTimeout = false;
				node.addError(namespace);
			}
			catch (AerospikeException ae) {
				// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
				node.addError(namespace);
				prepareException(node, ae, subExceptions);
				throw ae;
			}
			catch (Throwable t) {
				node.addError(namespace);
				AerospikeException ae = new AerospikeException(t);
				prepareException(node, ae, subExceptions);
				throw ae;
			}

			// Check maxRetries.
			if (iteration > maxRetries) {
				break;
			}

			if (totalTimeout > 0) {
				// Check for total timeout.
				long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(retryInterval);

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

			if (!isClientTimeout && retryInterval > 0) {
				// Sleep before trying again.
				Util.sleep(retryInterval);
				if (sleepMultiplier > 1) {
					retryInterval = (int) Math.round(retryInterval * sleepMultiplier);
				}
			}

			exception.setNode(node);
			exception.setPolicy(policy);
			exception.setIteration(iteration);
			exception.setInDoubt(isWrite(), commandSentCounter);
			addSubException(exception);
			iteration++;

			if (! prepareRetry(isClientTimeout || exception.getResultCode() != ResultCode.SERVER_NOT_AVAILABLE)) {
				// Batch may be retried in separate commands.
				if (retryBatch(cluster, socketTimeout, totalTimeout, deadline, iteration, commandSentCounter)) {
					// Batch was retried in separate commands.  Complete this command.
					return;
				}
			}

			cluster.addRetry();
		}

		// Retries have been exhausted.  Throw last exception.
		// Log.info("Runtime exception: " + tranId + ',' + sequence + ',' + iteration + ',' + exception.getMessage());
		prepareException(node, exception, subExceptions);
		throw exception;
	}

	protected void addSubException(AerospikeException exception) {
		if (subExceptions == null) {
			subExceptions = new ArrayList<AerospikeException>(policy.maxRetries);
		}
		subExceptions.add(exception);
	}

	private void prepareException(Node node, AerospikeException ae, List<AerospikeException> subExceptions) {
		ae.setNode(node);
		ae.setPolicy(policy);
		ae.setIteration(iteration);
		ae.setInDoubt(isWrite(), commandSentCounter);
		ae.setSubExceptions(subExceptions);

		if (ae.getInDoubt()) {
			onInDoubt();
		}
	}

	protected void onInDoubt() {
		// Write commands will override this method.
	}

	public void resetDeadline(long startTime) {
		long elapsed = System.nanoTime() - startTime;
		deadline += elapsed;
	}

	@Override
	protected void sizeBuffer() {
		dataBuffer = new byte[dataOffset];
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
	protected abstract LatencyType getLatencyType();
	protected abstract void writeBuffer();
	protected abstract void parseResult(Node node, Connection conn) throws AerospikeException, IOException;
	protected abstract boolean prepareRetry(boolean timeout);
}
