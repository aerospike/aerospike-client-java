/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.client;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

/**
 * Aerospike exceptions that can be thrown from the client.
 */
public class AerospikeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	protected transient Node node;
	protected transient Policy policy;
	protected int resultCode = ResultCode.CLIENT_ERROR;
	protected int iteration = -1;
	protected boolean inDoubt;

	public AerospikeException(int resultCode, String message) {
		super(message);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, Throwable e) {
		super(e);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode) {
		super();
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, boolean inDoubt) {
		super();
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
	}

	public AerospikeException(String message, Throwable e) {
		super(message, e);
	}

	public AerospikeException(String message) {
		super(message);
	}

	public AerospikeException(Throwable e) {
		super(e);
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder(512);
		String message = super.getMessage();

		sb.append("Error ");
		sb.append(resultCode);

		if (iteration >= 0) {
			sb.append(',');
			sb.append(iteration);
		}

		if (policy != null) {
			sb.append(',');
			sb.append(policy.socketTimeout);
			sb.append(',');
			sb.append(policy.totalTimeout);
			sb.append(',');
			sb.append(policy.maxRetries);
		}

		if (inDoubt) {
			sb.append(",inDoubt");
		}

		if (node != null) {
			sb.append(',');
			sb.append(node.toString());
		}

		sb.append(": ");

		if (message != null) {
			sb.append(message);
		}
		else {
			sb.append(ResultCode.getResultString(resultCode));
		}
		return sb.toString();
	}

	/**
	 * Should connection be put back into pool.
	 */
	public final boolean keepConnection() {
		return ResultCode.keepConnection(resultCode);
	}

	/**
	 * Get last node used.
	 */
	public final Node getNode() {
		return node;
	}

	/**
	 * Set last node used.
	 */
	public final void setNode(Node node) {
		this.node = node;
	}

	/**
	 * Get transaction policy.  Will be null for non-transaction exceptions.
	 */
	public final Policy getPolicy() {
		return policy;
	}

	/**
	 * Set transaction policy.
	 */
	public final void setPolicy(Policy policy) {
		this.policy = policy;
	}

	/**
	 * Get integer result code.
	 */
	public final int getResultCode() {
		return resultCode;
	}

	/**
	 * Get number of attempts before failing.
	 */
	public final int getIteration() {
		return iteration;
	}

	/**
	 * Set number of attempts before failing.
	 */
	public final void setIteration(int iteration) {
		this.iteration = iteration;
	}

	/**
	 * Is it possible that write transaction may have completed.
	 */
	public final boolean getInDoubt() {
		return inDoubt;
	}

	/**
	 * Set whether it is possible that the write transaction may have completed
	 * even though this exception was generated.  This may be the case when a
	 * client error occurs (like timeout) after the command was sent to the server.
	 */
	public final void setInDoubt(boolean isWrite, int commandSentCounter) {
		if (isWrite && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0)))) {
			this.inDoubt = true;
		}
	}

	/**
	 * Exception thrown when database request expires before completing.
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * Socket idle timeout in milliseconds.
		 */
		public int socketTimeout;

		/**
		 * Total timeout in milliseconds.
		 */
		public int timeout;

		/**
		 * If true, client initiated timeout.  If false, server initiated timeout.
		 */
		public boolean client;

		public Timeout(int totalTimeout, boolean inDoubt) {
			super(ResultCode.TIMEOUT, inDoubt);
			this.socketTimeout = 0;
			this.timeout = totalTimeout;
			this.client = true;
		}

		public Timeout(Policy policy, boolean client) {
			super(ResultCode.TIMEOUT);
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = client;
		}

		public Timeout(Policy policy, int iteration) {
			super(ResultCode.TIMEOUT);
			super.node = node;
			super.iteration = iteration;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = true;
		}

		public Timeout(Node node, int totalTimeout) {
			super(ResultCode.TIMEOUT);
			super.node = node;
			super.iteration = 1;
			this.socketTimeout = 0;
			this.timeout = totalTimeout;
			this.client = true;
		}

		@Override
		public String getMessage() {
			if (iteration == -1) {
				return "Client timeout: " + timeout;
			}

			StringBuilder sb = new StringBuilder(512);

			if (client) {
				sb.append("Client");
			}
			else {
				sb.append("Server");
			}
			sb.append(" timeout:");
			sb.append(" iteration=");
			sb.append(iteration);
			sb.append(" socket=");
			sb.append(socketTimeout);
			sb.append(" total=");
			sb.append(timeout);

			if (policy != null) {
				sb.append(" maxRetries=");
				sb.append(policy.maxRetries);
			}
			sb.append(" node=");
			sb.append(node);
			sb.append(" inDoubt=");
			sb.append(inDoubt);
			return sb.toString();
		}
	}

	/**
	 * Exception thrown when Java serialization error occurs.
	 */
	public static final class Serialize extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Serialize(Exception e) {
			super(ResultCode.SERIALIZE_ERROR, e);
		}
	}

	/**
	 * Exception thrown when client can't parse data returned from server.
	 */
	public static final class Parse extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Parse(String message) {
			super(ResultCode.PARSE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when client can't connect to the server.
	 */
	public static final class Connection extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Connection(String message) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message);
		}

		public Connection(Exception e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, e);
		}

		public Connection(int resultCode, String message) {
			super(resultCode, message);
		}
	}

	/**
	 * Exception thrown when chosen node is not active.
	 */
	public static final class InvalidNode extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNode(int clusterSize, Partition partition) {
			super(ResultCode.INVALID_NODE_ERROR,
				(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition);
		}

		public InvalidNode(int partitionId) {
			super(ResultCode.INVALID_NODE_ERROR, "Node not found for partition " + partitionId);
		}

		public InvalidNode(String message) {
			super(ResultCode.INVALID_NODE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when namespace is invalid.
	 */
	public static final class InvalidNamespace extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNamespace(String ns, int mapSize) {
			super(ResultCode.INVALID_NAMESPACE,
				(mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns);
		}
	}

	/**
	 * Exception thrown when scan was terminated prematurely.
	 */
	public static final class ScanTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public ScanTerminated() {
			super(ResultCode.SCAN_TERMINATED);
		}

		public ScanTerminated(Exception e) {
			super(ResultCode.SCAN_TERMINATED, e);
		}
	}

	/**
	 * Exception thrown when query was terminated prematurely.
	 */
	public static final class QueryTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public QueryTerminated() {
			super(ResultCode.QUERY_TERMINATED);
		}

		public QueryTerminated(Exception e) {
			super(ResultCode.QUERY_TERMINATED, e);
		}
	}

	/**
	 * Exception thrown when async command was rejected because the
	 * async delay queue is full.
	 */
	public static final class AsyncQueueFull extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public AsyncQueueFull() {
			super(ResultCode.ASYNC_QUEUE_FULL);
		}
	}
}
