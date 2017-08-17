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
package com.aerospike.client;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

/**
 * Aerospike exceptions that can be thrown from the client.
 */
public class AerospikeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	private int resultCode;
	
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
	
	public AerospikeException(String message, Throwable e) {
		super(message, e);
	}

	public AerospikeException(String message) {
		super(message);
	}

	public AerospikeException(Throwable e) {
		super(e);
	}
	
	public AerospikeException() {
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder();
		String message = super.getMessage();
		
		if (resultCode != 0) {
			sb.append("Error Code ");
			sb.append(resultCode);
			sb.append(": ");

			if (message != null) {
				sb.append(message);
			}
			else {
				sb.append(ResultCode.getResultString(resultCode));
			}
		}
		else {
			if (message != null) {
				sb.append(message);
			}
			else {
				sb.append(this.getClass().getName());
			}
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
	 * Get integer result code.
	 */
	public final int getResultCode() {
		return resultCode;
	}
	
	/**
	 * Exception thrown when database request expires before completing.
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;
		
		/**
		 * Last node used before timeout.
		 */
		public Node node;
		
		/**
		 * Socket idle timeout in milliseconds.
		 */
		public int socketTimeout;
		
		/**
		 * Total timeout in milliseconds.
		 */
		public int timeout;
		
		/**
		 * Number of attempts before failing.
		 */
		public int iterations;
		
		/**
		 * If true, client initiated timeout.  If false, server initiated timeout.
		 */
		public boolean client;
		
		public Timeout(int totalTimeout) {
			super(ResultCode.TIMEOUT);
			this.timeout = totalTimeout;
			this.iterations = -1;
			this.client = true;
		}
		
		public Timeout(Node node, Policy policy, int iterations, boolean client) {
			super(ResultCode.TIMEOUT);
			this.node = node;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.iterations = iterations;
			this.client = client;
		}
		
		@Override
		public String getMessage() {
			if (iterations == -1) {
				return "Client timeout: " + timeout;
			}
			String type = client ? "Client" : "Server";
			return type + " timeout: socket=" + socketTimeout + " total=" + timeout + " iterations=" + iterations + 
				" lastNode=" + node;
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

		public InvalidNode() {
			super(ResultCode.INVALID_NODE_ERROR);
		}
		
		public InvalidNode(String message) {
			super(ResultCode.INVALID_NODE_ERROR, message);
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
	 * Exception thrown when asynchronous command was rejected because the 
	 * max concurrent database commands have been exceeded.
	 */
	public static final class CommandRejected extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public CommandRejected() {
			super(ResultCode.COMMAND_REJECTED);
		}
	}
}
