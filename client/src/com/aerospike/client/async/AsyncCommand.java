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
package com.aerospike.client.async;

import java.util.ArrayDeque;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;

/**
 * Asynchronous command handler.
 */
public abstract class AsyncCommand extends Command {
	static final int MAX_BUFFER_SIZE = 1024 * 128;  // 128 KB	
	static final int REGISTERED = 1;
	static final int DELAY_QUEUE = 2;
	static final int CONNECT = 3;
	static final int AUTH_WRITE = 4;
	static final int AUTH_READ_HEADER = 5;
	static final int AUTH_READ_BODY = 6;
	static final int COMMAND_WRITE = 7;
	static final int COMMAND_READ_HEADER = 8;
	static final int COMMAND_READ_BODY = 9;
	static final int COMPLETE = 10;
	
	Policy policy;
	final Partition partition;
	Node node;
	ArrayDeque<byte[]> bufferQueue;
	int receiveSize;
	final boolean isRead;
	final boolean isSingle;
	boolean valid = true;

	public AsyncCommand(Policy policy, Partition partition, Node node, boolean isRead) {
		this.policy = policy;
		this.partition = partition;
		this.node = node;
		this.isRead = isRead;
		this.isSingle = partition != null;
	}

	public AsyncCommand(Policy policy, Node node, boolean isRead, boolean isSingle) {
		this.policy = policy;
		this.partition = null;
		this.node = node;
		this.isRead = isRead;
		this.isSingle = isSingle;
	}

	final Node getNode(Cluster cluster) {		
		if (partition != null) {
			node = getNode(cluster, partition, policy.replica, isRead);
		}
		return node;
	}
	
	final void initBuffer() {
		dataBuffer = getBuffer(8192);
	}
	
	@Override
	protected final void sizeBuffer() {
		sizeBuffer(dataOffset);
	}
	
	final void sizeBuffer(int size) {
		if (dataBuffer == null) {
			dataBuffer = getBuffer(size);
		}
		else {
			if (size > dataBuffer.length) {
				dataBuffer = resizeBuffer(dataBuffer, size);
			}
		}
	}

	final void putBuffer() {
		if (dataBuffer != null) {
			putBuffer(dataBuffer);
			dataBuffer = null;
		}
	}
	
	private final byte[] getBuffer(int size) {
		if (size > MAX_BUFFER_SIZE) {
			// Allocate huge buffer, but do not put back in pool.
			return new byte[size];
		}
		
		byte[] buffer = bufferQueue.pollFirst();
		
		if (buffer == null || buffer.length < size) {
			// Round up to nearest 8KB.
			buffer = new byte[(size + 8191) & ~8191];
		}
		return buffer;
	}
	
	private final byte[] resizeBuffer(byte[] buffer, int size) {
		if (size > MAX_BUFFER_SIZE) {
			// Put original buffer back in pool.
			putBuffer(buffer);
			// Allocate huge buffer, but do not put back in pool.
			return new byte[size];
		}
		// Round up to nearest 8KB.
		return new byte[(size + 8191) & ~8191];
	}
	
	private final void putBuffer(byte[] buffer) {
		if (buffer.length <= MAX_BUFFER_SIZE) {
			bufferQueue.addLast(buffer);
		}
	}
	
	final void validateHeaderSize() {
		if (receiveSize < Command.MSG_REMAINING_HEADER_SIZE) {
			throw new AerospikeException.Parse("Invalid receive size: " + receiveSize);
		}
	}

	final void stop() {
		valid = false;
	}

	abstract void writeBuffer();
	abstract boolean parseResult();
	abstract void onSuccess();
	abstract void onFailure(AerospikeException ae);
}
