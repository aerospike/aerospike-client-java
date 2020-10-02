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
package com.aerospike.client.async;

import java.util.ArrayDeque;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.policy.Policy;

/**
 * Asynchronous command handler.
 */
public abstract class AsyncCommand extends Command {
	static final int MAX_BUFFER_SIZE = 1024 * 128;  // 128 KB
	static final int REGISTERED = 1;
	static final int DELAY_QUEUE = 2;
	static final int CHANNEL_INIT = 3;
	static final int CONNECT = 4;
	static final int TLS_HANDSHAKE = 5;
	static final int AUTH_WRITE = 6;
	static final int AUTH_READ_HEADER = 7;
	static final int AUTH_READ_BODY = 8;
	static final int COMMAND_WRITE = 9;
	static final int COMMAND_READ_HEADER = 10;
	static final int COMMAND_READ_BODY = 11;
	static final int COMPLETE = 12;

	Policy policy;
	ArrayDeque<byte[]> bufferQueue;
	int receiveSize;
	final boolean isSingle;
	boolean compressed;
	boolean valid = true;

	/**
	 * Default constructor.
	 */
	public AsyncCommand(Policy policy, boolean isSingle) {
		super(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
		this.policy = policy;
		this.isSingle = isSingle;
	}

	/**
	 * Scan/Query constructor.
	 */
	public AsyncCommand(Policy policy, int socketTimeout, int totalTimeout) {
		super(socketTimeout, totalTimeout, 0);
		this.policy = policy;
		this.isSingle = false;
	}

	final int parseProto(long proto) {
		compressed = (((proto >> 48) & 0xFF) == Command.MSG_TYPE_COMPRESSED);
		receiveSize = (int)(proto & 0xFFFFFFFFFFFFL);
		return receiveSize;
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

	final boolean parseCommandResult() {
		if (compressed) {
			int usize = (int)Buffer.bytesToLong(dataBuffer, 0);
			byte[] buf = new byte[usize];

			Inflater inf = new Inflater();
			inf.setInput(dataBuffer, 8, receiveSize - 8);
			int rsize;

			try {
				rsize = inf.inflate(buf);
			}
			catch (DataFormatException dfe) {
				throw new AerospikeException.Serialize(dfe);
			}

			if (rsize != usize) {
				throw new AerospikeException("Decompressed size " + rsize + " is not expected " + usize);
			}

			dataBuffer = buf;
			dataOffset = 8;
			receiveSize = usize - 8;
		}
		else {
			dataOffset = 0;
		}
		return parseResult();
	}

	final void stop() {
		valid = false;
	}

	boolean retryBatch(Runnable command, long deadline) {
		// Override this method in batch to regenerate node assignments.
		return false;
	}

	boolean isWrite() {
		return false;
	}

	abstract Node getNode(Cluster cluster);
	abstract void writeBuffer();
	abstract boolean parseResult();
	abstract boolean prepareRetry(boolean timeout);
	abstract void onSuccess();
	abstract void onFailure(AerospikeException ae);
}
