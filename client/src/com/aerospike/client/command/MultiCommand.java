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
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Connection.ReadTimeout;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.QueryValidate;

public abstract class MultiCommand extends SyncCommand {
	private static final int MAX_BUFFER_SIZE = 1024 * 1024 * 128;  // 128 MB

	private final Node node;
	private final long clusterKey;
	protected int info3;
	protected int resultCode;
	protected int generation;
	protected int expiration;
	protected int batchIndex;
	protected int fieldCount;
	protected int opCount;
	protected final boolean isOperation;
	private final boolean first;
	protected volatile boolean valid = true;

	/**
	 * Batch and server execute constructor.
	 */
	protected MultiCommand(Cluster cluster, Policy policy, Node node, boolean isOperation) {
		super(cluster, policy, null);
		this.node = node;
		this.isOperation = isOperation;
		this.clusterKey = 0;
		this.first = false;
	}

	/**
	 * Partition scan/query constructor.
	 */
	protected MultiCommand(Cluster cluster, Policy policy, Node node, String namespace, int socketTimeout, int totalTimeout) {
		super(cluster, policy, socketTimeout, totalTimeout, namespace);
		this.node = node;
		this.isOperation = false;
		this.clusterKey = 0;
		this.first = false;
	}

	/**
	 * Legacy scan/query constructor.
	 */
	protected MultiCommand(Cluster cluster, Policy policy, Node node, String namespace, long clusterKey, boolean first) {
		super(cluster, policy, policy.socketTimeout, policy.totalTimeout, namespace);
		this.node = node;
		this.isOperation = false;
		this.clusterKey = clusterKey;
		this.first = first;
	}

	public void executeAndValidate(int infoTimeout) {
		if (clusterKey != 0) {
			if (! first) {
				QueryValidate.validate(node, namespace, clusterKey, infoTimeout);
			}
			super.execute();
			QueryValidate.validate(node, namespace, clusterKey, infoTimeout);
		}
		else {
			super.execute();
		}
	}

	@Override
	protected boolean isSingle() {
		return false;
	}

	@Override
	protected Node getNode() {
		return node;
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		return true;
	}

	@Override
	protected final void parseResult(Node node, Connection conn) throws IOException {
		// Read blocks of records.  Do not use thread local receive buffer because each
		// block will likely be too big for a cache.  Also, scan callbacks can nest
		// further database commands which would contend with the thread local receive buffer.
		// Instead, use separate heap allocated buffers.
		byte[] buf = null;
		byte[] ubuf = null;
		int receiveSize;
		int bytesIn = 0;

		while (true) {
			// Read header
			byte[] protoBuf = new byte[8];
			conn.readFully(protoBuf, 8, Command.STATE_READ_HEADER);
			bytesIn += 8;

			long proto = Buffer.bytesToLong(protoBuf, 0);
			int size = (int)(proto & 0xFFFFFFFFFFFFL);

			if (size <= 0) {
				continue;
			}

			// Prepare buffer
			if (buf == null || size > buf.length) {
				// Corrupted data streams can result in a huge length.
				// Do a sanity check here.
				if (size > MAX_BUFFER_SIZE) {
					throw new AerospikeException("Invalid proto size: " + size);
				}

				int capacity = (size + 16383) & ~16383; // Round up in 16KB increments.
				buf = new byte[capacity];
			}

			// Read remaining message bytes in group.
			try {
				conn.readFully(buf, size, Command.STATE_READ_DETAIL);
				bytesIn += size;
				if (node.areMetricsEnabled()) {
					node.addBytesIn(namespace, bytesIn);
				}
				conn.updateLastUsed();
			}
			catch (ReadTimeout rt) {
				if (rt.offset >= 4) {
					throw rt;
				}

				// First 4 bytes of detail contains whether this is the last
				// group to be sent.  Consider this as part of header.
				// Copy proto back into buffer to complete header.
				byte[] b = new byte[12];
				int count = 0;

				for (int i = 0; i < 8; i++) {
					b[count++] = protoBuf[i];
				}

				for (int i = 0; i < rt.offset; i++) {
					b[count++] = buf[i];
				}

				throw new ReadTimeout(b, rt.offset + 8, count, Command.STATE_READ_HEADER);
			}

			long type = (proto >> 48) & 0xff;

			if (type == Command.AS_MSG_TYPE) {
				dataBuffer = buf;
				dataOffset = 0;
				receiveSize = size;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED) {
				int usize = (int)Buffer.bytesToLong(buf, 0);

				if (ubuf == null || usize > ubuf.length) {
					if (usize > MAX_BUFFER_SIZE) {
						throw new AerospikeException("Invalid proto size: " + usize);
					}

					int capacity = (usize + 16383) & ~16383; // Round up in 16KB increments.
					ubuf = new byte[capacity];
				}

				Inflater inf = new Inflater();
				try {
					inf.setInput(buf, 8, size - 8);
					int rsize;

					try {
						rsize = inf.inflate(ubuf);
					}
					catch (DataFormatException dfe) {
						throw new AerospikeException.Serialize(dfe);
					}

					if (rsize != usize) {
						throw new AerospikeException("Decompressed size " + rsize + " is not expected " + usize);
					}
					dataBuffer = ubuf;
					dataOffset = 8;
					receiveSize = usize - 8;
				} finally {
					inf.end();
				}
			}
			else {
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			if (! parseGroup(receiveSize)) {
				break;
			}
		}
	}

	private boolean parseGroup(int receiveSize) {
		while (dataOffset < receiveSize) {
			dataOffset += 3;
			info3 = dataBuffer[dataOffset] & 0xFF;
			dataOffset += 2;
			resultCode = dataBuffer[dataOffset] & 0xFF;

			// If this is the end marker of the response, do not proceed further.
			if ((info3 & Command.INFO3_LAST) != 0) {
				if (resultCode != 0) {
					// The server returned a fatal error.
					throw new AerospikeException(resultCode);
				}
				return false;
			}

			dataOffset++;
			generation = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			expiration = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			batchIndex = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			fieldCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			opCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;

			// Note: parseRow() also handles sync error responses.
			if (! parseRow()) {
				return false;
			}
		}
		return true;
	}

	protected abstract boolean parseRow();

	protected final Record parseRecord() {
		if (opCount <= 0) {
			return new Record(null, generation, expiration);
		}

		return parseRecord(opCount, generation, expiration, isOperation);
	}

	public void stop() {
		valid = false;
	}

	public boolean isValid() {
		return valid;
	}
}
