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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.command.Command.OpResults;

public final class RecordParser {
	public final byte[] dataBuffer;
	public final int resultCode;
	public final int generation;
	public final int expiration;
	public final int fieldCount;
	public final int opCount;
	public int dataOffset;
	public long bytesIn;

	/**
	 * Sync record parser.
	 */
	public RecordParser(Connection conn, byte[] buffer) throws IOException {
		bytesIn = 0;
		// Read header.
		conn.readFully(buffer, 8, Command.STATE_READ_HEADER);
		bytesIn += 8;

		long sz = Buffer.bytesToLong(buffer, 0);
		int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

		if (receiveSize <= 0) {
			throw new AerospikeException("Invalid receive size: " + receiveSize);
		}

		/*
		byte version = (byte) (((int)(sz >> 56)) & 0xff);
		if (version != MSG_VERSION) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect version.");
			}
		}

		if (type != MSG_TYPE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: incorrect message type, aborting receive");
			}
		}

		if (headerLength != MSG_REMAINING_HEADER_SIZE) {
			if (Log.debugEnabled()) {
				Log.debug("read header: unexpected header size, aborting");
			}
		}*/

		// Read remaining message bytes.
		if (receiveSize > buffer.length) {
			buffer = new byte[receiveSize];
		}

		conn.readFully(buffer, receiveSize, Command.STATE_READ_DETAIL);
		bytesIn += receiveSize;
		conn.updateLastUsed();

		long type = (sz >> 48) & 0xff;
		int offset;

		if (type == Command.AS_MSG_TYPE) {
			offset = 5;
		}
		else if (type == Command.MSG_TYPE_COMPRESSED) {
			int usize = (int)Buffer.bytesToLong(buffer, 0);
			byte[] buf = new byte[usize];

			Inflater inf = new Inflater();
			try {
				inf.setInput(buffer, 8, receiveSize - 8);
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

				buffer = buf;
				offset = 13;
			} finally {
				inf.end();
			}
		}
		else {
			throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
		}

		resultCode = buffer[offset] & 0xFF;
		offset++;
		generation = Buffer.bytesToInt(buffer, offset);
		offset += 4;
		expiration = Buffer.bytesToInt(buffer, offset);
		offset += 8;
		fieldCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		opCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		dataOffset = offset;
		dataBuffer = buffer;
	}

	/**
	 * Async record parser.
	 */
	public RecordParser(byte[] buffer, int offset, int receiveSize) {
		if (receiveSize < Command.MSG_REMAINING_HEADER_SIZE) {
			throw new AerospikeException.Parse("Invalid receive size: " + receiveSize);
		}

		offset += 5;
		resultCode = buffer[offset] & 0xFF;
		offset++;
		generation = Buffer.bytesToInt(buffer, offset);
		offset += 4;
		expiration = Buffer.bytesToInt(buffer, offset);
		offset += 8;
		fieldCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		opCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		dataOffset = offset;
		dataBuffer = buffer;
	}

	public void parseFields(Txn txn, Key key, boolean hasWrite) {
		if (txn == null) {
			skipFields();
			return;
		}

		Long version = null;

		for (int i = 0; i < fieldCount; i++) {
			int len = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int type = dataBuffer[dataOffset++];
			int size = len - 1;

			if (type == FieldType.RECORD_VERSION) {
				if (size == 7) {
					version = Buffer.versionBytesToLong(dataBuffer, dataOffset);
				}
				else {
					throw new AerospikeException("Record version field has invalid size: " + size);
				}
			}
			dataOffset += size;
		}

		if (hasWrite) {
			txn.onWrite(key, version, resultCode);
		} else {
			txn.onRead(key, version);
		}
	}

	public void parseTranDeadline(Txn txn) {
		for (int i = 0; i < fieldCount; i++) {
			int len = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int type = dataBuffer[dataOffset++];
			int size = len - 1;

			if (type == FieldType.TXN_DEADLINE) {
				int deadline = Buffer.littleBytesToInt(dataBuffer, dataOffset);
				txn.setDeadline(deadline);
			}
			dataOffset += size;
		}
	}

	private void skipFields() {
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4 + fieldlen;
		}
	}

	public Record parseRecord(boolean isOperation)  {
		if (opCount == 0) {
			// Bin data was not returned.
			return new Record(null, generation, expiration);
		}

		Map<String,Object> bins = new LinkedHashMap<>();

		for (int i = 0 ; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(dataBuffer, dataOffset);
			byte particleType = dataBuffer[dataOffset + 5];
			byte nameSize = dataBuffer[dataOffset + 7];
			String name = Buffer.utf8ToString(dataBuffer, dataOffset + 8, nameSize);
			dataOffset += 4 + 4 + nameSize;

			int particleBytesSize = opSize - (4 + nameSize);
			Object value = Buffer.bytesToParticle(particleType, dataBuffer, dataOffset, particleBytesSize);
			dataOffset += particleBytesSize;

			if (isOperation) {
				if (bins.containsKey(name)) {
					// Multiple values returned for the same bin.
					Object prev = bins.get(name);

					if (prev instanceof OpResults) {
						// List already exists.  Add to it.
						OpResults list = (OpResults)prev;
						list.add(value);
					}
					else {
						// Make a list to store all values.
						OpResults list = new OpResults();
						list.add(prev);
						list.add(value);
						bins.put(name, list);
					}
				}
				else {
					bins.put(name, value);
				}
			}
			else {
				bins.put(name, value);
			}
		}
		return new Record(bins, generation, expiration);
	}
}
