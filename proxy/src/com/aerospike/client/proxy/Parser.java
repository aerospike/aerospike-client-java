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
package com.aerospike.client.proxy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.Command.OpResults;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.query.BVal;

public final class Parser {
	private byte[] buffer;
	private int offset;
	private int receiveSize;
	private int resultCode;
	int generation;
	int expiration;
	int batchIndex;
	int fieldCount;
	int opCount;
	int info3;

	public Parser(byte[] buffer) {
		this.buffer = buffer;
	}

	public void parseProto() {
		long sz = Buffer.bytesToLong(buffer, offset);
		receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);
		int totalSize = receiveSize + 8;

		if (totalSize != buffer.length) {
			throw new AerospikeException("size " + totalSize + " != buffer length " + buffer.length);
		}

		offset += 8;
		long type = (sz >> 48) & 0xff;

		if (type == Command.AS_MSG_TYPE) {
			offset += 5;
		}
		else if (type == Command.MSG_TYPE_COMPRESSED) {
			int usize = (int)Buffer.bytesToLong(buffer, offset);
			offset += 8;

			byte[] buf = new byte[usize];

			Inflater inf = new Inflater();
			try {
				inf.setInput(buffer, offset, receiveSize - 8);
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
			}
			finally {
				inf.end();
			}
		}
		else {
			throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
		}
		info3 = buffer[offset - 2] & 0xFF;
    }

	public int parseResultCode() {
		return buffer[offset] & 0xFF;
	}

	public int parseHeader() {
		resultCode = parseResultCode();
		offset += 1;
		generation = Buffer.bytesToInt(buffer, offset);
		offset += 4;
		expiration = Buffer.bytesToInt(buffer, offset);
		offset += 4;
		batchIndex = Buffer.bytesToInt(buffer, offset);
		offset += 4;
		fieldCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		opCount = Buffer.bytesToShort(buffer, offset);
		offset += 2;
		return resultCode;
	}

	public void skipKey() {
		// There can be fields in the response (setname etc).
		// But for now, ignore them. Expose them to the API if needed in the future.
		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(buffer, offset);
			offset += 4 + fieldlen;
		}
	}

	public Key parseKey(BVal bVal) {
		byte[] digest = null;
		String namespace = null;
		String setName = null;
		Value userKey = null;

		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(buffer, offset);
			offset += 4;

			int fieldtype = buffer[offset++];
			int size = fieldlen - 1;

			switch (fieldtype) {
				case FieldType.DIGEST_RIPE:
					digest = new byte[size];
					System.arraycopy(buffer, offset, digest, 0, size);
					break;

				case FieldType.NAMESPACE:
					namespace = Buffer.utf8ToString(buffer, offset, size);
					break;

				case FieldType.TABLE:
					setName = Buffer.utf8ToString(buffer, offset, size);
					break;

				case FieldType.KEY:
					int type = buffer[offset++];
					size--;
					userKey = Buffer.bytesToKeyValue(type, buffer, offset, size);
					break;

				case FieldType.BVAL_ARRAY:
					bVal.val = Buffer.littleBytesToLong(buffer, offset);
					break;
			}
			offset += size;
		}
		return new Key(namespace, digest, setName, userKey);
	}

	public Map<String, String> parseInfoResult(){
		HashMap<String, String> responses;
		Info info = new Info(buffer, receiveSize);
		responses = info.parseMultiResponse();
		return responses;
	}



	public Record parseRecord(boolean isOperation) {
		Map<String, Object> bins = new LinkedHashMap<>();

		for (int i = 0; i < opCount; i++) {
			int opSize = Buffer.bytesToInt(buffer, offset);
			byte particleType = buffer[offset + 5];
			byte nameSize = buffer[offset + 7];
			String name = Buffer.utf8ToString(buffer, offset + 8, nameSize);
			offset += 4 + 4 + nameSize;

			int particleBytesSize = opSize - (4 + nameSize);
			Object value = Buffer.bytesToParticle(particleType, buffer, offset, particleBytesSize);
			offset += particleBytesSize;

			if (isOperation) {
				if (bins.containsKey(name)) {
					// Multiple values returned for the same bin.
					Object prev = bins.get(name);

					if (prev instanceof Command.OpResults) {
						// List already exists.  Add to it.
						Command.OpResults list = (Command.OpResults)prev;
						list.add(value);
					}
					else {
						// Make a list to store all values.
						Command.OpResults list = new OpResults();
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

	public LuaValue getLuaAggregateValue(LuaInstance instance) {
		// Parse aggregateValue.
		int opSize = Buffer.bytesToInt(buffer, offset);
		offset += 5;
		byte particleType = buffer[offset];
		offset += 2;
		byte nameSize = buffer[offset++];

		String name = Buffer.utf8ToString(buffer, offset, nameSize);
		offset += nameSize;

		int particleBytesSize = opSize - (4 + nameSize);

		if (!name.equals("SUCCESS")) {
			if (name.equals("FAILURE")) {
				Object value = Buffer.bytesToParticle(particleType, buffer, offset, particleBytesSize);
				throw new AerospikeException(ResultCode.QUERY_GENERIC, value != null ? value.toString() : null);
			}
			else {
				throw new AerospikeException(ResultCode.PARSE_ERROR, "Query aggregate expected bin name SUCCESS.  Received " + name);
			}
		}

		LuaValue aggregateValue = instance.getLuaValue(particleType, buffer, offset, particleBytesSize);
		offset += particleBytesSize;
		return aggregateValue;
	}
}
