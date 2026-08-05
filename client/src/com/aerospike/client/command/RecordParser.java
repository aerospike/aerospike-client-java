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
import com.aerospike.client.ExpressionTrace;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.SubCode;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.command.Command.OpResults;
import com.aerospike.client.util.ThreadLocalData;

public final class RecordParser {
	public final byte[] dataBuffer;
	public final int resultCode;
	public final int generation;
	public final int expiration;
	public final int fieldCount;
	public final int opCount;
	public int dataOffset;
	public long bytesIn;
	public String serverMessage;
	public int serverSubcode = SubCode.NONE;
	public ExpressionTrace expTrace;

	/**
	 * Build a failure exception that includes the server's extended-error
	 * detail when present. Route all non-OK throws through here so the
	 * detail is never silently dropped on special-case result codes such
	 * as FILTERED_OUT or KEY_NOT_FOUND_ERROR.
	 */
	public static AerospikeException toException(int resultCode, String serverMessage) {
		return toException(resultCode, serverMessage, SubCode.NONE, null);
	}

	/**
	 * Build a failure exception that carries the server's extended-error detail —
	 * the human-readable message and the numeric subcode — when present.
	 */
	public static AerospikeException toException(int resultCode, String serverMessage, int subcode) {
		return toException(resultCode, serverMessage, subcode, null);
	}

	/**
	 * Build a failure exception that carries the server's extended-error detail —
	 * the human-readable message, the numeric subcode, and (on expression
	 * build-failure paths at verbosity 3) the structured expression trace — when
	 * present.
	 */
	public static AerospikeException toException(int resultCode, String serverMessage, int subcode, ExpressionTrace expTrace) {
		AerospikeException ae = (serverMessage != null) ?
			new AerospikeException(resultCode, serverMessage) :
			new AerospikeException(resultCode);
		ae.setSubcode(subcode);
		ae.setExpressionTrace(expTrace);
		return ae;
	}

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
			buffer = ThreadLocalData.resizeBuffer(receiveSize);
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
	 * Error-detail-only parser. Wraps an existing response buffer so batch/multi
	 * commands can reuse the shared msgpack error-detail decoder ({@link
	 * #parseErrorDetails(int, int)}) over the command's own dataBuffer without a
	 * full message header. Header fields are left zeroed. For internal use only.
	 */
	public RecordParser(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
		this.resultCode = 0;
		this.generation = 0;
		this.expiration = 0;
		this.fieldCount = 0;
		this.opCount = 0;
		this.dataOffset = 0;
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
			parseFieldsError();
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
			else if (type == FieldType.ERROR_MESSAGE && size > 0) {
				serverMessage = parseErrorDetails(dataOffset, size);
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

	private void parseFieldsError() {
		for (int i = 0; i < fieldCount; i++) {
			int len = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int type = dataBuffer[dataOffset++];
			int size = len - 1;

			if (type == FieldType.ERROR_MESSAGE && size > 0) {
				serverMessage = parseErrorDetails(dataOffset, size);
			}
			dataOffset += size;
		}
	}

	/**
	 * Parse error detail msgpack map from server response.
	 * Map keys: 1 = subcode (uint), 2 = message (string).
	 * Returns formatted error message string. Also populates
	 * {@link #serverSubcode} and {@link #expTrace} as side effects.
	 * For internal use only.
	 */
	String parseErrorDetails(int offset, int size) {
		int end = offset + size;

		if (offset >= end) {
			return null;
		}

		// Read map header (fixmap, map16, map32).
		int b = dataBuffer[offset++] & 0xFF;
		int count;

		if ((b & 0xF0) == 0x80) {
			count = b & 0x0F;
		}
		else if (b == 0xDE && offset + 2 <= end) {
			count = Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF;
			offset += 2;
		}
		else if (b == 0xDF && offset + 4 <= end) {
			count = Buffer.bytesToInt(dataBuffer, offset);
			offset += 4;
		}
		else {
			return null;
		}

		if (count <= 0) {
			return null;
		}

		String message = null;
		long subcode = -1;

		for (int i = 0; i < count && offset < end; i++) {
			// Read key (positive fixint or uint8).
			int key;
			b = dataBuffer[offset++] & 0xFF;

			if (b <= 0x7F) {
				key = b;
			}
			else if (b == 0xCC && offset < end) {
				key = dataBuffer[offset++] & 0xFF;
			}
			else {
				break;
			}

			switch (key) {
			case 1: // AS_ERROR_DETAIL_KEY_SUBCODE
				subcode = unpackUint(offset, end);
				offset = skipMsgpackValue(offset, end);
				break;

			case 2: // AS_ERROR_DETAIL_KEY_MESSAGE
				int[] strResult = unpackStr(offset, end);
				if (strResult != null) {
					message = new String(dataBuffer, strResult[0], strResult[1], java.nio.charset.StandardCharsets.UTF_8);
					offset = strResult[0] + strResult[1];
				}
				else {
					offset = skipMsgpackValue(offset, end);
				}
				break;

			case ExpressionTrace.AS_ERROR_DETAIL_KEY_EXP_TRACE: // nested expression-trace map (verbosity 3)
				expTrace = parseExpTrace(offset, end);
				offset = skipMsgpackValue(offset, end);
				break;

			default:
				offset = skipMsgpackValue(offset, end);
				break;
			}
		}

		// Retain the numeric subcode as a first-class value. The server only
		// serializes subcodes >= 1 (AS_SUB_NONE = 0 is never sent), so a parsed
		// subcode always overrides the SubCode.NONE default.
		if (subcode >= 0) {
			serverSubcode = (int)subcode;
		}

		if (message != null && subcode >= 0) {
			return message + " (subcode=" + subcode + ")";
		}
		else if (subcode >= 0) {
			return "error subcode=" + subcode;
		}
		else if (message != null) {
			return message;
		}
		return null;
	}

	/**
	 * Parse the nested expression-trace map (top-level error-detail key 3, only sent
	 * at verbosity 3 on expression build-failure paths) into an {@link ExpressionTrace}.
	 * <p>
	 * Reuses the shared msgpack decoder. Treats every trace key as optional (never
	 * requires key 1 — build failures carry AS_SUB_NONE), skips unknown trace keys,
	 * tolerates the "..." path-truncation sentinel as an ordinary element, and never
	 * throws on a missing/truncated trace. {@code lang} absent is left as -1 and
	 * surfaces as msgpack via {@link ExpressionTrace#getLang()}. Returns null when the
	 * value is not a readable, non-empty map.
	 */
	private ExpressionTrace parseExpTrace(int offset, int end) {
		if (offset >= end) {
			return null;
		}

		// Read nested map header (fixmap, map16, map32).
		int b = dataBuffer[offset++] & 0xFF;
		int count;

		if ((b & 0xF0) == 0x80) {
			count = b & 0x0F;
		}
		else if (b == 0xDE && offset + 2 <= end) {
			count = Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF;
			offset += 2;
		}
		else if (b == 0xDF && offset + 4 <= end) {
			count = Buffer.bytesToInt(dataBuffer, offset);
			offset += 4;
		}
		else {
			return null;
		}

		if (count <= 0) {
			return null;
		}

		int phase = -1;
		int byteOffset = -1;
		String op = null;
		int depth = -1;
		String[] path = null;
		String snippet = null;
		int lang = -1;
		int aelOffset = -1;
		int aelSpan = -1;

		for (int i = 0; i < count && offset < end; i++) {
			// Read key (positive fixint or uint8).
			int key;
			b = dataBuffer[offset++] & 0xFF;

			if (b <= 0x7F) {
				key = b;
			}
			else if (b == 0xCC && offset < end) {
				key = dataBuffer[offset++] & 0xFF;
			}
			else {
				break;
			}

			switch (key) {
			case ExpressionTrace.KEY_PHASE:
				phase = (int)unpackUint(offset, end);
				break;
			case ExpressionTrace.KEY_BYTE_OFFSET:
				byteOffset = (int)unpackUint(offset, end);
				break;
			case ExpressionTrace.KEY_OP:
				op = unpackStrValue(offset, end);
				break;
			case ExpressionTrace.KEY_DEPTH:
				depth = (int)unpackUint(offset, end);
				break;
			case ExpressionTrace.KEY_PATH:
				path = unpackStrArray(offset, end);
				break;
			case ExpressionTrace.KEY_SNIPPET:
				snippet = unpackStrValue(offset, end);
				break;
			case ExpressionTrace.KEY_LANG:
				lang = (int)unpackUint(offset, end);
				break;
			case ExpressionTrace.KEY_AEL_OFFSET:
				aelOffset = (int)unpackUint(offset, end);
				break;
			case ExpressionTrace.KEY_AEL_SPAN:
				aelSpan = (int)unpackUint(offset, end);
				break;
			default:
				// Unknown / reserved trace key (outcome, ael_line, ael_col, etc.) — skip.
				break;
			}

			// Advance past the value regardless of whether the key was recognized.
			offset = skipMsgpackValue(offset, end);
		}

		return new ExpressionTrace(phase, byteOffset, op, depth, path, snippet, lang, aelOffset, aelSpan);
	}

	/**
	 * Unpack a msgpack string value to a Java String, or null if the value at the
	 * offset is not a readable string.
	 */
	private String unpackStrValue(int offset, int end) {
		int[] r = unpackStr(offset, end);
		if (r == null) {
			return null;
		}
		return new String(dataBuffer, r[0], r[1], java.nio.charset.StandardCharsets.UTF_8);
	}

	/**
	 * Unpack a msgpack array of strings (the expression-trace path). Preserves element
	 * order, keeps the "..." truncation sentinel as an ordinary element, and leaves a
	 * null slot for any element that is not a readable string. Returns null when the
	 * value is not a readable array.
	 */
	private String[] unpackStrArray(int offset, int end) {
		if (offset >= end) {
			return null;
		}

		int b = dataBuffer[offset++] & 0xFF;
		int len;

		if ((b & 0xF0) == 0x90) {
			len = b & 0x0F;
		}
		else if (b == 0xDC && offset + 2 <= end) {
			len = Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF;
			offset += 2;
		}
		else if (b == 0xDD && offset + 4 <= end) {
			len = Buffer.bytesToInt(dataBuffer, offset);
			offset += 4;
		}
		else {
			return null;
		}

		if (len < 0) {
			return null;
		}

		String[] result = new String[len];

		for (int i = 0; i < len && offset < end; i++) {
			result[i] = unpackStrValue(offset, end);
			offset = skipMsgpackValue(offset, end);
		}
		return result;
	}

	/**
	 * Unpack a msgpack unsigned integer value. Returns -1 on failure.
	 */
	private long unpackUint(int offset, int end) {
		if (offset >= end) {
			return -1;
		}

		int b = dataBuffer[offset] & 0xFF;

		if (b <= 0x7F) {
			return b;
		}
		else if (b == 0xCC && offset + 1 < end) {
			return dataBuffer[offset + 1] & 0xFF;
		}
		else if (b == 0xCD && offset + 2 < end) {
			return Buffer.bytesToShort(dataBuffer, offset + 1) & 0xFFFF;
		}
		else if (b == 0xCE && offset + 4 < end) {
			return Buffer.bytesToInt(dataBuffer, offset + 1) & 0xFFFFFFFFL;
		}
		else if (b == 0xCF && offset + 8 < end) {
			return Buffer.bytesToLong(dataBuffer, offset + 1);
		}
		return -1;
	}

	/**
	 * Unpack a msgpack string. Returns [offset, length] or null on failure.
	 */
	private int[] unpackStr(int offset, int end) {
		if (offset >= end) {
			return null;
		}

		int b = dataBuffer[offset++] & 0xFF;
		int len;

		if ((b & 0xE0) == 0xA0) {
			len = b & 0x1F;
		}
		else if (b == 0xD9 && offset < end) {
			len = dataBuffer[offset++] & 0xFF;
		}
		else if (b == 0xDA && offset + 1 < end) {
			len = Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF;
			offset += 2;
		}
		else if (b == 0xDB && offset + 3 < end) {
			len = Buffer.bytesToInt(dataBuffer, offset);
			offset += 4;
		}
		else {
			return null;
		}

		if (len < 0 || offset + len > end) {
			return null;
		}

		return new int[]{offset, len};
	}

	/**
	 * Skip a single msgpack value, returning the new offset.
	 */
	private int skipMsgpackValue(int offset, int end) {
		if (offset >= end) {
			return end;
		}

		int b = dataBuffer[offset++] & 0xFF;

		// Positive fixint / negative fixint
		if (b <= 0x7F || b >= 0xE0) {
			return offset;
		}
		// fixstr
		if ((b & 0xE0) == 0xA0) {
			return offset + (b & 0x1F);
		}
		// fixmap
		if ((b & 0xF0) == 0x80) {
			int count = (b & 0x0F) * 2;
			for (int i = 0; i < count && offset < end; i++) {
				offset = skipMsgpackValue(offset, end);
			}
			return offset;
		}
		// fixarray
		if ((b & 0xF0) == 0x90) {
			int count = b & 0x0F;
			for (int i = 0; i < count && offset < end; i++) {
				offset = skipMsgpackValue(offset, end);
			}
			return offset;
		}

		switch (b) {
		case 0xC0: // nil
		case 0xC2: // false
		case 0xC3: // true
			return offset;
		case 0xCC: // uint8
		case 0xD0: // int8
			return offset + 1;
		case 0xCD: // uint16
		case 0xD1: // int16
			return offset + 2;
		case 0xCE: // uint32
		case 0xD2: // int32
		case 0xCA: // float32
			return offset + 4;
		case 0xCF: // uint64
		case 0xD3: // int64
		case 0xCB: // float64
			return offset + 8;
		case 0xD9: // str8
		case 0xC4: // bin8
			if (offset < end) {
				return offset + 1 + (dataBuffer[offset] & 0xFF);
			}
			return end;
		case 0xDA: // str16
		case 0xC5: // bin16
			if (offset + 1 < end) {
				return offset + 2 + (Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF);
			}
			return end;
		case 0xDB: // str32
		case 0xC6: // bin32
			if (offset + 3 < end) {
				return offset + 4 + Buffer.bytesToInt(dataBuffer, offset);
			}
			return end;
		case 0xDC: // array16
		case 0xDE: { // map16
			if (offset + 1 >= end) {
				return end;
			}
			int count = (Buffer.bytesToShort(dataBuffer, offset) & 0xFFFF) * ((b == 0xDE) ? 2 : 1);
			offset += 2;
			for (int i = 0; i < count && offset < end; i++) {
				offset = skipMsgpackValue(offset, end);
			}
			return offset;
		}
		case 0xDD: // array32
		case 0xDF: { // map32
			if (offset + 3 >= end) {
				return end;
			}
			int count = Buffer.bytesToInt(dataBuffer, offset) * ((b == 0xDF) ? 2 : 1);
			offset += 4;
			for (int i = 0; i < count && offset < end; i++) {
				offset = skipMsgpackValue(offset, end);
			}
			return offset;
		}
		default:
			return end;
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
