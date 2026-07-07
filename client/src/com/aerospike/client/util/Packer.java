/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.util;

import static com.aerospike.client.Value.MapValue.getMapOrder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

/**
 * Serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
public final class Packer {

	public static byte[] pack(Value[] val) {
		try {
			Packer packer = new Packer();
			packer.packValueArray(val);
			packer.createBuffer();
			packer.packValueArray(val);
			return packer.getBuffer();
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] pack(List<?> val) {
		try {
			Packer packer = new Packer();
			packer.packList(val);
			packer.createBuffer();
			packer.packList(val);
			return packer.getBuffer();
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] pack(Map<?,?> val, MapOrder order) {
		try {
			Packer packer = new Packer();
			packer.packMap(val, order);
			packer.createBuffer();
			packer.packMap(val, order);
			return packer.getBuffer();
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] pack(List<? extends Entry<?,?>> val, MapOrder order) {
		try {
			Packer packer = new Packer();
			packer.packMap(val, order);
			packer.createBuffer();
			packer.packMap(val, order);
			return packer.getBuffer();
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	private byte[] buffer;
	private int offset;
	private boolean sortMaps;

	public Packer() {
		// Default to null buffer in estimate buffer size mode.
	}

	/**
	 * Pack unordered maps at any depth with entries sorted by key in the server's
	 * canonical msgpack order, without adding an order flag ext header. Servers
	 * that include AER-6930 (8.1.2.3+) require map value literals in expressions
	 * to be in canonical form. Default is false.
	 */
	public void sortMaps(boolean sortMaps) {
		this.sortMaps = sortMaps;
	}

	public void packValueArray(Value[] values) {
		packArrayBegin(values.length);
		for (Value value : values) {
			value.pack(this);
		}
	}

	public void packValueList(List<Value> list) {
		packArrayBegin(list.size());
		for (Value value : list) {
			value.pack(this);
		}
	}

	public void packList(List<?> list) {
		packArrayBegin(list.size());
		for (Object obj : list) {
			packObject(obj);
		}
	}

	public void packArrayBegin(int size) {
		if (size < 16) {
			packByte(0x90 | size);
		}
		else if (size < 65536) {
			packShort(0xdc, size);
		}
		else {
			packInt(0xdd, size);
		}
	}

	public void packValueMap(Map<Value,Value> map) {
		MapOrder order = getMapOrder(map);

		if (sortMaps && order == MapOrder.UNORDERED && map.size() > 1) {
			packMapCanonical(map.entrySet(), map.size());
			return;
		}
		packMapBegin(map.size(), order);

		for (Entry<Value,Value> entry : map.entrySet()) {
			entry.getKey().pack(this);
			entry.getValue().pack(this);
		}
	}

	public void packMap(Map<?,?> map) {
		MapOrder order = getMapOrder(map);
		packMap(map, order);
	}

	public void packMap(Map<?,?> map, MapOrder order) {
		if (sortMaps && order == MapOrder.UNORDERED && map.size() > 1) {
			packMapCanonical(map.entrySet(), map.size());
			return;
		}
		packMapBegin(map.size(), order);

		for (Entry<?,?> entry : map.entrySet()) {
			packObject(entry.getKey());
			packObject(entry.getValue());
		}
	}

	public void packMap(List<? extends Entry<?,?>> list, MapOrder order) {
		if (sortMaps && order == MapOrder.UNORDERED && list.size() > 1) {
			packMapCanonical(list, list.size());
			return;
		}
		packMapBegin(list.size(), order);

		for (Entry<?,?> entry : list) {
			packObject(entry.getKey());
			packObject(entry.getValue());
		}
	}

	private void packMapCanonical(Iterable<? extends Entry<?,?>> entries, int size) {
		final byte[][] keys = new byte[size][];
		Object[] values = new Object[size];
		Integer[] ranks = new Integer[size];
		int i = 0;

		for (Entry<?,?> entry : entries) {
			Packer packer = new Packer();
			packer.sortMaps = true;
			packer.packObject(entry.getKey());
			packer.createBuffer();
			packer.packObject(entry.getKey());
			keys[i] = packer.getBuffer();
			values[i] = entry.getValue();
			ranks[i] = i;
			i++;
		}

		Arrays.sort(ranks, (a, b) -> CanonicalCompare.compare(keys[a], keys[b]));

		for (i = 1; i < size; i++) {
			if (CanonicalCompare.compare(keys[ranks[i - 1]], keys[ranks[i]]) == 0) {
				throw new AerospikeException(ResultCode.PARAMETER_ERROR,
					"Map keys pack to duplicate msgpack keys in expression map literal");
			}
		}

		packMapBegin(size);

		for (i = 0; i < size; i++) {
			byte[] key = keys[ranks[i]];
			packByteArray(key, 0, key.length);
			packObject(values[ranks[i]]);
		}
	}

	/**
	 * Compare packed msgpack elements using the same ordering as the server's
	 * msgpack_cmp (cf/src/msgpack_in.c).
	 */
	private static final class CanonicalCompare {
		// Type ranks from the server's msgpack_type enum (cf/include/msgpack_in.h).
		private static final int TYPE_NIL = 1;
		private static final int TYPE_FALSE = 2;
		private static final int TYPE_TRUE = 3;
		private static final int TYPE_NEGINT = 4;
		private static final int TYPE_INT = 5;
		private static final int TYPE_STRING = 6;
		private static final int TYPE_LIST = 7;
		private static final int TYPE_MAP = 8;
		private static final int TYPE_BYTES = 9;
		private static final int TYPE_DOUBLE = 10;
		private static final int TYPE_GEOJSON = 11;
		private static final int TYPE_EXT = 12;
		private static final int TYPE_WILDCARD = 13;
		private static final int TYPE_INF = 14;

		private final byte[] buf;
		private int offset;

		// State of most recently parsed element.
		private int type;
		private long iNum;
		private double dNum;
		private int dataOffset;
		private int dataLen;
		private int count;

		private CanonicalCompare(byte[] buf) {
			this.buf = buf;
		}

		private static int compare(byte[] b0, byte[] b1) {
			return compareElement(new CanonicalCompare(b0), new CanonicalCompare(b1));
		}

		private static int compareElement(CanonicalCompare c0, CanonicalCompare c1) {
			c0.parse();
			c1.parse();

			if (c0.type == TYPE_WILDCARD || c1.type == TYPE_WILDCARD) {
				c0.skipParsed();
				c1.skipParsed();
				return 0;
			}

			if (c0.type != c1.type) {
				return Integer.compare(c0.type, c1.type);
			}

			switch (c0.type) {
			case TYPE_NEGINT:
			case TYPE_INT:
				return Long.compareUnsigned(c0.iNum, c1.iNum);

			case TYPE_STRING:
			case TYPE_BYTES:
			case TYPE_GEOJSON:
			case TYPE_EXT: {
				int len = Math.min(c0.dataLen, c1.dataLen);

				for (int i = 0; i < len; i++) {
					int cmp = (c0.buf[c0.dataOffset + i] & 0xff) - (c1.buf[c1.dataOffset + i] & 0xff);

					if (cmp != 0) {
						return cmp;
					}
				}
				return Integer.compare(c0.dataLen, c1.dataLen);
			}

			case TYPE_LIST: {
				int n0 = c0.count;
				int n1 = c1.count;
				int n = Math.min(n0, n1);

				for (int i = 0; i < n; i++) {
					int cmp = compareElement(c0, c1);

					if (cmp != 0) {
						return cmp;
					}
				}
				return Integer.compare(n0, n1);
			}

			case TYPE_MAP: {
				if (c0.count != c1.count) {
					return Integer.compare(c0.count, c1.count);
				}

				int n = c0.count * 2;

				for (int i = 0; i < n; i++) {
					int cmp = compareElement(c0, c1);

					if (cmp != 0) {
						return cmp;
					}
				}
				return 0;
			}

			case TYPE_DOUBLE:
				// Match C comparison semantics. Do not use Double.compare which
				// orders -0.0 before 0.0 and NaN above all values.
				if (c0.dNum > c1.dNum) {
					return 1;
				}
				if (c0.dNum < c1.dNum) {
					return -1;
				}
				return 0;

			default:
				// NIL, FALSE, TRUE and INF have no payload.
				return 0;
			}
		}

		private void skipParsed() {
			int n;

			switch (type) {
			case TYPE_LIST:
				n = count;
				break;
			case TYPE_MAP:
				n = count * 2;
				break;
			default:
				return;
			}

			for (int i = 0; i < n; i++) {
				parse();
				skipParsed();
			}
		}

		private void parse() {
			int b = buf[offset++] & 0xff;

			switch (b) {
			case 0xc0:
				type = TYPE_NIL;
				return;
			case 0xc2:
				type = TYPE_FALSE;
				return;
			case 0xc3:
				type = TYPE_TRUE;
				return;

			case 0xcc:
				iNum = buf[offset++] & 0xff;
				type = TYPE_INT;
				return;
			case 0xcd:
				iNum = readUint(2);
				type = TYPE_INT;
				return;
			case 0xce:
				iNum = readUint(4);
				type = TYPE_INT;
				return;
			case 0xcf:
				iNum = readUint(8);
				type = TYPE_INT;
				return;

			case 0xd0:
				iNum = buf[offset++];
				type = (iNum < 0) ? TYPE_NEGINT : TYPE_INT;
				return;
			case 0xd1:
				iNum = readSint(2);
				type = (iNum < 0) ? TYPE_NEGINT : TYPE_INT;
				return;
			case 0xd2:
				iNum = readSint(4);
				type = (iNum < 0) ? TYPE_NEGINT : TYPE_INT;
				return;
			case 0xd3:
				iNum = readSint(8);
				type = (iNum < 0) ? TYPE_NEGINT : TYPE_INT;
				return;

			case 0xca:
				dNum = Float.intBitsToFloat((int)readUint(4));
				type = TYPE_DOUBLE;
				return;
			case 0xcb:
				dNum = Double.longBitsToDouble(readUint(8));
				type = TYPE_DOUBLE;
				return;

			case 0xc4:
			case 0xd9:
				setRaw(buf[offset++] & 0xff);
				return;
			case 0xc5:
			case 0xda:
				setRaw((int)readUint(2));
				return;
			case 0xc6:
			case 0xdb:
				setRaw((int)readUint(4));
				return;

			case 0xdc:
				count = (int)readUint(2);
				type = TYPE_LIST;
				return;
			case 0xdd:
				count = (int)readUint(4);
				type = TYPE_LIST;
				return;
			case 0xde:
				count = (int)readUint(2);
				type = TYPE_MAP;
				return;
			case 0xdf:
				count = (int)readUint(4);
				type = TYPE_MAP;
				return;

			case 0xd4: {
				int extType = buf[offset++] & 0xff;

				if (extType == 0xff) {
					int val = buf[offset] & 0xff;

					if (val == 0x00) {
						offset++;
						type = TYPE_WILDCARD;
						return;
					}

					if (val == 0x01) {
						offset++;
						type = TYPE_INF;
						return;
					}
				}
				dataOffset = offset++;
				dataLen = 1;
				type = TYPE_EXT;
				return;
			}
			case 0xd5:
				setExt(2);
				return;
			case 0xd6:
				setExt(4);
				return;
			case 0xd7:
				setExt(8);
				return;
			case 0xd8:
				setExt(16);
				return;
			case 0xc7: {
				int len = buf[offset++] & 0xff;
				int extType = buf[offset++] & 0xff;

				if (extType == 0xff && len == 1) {
					int val = buf[offset] & 0xff;

					if (val == 0x00) {
						offset++;
						type = TYPE_WILDCARD;
						return;
					}

					if (val == 0x01) {
						offset++;
						type = TYPE_INF;
						return;
					}
				}
				dataOffset = offset;
				dataLen = len;
				offset += len;
				type = TYPE_EXT;
				return;
			}
			case 0xc8:
				setExt((int)readUint(2));
				return;
			case 0xc9:
				setExt((int)readUint(4));
				return;

			default:
				if (b < 0x80) {
					iNum = b;
					type = TYPE_INT;
					return;
				}

				if (b >= 0xe0) {
					iNum = (byte)b;
					type = TYPE_NEGINT;
					return;
				}

				if ((b & 0xe0) == 0xa0) {
					setRaw(b & 0x1f);
					return;
				}

				if ((b & 0xf0) == 0x80) {
					count = b & 0x0f;
					type = TYPE_MAP;
					return;
				}

				if ((b & 0xf0) == 0x90) {
					count = b & 0x0f;
					type = TYPE_LIST;
					return;
				}
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Unexpected msgpack header: " + b);
			}
		}

		private void setRaw(int len) {
			dataOffset = offset;
			dataLen = len;
			offset += len;

			if (len == 0) {
				type = TYPE_BYTES;
				return;
			}

			switch (buf[dataOffset] & 0xff) {
			case ParticleType.STRING:
				type = TYPE_STRING;
				return;
			case ParticleType.GEOJSON:
				type = TYPE_GEOJSON;
				return;
			default:
				type = TYPE_BYTES;
				return;
			}
		}

		private void setExt(int len) {
			// The ext type byte is not compared by the server.
			offset++;
			dataOffset = offset;
			dataLen = len;
			offset += len;
			type = TYPE_EXT;
		}

		private long readUint(int size) {
			long val = 0;

			for (int i = 0; i < size; i++) {
				val = (val << 8) | (buf[offset++] & 0xff);
			}
			return val;
		}

		private long readSint(int size) {
			long val = buf[offset++];

			for (int i = 1; i < size; i++) {
				val = (val << 8) | (buf[offset++] & 0xff);
			}
			return val;
		}
	}

	public void packMapBegin(int size, MapOrder order) {
		if (order == MapOrder.UNORDERED) {
			packMapBegin(size);
		}
		else {
			// Map is sorted.
			packMapBegin(size + 1);
			packByte(0xc7);
			packByte(0);
			packByte(order.attributes);
			packByte(0xc0);
		}
	}

	public void packMapBegin(int size) {
		if (size < 16) {
			packByte(0x80 | size);
		}
		else if (size < 65536) {
			packShort(0xde, size);
		}
		else {
			packInt(0xdf, size);
		}
	}

	public void packBytes(byte[] b) {
		packByteArrayBegin(b.length);
		packByteArray(b, 0, b.length);
	}

	public void packParticleBytes(byte[] b) {
		packByteArrayBegin(b.length + 1);
		packByte(ParticleType.BLOB);
		packByteArray(b, 0, b.length);
	}

	public void packParticleBytes(byte[] b, int type) {
		packByteArrayBegin(b.length + 1);
		packByte(type);
		packByteArray(b, 0, b.length);
	}

	public void packParticleBytes(byte[] b, int offset, int length) {
		packByteArrayBegin(length + 1);
		packByte(ParticleType.BLOB);
		packByteArray(b, offset, length);
	}

	public void packGeoJSON(String val) {
		byte[] buffer = Buffer.stringToUtf8(val);
		packByteArrayBegin(buffer.length + 1);
		packByte(ParticleType.GEOJSON);
		packByteArray(buffer, 0, buffer.length);
	}

	private void packByteArrayBegin(int size) {
		// Use string header codes for byte arrays.
		packStringBegin(size);
		/*
		if (size < 256) {
			packByte(0xc4, size);
		}
		else if (size < 65536) {
			packShort(0xc5, size);
		}
		else {
			packInt(0xc6, size);
		}
		*/
	}

	public void packObject(Object obj) {
		if (obj == null) {
			packNil();
			return;
		}

		if (obj instanceof Value) {
			Value value = (Value)obj;
			value.pack(this);
			return;
		}

		if (obj instanceof byte[]) {
			packParticleBytes((byte[])obj);
			return;
		}

		if (obj instanceof String) {
			packParticleString((String)obj);
			return;
		}

		if (obj instanceof Integer) {
			packInt((Integer)obj);
			return;
		}

		if (obj instanceof Long) {
			packLong((Long)obj);
			return;
		}

		if (obj instanceof List<?>) {
			packList((List<?>)obj);
			return;
		}

		if (obj instanceof Map<?,?>) {
			packMap((Map<?,?>)obj);
			return;
		}

		if (obj instanceof Double) {
			packDouble((Double)obj);
			return;
		}

		if (obj instanceof Float) {
			packFloat((Float)obj);
			return;
		}

		if (obj instanceof Short) {
			packInt((Short)obj);
			return;
		}

		if (obj instanceof Boolean) {
			packBoolean((Boolean)obj);
			return;
		}

		if (obj instanceof Byte) {
			packInt(((Byte)obj) & 0xff);
			return;
		}

		if (obj instanceof Character) {
			packInt(((Character)obj).charValue());
			return;
		}

		if (obj instanceof Enum) {
			packString(obj.toString());
			return;
		}

		if (obj instanceof UUID) {
			packString(obj.toString());
			return;
		}

		if (obj instanceof ByteBuffer) {
			packByteBuffer((ByteBuffer) obj);
			return;
		}

		throw new AerospikeException("Unsupported type: " + obj.getClass().getName());
	}

	public void packByteBuffer(ByteBuffer bb) {
		byte[] b = bb.array();
		packParticleBytes(b);
	}

	public void packLong(long val) {
		if (val >= 0L) {
			if (val < 128L) {
				packByte((int)val);
				return;
			}

			if (val < 256L) {
				packByte(0xcc, (int)val);
				return;
			}

			if (val < 65536L) {
				packShort(0xcd, (int)val);
				return;
			}

			if (val < 4294967296L) {
				packInt(0xce, (int)val);
				return;
			}
			packLong(0xcf, val);
		}
		else {
			if (val >= -32) {
				packByte(0xe0 | ((int)val + 32));
				return;
			}

			if (val >= Byte.MIN_VALUE) {
				packByte(0xd0, (int)val);
				return;
			}

			if (val >= Short.MIN_VALUE) {
				packShort(0xd1, (int)val);
				return;
			}

			if (val >= Integer.MIN_VALUE) {
				packInt(0xd2, (int)val);
				return;
			}
			packLong(0xd3, val);
		}
	}

	public void packInt(int val) {
		if (val >= 0) {
			if (val < 128) {
				packByte(val);
				return;
			}

			if (val < 256) {
				packByte(0xcc, val);
				return;
			}

			if (val < 65536) {
				packShort(0xcd, val);
				return;
			}
			packInt(0xce, val);
		}
		else {
			if (val >= -32) {
				packByte(0xe0 | (val + 32));
				return;
			}

			if (val >= Byte.MIN_VALUE) {
				packByte(0xd0, val);
				return;
			}

			if (val >= Short.MIN_VALUE) {
				packShort(0xd1, val);
				return;
			}
			packInt(0xd2, val);
		}
	}

	public void packString(String val) {
		int size = Buffer.estimateSizeUtf8(val);
		packStringBegin(size);

		if (buffer == null) {
			offset += size;
			return;
		}
		offset += Buffer.stringToUtf8(val, buffer, offset);
	}

	public void packParticleString(String val) {
		int size = Buffer.estimateSizeUtf8(val) + 1;
		packStringBegin(size);

		if (buffer == null) {
			offset += size;
			return;
		}
		buffer[offset++] = (byte)ParticleType.STRING;
		offset += Buffer.stringToUtf8(val, buffer, offset);
	}

	private void packStringBegin(int size) {
		if (size < 32) {
			packByte(0xa0 | size);
		}
		else if (size < 256) {
			packByte(0xd9, size);
		}
		else if (size < 65536) {
			packShort(0xda, size);
		}
		else {
			packInt(0xdb, size);
		}
	}

	public void packByteArray(byte[] src, int srcOffset, int srcLength) {
		if (buffer == null) {
			offset += srcLength;
			return;
		}
 		System.arraycopy(src, srcOffset, buffer, offset, srcLength);
		offset += srcLength;
	}

	public void packDouble(double val) {
		if (buffer == null) {
			offset += 9;
			return;
		}
		buffer[offset++] = (byte)0xcb;
		Buffer.longToBytes(Double.doubleToLongBits(val), buffer, offset);
		offset += 8;
	}

	public void packFloat(float val) {
		if (buffer == null) {
			offset += 5;
			return;
		}
		buffer[offset++] = (byte)0xca;
		Buffer.intToBytes(Float.floatToIntBits(val), buffer, offset);
		offset += 4;
	}

	private void packLong(int type, long val) {
		if (buffer == null) {
			offset += 9;
			return;
		}
		buffer[offset++] = (byte)type;
		Buffer.longToBytes(val, buffer, offset);
		offset += 8;
	}

	private void packInt(int type, int val) {
		if (buffer == null) {
			offset += 5;
			return;
		}
		buffer[offset++] = (byte)type;
		Buffer.intToBytes(val, buffer, offset);
		offset += 4;
	}

	private void packShort(int type, int val) {
		if (buffer == null) {
			offset += 3;
			return;
		}
		buffer[offset++] = (byte)type;
		Buffer.shortToBytes(val, buffer, offset);
		offset += 2;
	}

	private void packByte(int type, int val) {
		if (buffer == null) {
			offset += 2;
			return;
		}
		buffer[offset++] = (byte)type;
		buffer[offset++] = (byte)val;
	}

	public void packBoolean(boolean val) {
		if (buffer == null) {
			offset++;
			return;
		}

		if (val) {
			buffer[offset++] = (byte)0xc3;
		}
		else {
			buffer[offset++] = (byte)0xc2;
		}
	}

	public void packNil() {
		if (buffer == null) {
			offset++;
			return;
		}
		buffer[offset++] = (byte)0xc0;
	}

	public void packInfinity() {
		if (buffer == null) {
			offset += 3;
			return;
		}
		buffer[offset++] = (byte)0xd4;
		buffer[offset++] = (byte)0xff;
		buffer[offset++] = (byte)0x01;
	}

	public void packWildcard() {
		if (buffer == null) {
			offset += 3;
			return;
		}
		buffer[offset++] = (byte)0xd4;
		buffer[offset++] = (byte)0xff;
		buffer[offset++] = (byte)0x00;
	}

	public void packByte(int val) {
		if (buffer == null) {
			offset++;
			return;
		}
		buffer[offset++] = (byte)val;
	}

	public void createBuffer() {
		buffer = new byte[offset];
		offset = 0;
	}

	public byte[] getBuffer() {
		return buffer;
	}
}
