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
package com.aerospike.client.command;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.util.Unpacker;
import com.aerospike.client.util.Utf8;

public final class Buffer {

	public static Value bytesToKeyValue(int type, byte[] buf, int offset, int len)
		throws AerospikeException {

		switch (type) {
		case ParticleType.STRING:
			return Value.get(Buffer.utf8ToString(buf, offset, len));

		case ParticleType.INTEGER:
			return bytesToLongValue(buf, offset, len);

		case ParticleType.DOUBLE:
			return new Value.DoubleValue(Buffer.bytesToDouble(buf, offset));

		case ParticleType.BLOB:
			return Value.get(Arrays.copyOfRange(buf, offset, offset+len));

		default:
			return null;
		}
	}

	public static Object bytesToParticle(int type, byte[] buf, int offset, int len)
		throws AerospikeException {

		switch (type) {
		case ParticleType.STRING:
			return Buffer.utf8ToString(buf, offset, len);

		case ParticleType.INTEGER:
			return Buffer.bytesToNumber(buf, offset, len);

		case ParticleType.BOOL:
			return Buffer.bytesToBool(buf, offset, len);

		case ParticleType.DOUBLE:
			return Buffer.bytesToDouble(buf, offset);

		case ParticleType.BLOB:
			return Arrays.copyOfRange(buf, offset, offset+len);

		case ParticleType.JBLOB:
			return Buffer.bytesToObject(buf, offset, len);

		case ParticleType.GEOJSON:
			return Buffer.bytesToGeoJSON(buf, offset, len);

		case ParticleType.HLL:
			return Buffer.bytesToHLL(buf, offset, len);

		case ParticleType.LIST:
			return Unpacker.unpackObjectList(buf, offset, len);

		case ParticleType.MAP:
			return Unpacker.unpackObjectMap(buf, offset, len);

		default:
			return null;
		}
	}

	/*
	private static Object parseList(byte[] buf, int offset, int len) throws AerospikeException {
		int limit = offset + len;
		int itemCount = Buffer.bytesToInt(buf, offset);
		offset += 4;
		ArrayList<Object> list = new ArrayList<Object>(itemCount);

		while (offset < limit) {
			int sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			int type = buf[offset];
			offset++;
			list.add(bytesToParticle(type, buf, offset, sz));
			offset += sz;
		}
		return list;
	}

	private static Object parseMap(byte[] buf, int offset, int len) throws AerospikeException {
		Object key;
		Object value;

		int limit = offset + len;
		int n_items = Buffer.bytesToInt(buf, offset);
		offset += 4;
		HashMap<Object, Object> map = new HashMap<Object, Object>(n_items);

		while (offset < limit) {
			// read out the key
			int sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			int type = buf[offset];
			offset++;

			key = bytesToParticle(type, buf, offset, len);
			offset += sz;

			// read out the value
			sz = Buffer.bytesToInt(buf, offset);
			offset += 4;
			type = buf[offset];
			offset++;

			value = bytesToParticle(type, buf, offset, len);
			offset += sz;

			map.put(key, value);
		}
		return map;
	}
	*/

	/**
	 * Estimate size of Utf8 encoded bytes without performing the actual encoding.
	 */
	public static int estimateSizeUtf8(String s) {
		if (s == null || s.length() == 0) {
			return 0;
		}
		return Utf8.encodedLength(s);
	}

	public static byte[] stringToUtf8(String s) {
		if (s == null || s.length() == 0) {
			return new byte[0];
		}
		int size = Utf8.encodedLength(s);
		byte[] bytes = new byte[size];
		stringToUtf8(s, bytes, 0);
		return bytes;
	}

	/**
	 * Convert input string to UTF-8, copies into buffer (at given offset).
	 * Returns number of bytes in the string.
	 *
	 * Java's internal UTF8 conversion is very, very slow.
	 * This is, rather amazingly, 8x faster than the to-string method.
	 * Returns the number of bytes this translated into.
	 */
	public static int stringToUtf8(String s, byte[] buf, int offset) {
		if (s == null) {
			return 0;
		}
		int length = s.length();
		int startOffset = offset;

		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c < 0x80) {
				buf[offset++] = (byte) c;
			}
			else if (c < 0x800) {
				buf[offset++] = (byte)(0xc0 | ((c >> 6)));
				buf[offset++] = (byte)(0x80 | (c & 0x3f));
			}
			else {
				// Encountered a different encoding other than 2-byte UTF8. Let java handle it.
				byte[] value = s.getBytes(StandardCharsets.UTF_8);
				System.arraycopy(value, 0, buf, startOffset, value.length);
				return value.length;
			}
		}
		return offset - startOffset;
	}

	public static String utf8ToString(byte[] buf, int offset, int length) {
		// A Thread local implementation does not help here, so
		// allocate character buffer each time.
		if (length == 0) {
			return "";
		}

		char[] charBuffer = new char[length];
		int charCount = 0;
		int limit = offset + length;
		int origoffset = offset;

		while (offset < limit ) {
			int b1 = buf[offset];

			if (b1 >= 0) {
				charBuffer[charCount++] = (char)b1;
				offset++;
			}
			else if ((b1 >> 5) == -2) {
				int b2 = buf[offset + 1];
				charBuffer[charCount++] = (char) (((b1 << 6) ^ b2) ^ 0x0f80);
				offset += 2;
			}
			else {
				// Encountered an UTF encoding which uses more than 2 bytes.
				// Use a native function to do the conversion.
				return new String(buf, origoffset, length, StandardCharsets.UTF_8);
			}
		}
		return new String(charBuffer, 0, charCount);
	}

	public static String utf8ToString(byte[] buf, int offset, int length, StringBuilder sb) {
		if (length == 0) {
			return "";
		}

		// This method is designed to accommodate multiple string conversions on the same
		// thread, but without the ThreadLocal overhead.  The StringBuilder instance is
		// created on the stack and passed in each method invocation.
		sb.setLength(0);
		int limit = offset + length;
		int origoffset = offset;

		while (offset < limit ) {
			if ((buf[offset] & 0x80) == 0) { // 1 byte
				char c = (char) buf[offset];
				sb.append(c);
				offset++;
			}
			else if ((buf[offset] & 0xE0) == 0xC0) { // 2 bytes
				char c =  (char) (((buf[offset] & 0x1f) << 6) | (buf[offset+1] & 0x3f));
				sb.append(c);
				offset += 2;
			}
			else {
				// Encountered an UTF encoding which uses more than 2 bytes.
				// Use a native function to do the conversion.
				return new String(buf, origoffset, length, StandardCharsets.UTF_8);
			}
		}
		return sb.toString();
	}

	/**
	 * Convert UTF8 numeric digits to an integer.  Negative integers are not supported.
	 *
	 * Input format: 1234
	 */
	public static int utf8DigitsToInt(byte[] buf, int begin, int end) {
		int val = 0;
		int mult = 1;

		for (int i = end - 1; i >= begin; i--) {
			val += ((int)buf[i] - 48) * mult;
			mult *= 10;
		}
		return val;
	}

	public static String bytesToHexString(byte[] buf) {
		if (buf == null || buf.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder(buf.length * 2);

		for (int i = 0; i < buf.length; i++) {
			sb.append(String.format("%02x", buf[i]));
		}
		return sb.toString();
	}

	public static String bytesToHexString(byte[] buf, int offset, int length) {
		StringBuilder sb = new StringBuilder(length * 2);

		for (int i = offset; i < length; i++) {
			sb.append(String.format("%02x", buf[i]));
		}
		return sb.toString();
	}

	public static Object bytesToObject(byte[] buf, int offset, int length) {
		if (length <= 0) {
			return null;
		}

		if (Value.DisableDeserializer) {
			throw new AerospikeException.Serialize("Object deserializer has been disabled");
		}

		try (ByteArrayInputStream bastream = new ByteArrayInputStream(buf, offset, length)) {
			try (ObjectInputStream oistream = new ObjectInputStream(bastream)) {
				return oistream.readObject();
			}
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static Value bytesToLongValue(byte[] buf, int offset, int len) {
		long val = 0;

		for (int i = 0; i < len; i++) {
			val <<= 8;
			val |= buf[offset+i] & 0xFF;
		}

		return new Value.LongValue(val);
	}

	public static Object bytesToGeoJSON(byte[] buf, int offset, int len) {
		// Ignore the flags for now
		int ncells = bytesToShort(buf, offset + 1);
		int hdrsz = 1 + 2 + (ncells * 8);
		return Value.getAsGeoJSON(Buffer.utf8ToString(buf, offset + hdrsz, len - hdrsz));
	}

	public static Object bytesToHLL(byte[] buf, int offset, int len) {
		byte[] bytes = Arrays.copyOfRange(buf, offset, offset+len);
		return Value.getAsHLL(bytes);
	}

	public static Object bytesToNumber(byte[] buf, int offset, int len) {
		// Server always returns 8 for integer length.
		if (len == 8) {
			return bytesToLong(buf, offset);
		}

		// Handle other lengths just in case server changes.
		if (len < 8) {
			// Handle variable length long.
			long val = 0;

			for (int i = 0; i < len; i++) {
				val <<= 8;
				val |= buf[offset+i] & 0xFF;
			}
			return val;
		}

		// Handle huge numbers.
		return bytesToBigInteger(buf, offset, len);
	}

	public static Object bytesToBigInteger(byte[] buf, int offset, int len) {
		boolean negative = false;

		if ((buf[offset] & 0x80) != 0) {
			negative = true;
			buf[offset] &= 0x7f;
		}
		byte[] bytes = new byte[len];
		System.arraycopy(buf, offset, bytes, 0, len);

		BigInteger big = new BigInteger(bytes);

		if (negative) {
			big = big.negate();
		}
		return big;
	}

	public static boolean bytesToBool(byte[] buf, int offset, int len) {
		if (len <= 0) {
			return false;
		}
		return (buf[offset] == 0)? false : true;
	}

	//-------------------------------------------------------
	// 64 bit double conversions.
	//-------------------------------------------------------

	public static double bytesToDouble(byte[] buf, int offset) {
		return Double.longBitsToDouble(bytesToLong(buf, offset));
	}

	public static void doubleToBytes(double v, byte[] buf, int offset) {
		Buffer.longToBytes(Double.doubleToLongBits(v), buf, offset);
	}

	//-------------------------------------------------------
	// 64 bit number conversions.
	//-------------------------------------------------------

	/**
	 * Convert long to big endian signed or unsigned 64 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void longToBytes(long v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 56);
		buf[offset++] = (byte)(v >>> 48);
		buf[offset++] = (byte)(v >>> 40);
		buf[offset++] = (byte)(v >>> 32);
		buf[offset++] = (byte)(v >>> 24);
		buf[offset++] = (byte)(v >>> 16);
		buf[offset++] = (byte)(v >>>  8);
		buf[offset]   = (byte)(v >>>  0);
	}

	/**
	 * Convert long to little endian signed or unsigned 64 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void longToLittleBytes(long v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 0);
		buf[offset++] = (byte)(v >>> 8);
		buf[offset++] = (byte)(v >>> 16);
		buf[offset++] = (byte)(v >>> 24);
		buf[offset++] = (byte)(v >>> 32);
		buf[offset++] = (byte)(v >>> 40);
		buf[offset++] = (byte)(v >>> 48);
		buf[offset]   = (byte)(v >>> 56);
	}

	/**
	 * Convert big endian signed 64 bits to long.
	 */
	public static long bytesToLong(byte[] buf, int offset) {
		return (
			((long)(buf[offset]   & 0xFF) << 56) |
			((long)(buf[offset+1] & 0xFF) << 48) |
			((long)(buf[offset+2] & 0xFF) << 40) |
			((long)(buf[offset+3] & 0xFF) << 32) |
			((long)(buf[offset+4] & 0xFF) << 24) |
			((long)(buf[offset+5] & 0xFF) << 16) |
			((long)(buf[offset+6] & 0xFF) << 8) |
			((long)(buf[offset+7] & 0xFF) << 0)
			);
	}

	/**
	 * Convert little endian signed 64 bits to long.
	 */
	public static long littleBytesToLong(byte[] buf, int offset) {
		return (
			((long)(buf[offset]   & 0xFF) << 0) |
			((long)(buf[offset+1] & 0xFF) << 8) |
			((long)(buf[offset+2] & 0xFF) << 16) |
			((long)(buf[offset+3] & 0xFF) << 24) |
			((long)(buf[offset+4] & 0xFF) << 32) |
			((long)(buf[offset+5] & 0xFF) << 40) |
			((long)(buf[offset+6] & 0xFF) << 48) |
			((long)(buf[offset+7] & 0xFF) << 56)
			);
	}

	//-------------------------------------------------------
	// 32 bit number conversions.
	//-------------------------------------------------------

	/**
	 * Convert int to big endian signed or unsigned 32 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void intToBytes(int v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 24);
		buf[offset++] = (byte)(v >>> 16);
		buf[offset++] = (byte)(v >>> 8);
		buf[offset]   = (byte)(v >>> 0);
	}

	/**
	 * Convert int to little endian signed or unsigned 32 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void intToLittleBytes(int v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 0);
		buf[offset++] = (byte)(v >>> 8);
		buf[offset++] = (byte)(v >>> 16);
		buf[offset]   = (byte)(v >>> 24);
	}

	/**
	 * Convert big endian signed 32 bits to int.
	 */
	public static int bytesToInt(byte[] buf, int offset) {
		return (
			((buf[offset]   & 0xFF) << 24) |
			((buf[offset+1] & 0xFF) << 16) |
			((buf[offset+2] & 0xFF) << 8) |
			((buf[offset+3] & 0xFF) << 0)
			);
	}

	/**
	 * Convert little endian signed 32 bits to int.
	 */
	public static int littleBytesToInt(byte[] buf, int offset) {
		return (
			((buf[offset]   & 0xFF) << 0) |
			((buf[offset+1] & 0xFF) << 8) |
			((buf[offset+2] & 0xFF) << 16) |
			((buf[offset+3] & 0xFF) << 24)
			);
	}

	/**
	 * Convert big endian unsigned 32 bits to long.
	 */
	public static long bigUnsigned32ToLong(byte[] buf, int offset) {
		return (
			((long)(buf[offset]   & 0xFF) << 24) |
			((long)(buf[offset+1] & 0xFF) << 16) |
			((long)(buf[offset+2] & 0xFF) << 8) |
			((long)(buf[offset+3] & 0xFF) << 0)
			);
	}

	//-------------------------------------------------------
	// 16 bit number conversions.
	//-------------------------------------------------------

	/**
	 * Convert int to big endian signed or unsigned 16 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void shortToBytes(int v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 8);
		buf[offset]   = (byte)(v >>> 0);
	}

	/**
	 * Convert int to little endian signed or unsigned 16 bits.
	 * The bit pattern will be the same regardless of sign.
	 */
	public static void shortToLittleBytes(int v, byte[] buf, int offset) {
		buf[offset++] = (byte)(v >>> 0);
		buf[offset]   = (byte)(v >>> 8);
	}

	/**
	 * Convert big endian unsigned 16 bits to int.
	 */
	public static int bytesToShort(byte[] buf, int offset) {
		return (
			((buf[offset]   & 0xFF) << 8) |
			((buf[offset+1] & 0xFF) << 0)
			);
	}

	/**
	 * Convert little endian unsigned 16 bits to int.
	 */
	public static int littleBytesToShort(byte[] buf, int offset) {
		return (
			((buf[offset]   & 0xFF) << 0) |
			((buf[offset+1] & 0xFF) << 8)
			);
	}

	/**
	 * Convert big endian signed 16 bits to short.
	 */
	public static short bigSigned16ToShort(byte[] buf, int offset) {
		return (short)(
			((buf[offset]   & 0xFF) << 8) |
			((buf[offset+1] & 0xFF) << 0)
			);
	}

	//-------------------------------------------------------
	// Variable byte number conversions.
	//-------------------------------------------------------

	/**
	 *	Encode an integer in variable 7-bit format.
	 *	The high bit indicates if more bytes are used.
	 *  Return byte size of integer.
	 */
	public static int intToVarBytes(int v, byte[] buf, int offset) {
		int i = offset;

		while (i < buf.length && v >= 0x80) {
			buf[i++] = (byte)(v | 0x80);
			v >>>= 7;
		}

		if (i < buf.length) {
			buf[i++] = (byte)v;
			return i - offset;
		}
		return 0;
	}

	/**
	 *	Decode an integer in variable 7-bit format.
	 *	The high bit indicates if more bytes are used.
	 *  Return value and byte size in array.
	 */
	public static int[] varBytesToInt(byte[] buf, int offset) {
		int i = offset;
		int val = 0;
		int shift = 0;
		byte b;

		do {
			b = buf[i++];
			val |= (b & 0x7F) << shift;
			shift += 7;
		} while ((b & 0x80) != 0);

		return new int[] {val, i - offset};
	}
}
