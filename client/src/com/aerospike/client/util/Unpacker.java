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
package com.aerospike.client.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

/**
 * De-serialize collection objects using MessagePack format specification:
 *
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
public abstract class Unpacker<T> {

	private final byte[] buffer;
	private int offset;
	private final int length;

	public Unpacker(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;
	}

	public final T unpackList() throws AerospikeException {
		if (length <= 0) {
			return getList(new ArrayList<T>(0));
		}

		try {
			int type = buffer[offset++] & 0xff;
			int count;

			if ((type & 0xf0) == 0x90) {
				count = type & 0x0f;
			}
			else if (type == 0xdc) {
				count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
			}
			else if (type == 0xdd) {
				count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
			}
			else {
				return getList(new ArrayList<T>(0));
			}
			return unpackList(count);
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	private T unpackList(int count) throws IOException, ClassNotFoundException {
		if (count <= 0) {
			return getList(new ArrayList<T>(0));
		}

		// Extract first object.
		int mark = offset;
		int size = count;
		T val = unpackObject();

		if (val == null) {
			// Determine if null value is because of an extension type.
			int type = buffer[mark] & 0xff;

			if (type != 0xc0) {  // not nil type
				// Ignore extension type.
				size--;
			}
		}

		ArrayList<T> out = new ArrayList<T>(size);

		if (size == count) {
			out.add(val);
		}

		for (int i = 1; i < count; i++) {
			out.add(unpackObject());
		}
		return getList(out);
	}

	public final T unpackMap() throws AerospikeException {
		if (length <= 0) {
			return getMap(new HashMap<T,T>(0));
		}

		try {
			int type = buffer[offset++] & 0xff;
			int count;

			if ((type & 0xf0) == 0x80) {
				count = type & 0x0f;
			}
			else if (type == 0xde) {
				count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
			}
			else if (type == 0xdf) {
				count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
			}
			else {
				return getMap(new HashMap<T,T>(0));
			}
			return unpackMap(count);
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	private T unpackMap(int count) throws IOException, ClassNotFoundException {
		if (count <= 0) {
			return getMap(new HashMap<T,T>(0));
		}

		// Peek at buffer to determine map type, but do not advance.
		int type = buffer[offset] & 0xff;

		// Check for extension that the server uses.
		if (type == 0xc7) {
			int extensionType = buffer[offset + 1] & 0xff;

			if (extensionType == 0) {
				int mapBits = buffer[offset + 2] & 0xff;

				// Extension is a map type.  Determine which one.
				if ((mapBits & 0x08) != 0 && !Value.ReturnMapForKeyValue) {
					// Index/rank range result where order needs to be preserved.
					return unpackMapAsList(count);
				}
				else if ((mapBits & 0x01) != 0) {
					// Sorted map
					return unpackTreeMap(count);
				}
			}
		}
		return unpackHashMap(count);
	}

	private T unpackHashMap(int count) throws IOException, ClassNotFoundException {
		HashMap<T,T> map = new HashMap<T,T>(count);

		for (int i = 0; i < count; i++) {
			T key = unpackObject();
			T val = unpackObject();

			if (key != null) {
				map.put(key, val);
			}
		}
		return getMap(map);
	}

	@SuppressWarnings("unchecked")
	private T unpackTreeMap(int count) throws IOException, ClassNotFoundException {
		TreeMap<T,T> map = new TreeMap<T,T>();

		for (int i = 0; i < count; i++) {
			T key = unpackObject();
			T val = unpackObject();

			if (key != null) {
				if (key instanceof byte[]) {
					// TreeMap does not support byte[] keys,
					// so wrap byte[] in a ByteBuffer.
					key = (T)ByteBuffer.wrap((byte[])key);
				}
				map.put(key, val);
			}
		}
		return getMap(map);
	}

	@SuppressWarnings("unchecked")
	private T unpackMapAsList(int count) throws IOException, ClassNotFoundException {
		// Index/rank range result where order needs to be preserved.
		// Store in List<Entry<?,?> to preserve order.
		// The first entry is going to be null (ignored), so use "count - 1" size.
		List<Entry<T,T>> list = new ArrayList<Entry<T,T>>(count - 1);

		for (int i = 0; i < count; i++) {
			T key = unpackObject();
			T val = unpackObject();

			if (key != null) {
				list.add(new AbstractMap.SimpleEntry<T,T>(key, val));
			}
		}
		return getList((List<T>)list);
	}

	private T unpackBlob(int count) throws IOException, ClassNotFoundException {
		int type = buffer[offset++] & 0xff;
		count--;
		T val;

		switch (type) {
		case ParticleType.STRING:
			val = getString(Buffer.utf8ToString(buffer, offset, count));
			break;

		case ParticleType.JBLOB:
			if (Value.DisableDeserializer) {
				throw new AerospikeException.Serialize("Object deserializer has been disabled");
			}

			try (ByteArrayInputStream bastream = new ByteArrayInputStream(buffer, offset, count)) {
				try (ObjectInputStream oistream = new ObjectInputStream(bastream)) {
					val = getJavaBlob(oistream.readObject());
				}
			}
			catch (Throwable e) {
				throw new AerospikeException.Serialize(e);
			}
			break;

		case ParticleType.GEOJSON:
			val = getGeoJSON(Buffer.utf8ToString(buffer, offset, count));
			break;

		default:
			val = getBlob(Arrays.copyOfRange(buffer, offset, offset + count));
			break;
		}
		offset += count;
		return val;
	}

	public T unpackObject() throws IOException, ClassNotFoundException {
		int type = buffer[offset++] & 0xff;

		switch (type) {
			case 0xc0: { // nil
				return null;
			}

			case 0xc3: { // boolean true
				return getBoolean(true);
			}

			case 0xc2: { // boolean false
				return getBoolean(false);
			}

			case 0xca: { // float
				float val = Float.intBitsToFloat(Buffer.bytesToInt(buffer, offset));
				offset += 4;
				return getDouble(val);
			}

			case 0xcb: { // double
				double val = Double.longBitsToDouble(Buffer.bytesToLong(buffer, offset));
				offset += 8;
				return getDouble(val);
			}

			case 0xd0: { // signed 8 bit integer
				return getLong(buffer[offset++]);
			}

			case 0xcc: { // unsigned 8 bit integer
				return getLong(buffer[offset++] & 0xff);
			}

			case 0xd1: { // signed 16 bit integer
				short val = Buffer.bigSigned16ToShort(buffer, offset);
				offset += 2;
				return getLong(val);
			}

			case 0xcd: { // unsigned 16 bit integer
				int val = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return getLong(val);
			}

			case 0xd2: { // signed 32 bit integer
				int val = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return getLong(val);
			}

			case 0xce: { // unsigned 32 bit integer
				long val = Buffer.bigUnsigned32ToLong(buffer, offset);
				offset += 4;
				return getLong(val);
			}

			case 0xd3: { // signed 64 bit integer
				long val = Buffer.bytesToLong(buffer, offset);
				offset += 8;
				return getLong(val);
			}

			case 0xcf: { // unsigned 64 bit integer
				// Java is constrained to signed longs, so that is the best we can do here.
				long val = Buffer.bytesToLong(buffer, offset);
				offset += 8;
				return getLong(val);
			}

			case 0xc4:
			case 0xd9: { // string/raw bytes with 8 bit header
				int count = buffer[offset++] & 0xff;
				return (T)unpackBlob(count);
			}

			case 0xc5:
			case 0xda: { // string/raw bytes with 16 bit header
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return (T)unpackBlob(count);
			}

			case 0xc6:
			case 0xdb: { // string/raw bytes with 32 bit header
				// Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return (T)unpackBlob(count);
			}

			case 0xdc: { // list with 16 bit header
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return unpackList(count);
			}

			case 0xdd: { // list with 32 bit header
				// Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return unpackList(count);
			}

			case 0xde: { // map with 16 bit header
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return unpackMap(count);
			}

			case 0xdf: { // map with 32 bit header
				// Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return unpackMap(count);
			}

			case 0xd4: { // Skip over type extension with 1 byte
				offset += 1 + 1;
				return null;
			}

			case 0xd5: { // Skip over type extension with 2 bytes
				offset += 1 + 2;
				return null;
			}

			case 0xd6: { // Skip over type extension with 4 bytes
				offset += 1 + 4;
				return null;
			}

			case 0xd7: { // Skip over type extension with 8 bytes
				offset += 1 + 8;
				return null;
			}

			case 0xd8: { // Skip over type extension with 16 bytes
				offset += 1 + 16;
				return null;
			}

			case 0xc7: { // Skip over type extension with 8 bit header and bytes
				int count = buffer[offset] & 0xff;
				offset += count + 1 + 1;
				return null;
			}

			case 0xc8: { // Skip over type extension with 16 bit header and bytes
				int count = Buffer.bytesToShort(buffer, offset);
				offset += count + 1 + 2;
				return null;
			}

			case 0xc9: { // Skip over type extension with 32 bit header and bytes
				int count = Buffer.bytesToInt(buffer, offset);
				offset += count + 1 + 4;
				return null;
			}

			default: {
				if ((type & 0xe0) == 0xa0) { // raw bytes with 8 bit combined header
					return unpackBlob(type & 0x1f);
				}

				if ((type & 0xf0) == 0x80) { // map with 8 bit combined header
					return unpackMap(type & 0x0f);
				}

				if ((type & 0xf0) == 0x90) { // list with 8 bit combined header
					return unpackList(type & 0x0f);
				}

				if (type < 0x80) { // 8 bit combined unsigned integer
					return getLong(type);
				}

				if (type >= 0xe0) { // 8 bit combined signed integer
					return getLong(type - 0xe0 - 32);
				}
				throw new IOException("Unknown unpack type: " + type);
			}
		}
	}

	protected abstract T getMap(Map<T,T> value);
	protected abstract T getList(List<T> value);
	protected abstract T getJavaBlob(Object value);
	protected abstract T getBlob(byte[] value);
	protected abstract T getString(String value);
	protected abstract T getLong(long value);
	protected abstract T getDouble(double value);
	protected abstract T getBoolean(boolean value);
	protected abstract T getGeoJSON(String value);

	public static Object unpackObjectList(byte[] buffer, int offset, int length) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker(buffer, offset, length);
		return unpacker.unpackList();
	}

	public static Object unpackObjectMap(byte[] buffer, int offset, int length) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker(buffer, offset, length);
		return unpacker.unpackMap();
	}

	public static Object unpackObject(byte[] buffer, int offset, int length) throws AerospikeException {
		try {
			if (length <= 0) {
				return null;
			}
			ObjectUnpacker unpacker = new ObjectUnpacker(buffer, offset, length);
			return unpacker.unpackObject();
		}
		catch (Throwable e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static final class ObjectUnpacker extends Unpacker<Object> {

		public ObjectUnpacker(byte[] buffer, int offset, int length) {
			super(buffer, offset, length);
		}

		@Override
		protected Object getMap(Map<Object,Object> value) {
			return value;
		}

		@Override
		protected Object getList(List<Object> value) {
			return value;
		}

		@Override
		protected Object getJavaBlob(Object value) {
			return value;
		}

		@Override
		protected Object getBlob(byte[] value) {
			return value;
		}

		@Override
		protected Object getString(String value) {
			return value;
		}

		@Override
		protected Object getLong(long value) {
			return value;
		}

		@Override
		protected Object getDouble(double value) {
			return value;
		}

		@Override
		protected Object getBoolean(boolean value) {
			return value;
		}

		@Override
		protected Object getGeoJSON(String value) {
			return Value.getAsGeoJSON(value);
		}
	}
}
