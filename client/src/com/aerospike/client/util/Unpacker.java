/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

/**
 * De-serialize collection objects using MessagePack format specification:
 * 
 * http://wiki.msgpack.org/display/MSGPACK/Format+specification#Formatspecification-int32
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
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}

	private T unpackList(int count) throws IOException, ClassNotFoundException {
		ArrayList<T> out = new ArrayList<T>();
		
		for (int i = 0; i < count; i++) {
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
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}
	
	private T unpackMap(int count) throws IOException, ClassNotFoundException {
		HashMap<T,T> out = new HashMap<T,T>();

		for (int i = 0; i < count; i++) {
			T key = unpackObject();
			T val = unpackObject();
			out.put(key, val);
		}
		return getMap(out);
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
			ByteArrayInputStream bastream = new ByteArrayInputStream(buffer, offset, count);
			ObjectInputStream oistream = new ObjectInputStream(bastream);
			val = getJavaBlob(oistream.readObject());
			break;
			
		default:
			val = getBlob(Arrays.copyOfRange(buffer, offset, offset + count));
			break;
		}
		offset += count;
		return val;
	}

	private T unpackObject() throws IOException, ClassNotFoundException {
		int type = buffer[offset++] & 0xff;
		
		switch (type) {
			case 0xc0: {
				return null;
			}
	
			case 0xc3: {
				return getBoolean(true);
			}
				
			case 0xc2: {
				return getBoolean(false);
			}
				
			case 0xca: {
				float val = Float.intBitsToFloat(Buffer.bytesToInt(buffer, offset));
				offset += 4;			
				return getDouble(val);
			}
			
			case 0xcb: {
				double val = Double.longBitsToDouble(Buffer.bytesToLong(buffer, offset));
				offset += 8;			
				return getDouble(val);
			}

			case 0xcc: {
				return getLong(buffer[offset++] & 0xff);
			}
				
			case 0xcd: {
				int val = Buffer.bytesToShort(buffer, offset);
				offset += 2;			
				return getLong(val);
			}

			case 0xce: {
				int val = Buffer.bytesToInt(buffer, offset);
				offset += 4;			
				return getLong(val);
			}

			case 0xcf: {
				long val = Buffer.bytesToLong(buffer, offset);
				offset += 8;			
				return getLong(val);
			}

			case 0xd0: {
				return getLong(buffer[offset++]);
			}

			case 0xd1: {
				int val = Buffer.bytesToShort(buffer, offset);
				offset += 2;			
				return getLong(val);
			}

			case 0xd2: {
				int val = Buffer.bytesToInt(buffer, offset);
				offset += 4;			
				return getLong(val);
			}

			case 0xd3: {
				long val = Buffer.bytesToLong(buffer, offset);
				offset += 8;
				return getLong(val);
			}

			case 0xda: {
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return (T)unpackBlob(count);
			}
			
			case 0xdb: {
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return (T)unpackBlob(count);
			}

			case 0xdc: {
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return unpackList(count);
			}
			
			case 0xdd: {
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return unpackList(count);
			}
				
			case 0xde: {
				int count = Buffer.bytesToShort(buffer, offset);
				offset += 2;
				return unpackMap(count);
			}
			
			case 0xdf: {
				int count = Buffer.bytesToInt(buffer, offset);
				offset += 4;
				return unpackMap(count);
			}
			
			default: {
				if ((type & 0xe0) == 0xa0) {
					return unpackBlob(type & 0x1f);
				}

				if ((type & 0xf0) == 0x80) {	
					return unpackMap(type & 0x0f);
				}
				
				if ((type & 0xf0) == 0x90) {			
					return unpackList(type & 0x0f);
				}

				if (type < 0x80) {
		        	return getLong(type);
				}
				
				if (type >= 0xe0) {
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

	public static Object unpackObjectList(byte[] buffer, int offset, int length) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker(buffer, offset, length);
		return unpacker.unpackList();
	}

	public static Object unpackObjectMap(byte[] buffer, int offset, int length) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker(buffer, offset, length);
		return unpacker.unpackMap();
	}

	private static final class ObjectUnpacker extends Unpacker<Object> {
		
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
	}
}
