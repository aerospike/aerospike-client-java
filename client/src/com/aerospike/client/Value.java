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
package com.aerospike.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNil;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Packer;

/**
 * Polymorphic value classes used to efficiently serialize objects into the wire protocol.
 */
public abstract class Value {
	/**
	 * Get string or null value instance.
	 */
	public static Value get(String value) {
		return (value == null)? new NullValue() : new StringValue(value);
	}

	/**
	 * Get byte array or null value instance.
	 */
	public static Value get(byte[] value) {
		return (value == null)? new NullValue() : new BytesValue(value);
	}
	
	/**
	 * Get byte segment or null value instance.
	 */
	public static Value get(byte[] value, int offset, int length) {
		return (value == null)? new NullValue() : new ByteSegmentValue(value, offset, length);
	}

	/**
	 * Get integer value instance.
	 */
	public static Value get(int value) {
		return new IntegerValue(value);
	}
	
	/**
	 * Get long value instance.
	 */
	public static Value get(long value) {
		return new LongValue(value);
	}

	/**
	 * Get value array instance.
	 */
	public static Value get(Value[] value) {
		return (value == null)? new NullValue() : new ValueArray(value);
	}

	/**
	 * Get blob or null value instance.
	 */
	public static Value getAsBlob(Object value) {
		return (value == null)? new NullValue() : new BlobValue(value);
	}

	/**
	 * Get list or null value instance.
	 * Supported by Aerospike 3 servers only.
	 */
	public static Value getAsList(List<?> value) {
		return (value == null)? new NullValue() : new ListValue(value);
	}
	
	/**
	 * Get map or null value instance.
	 * Supported by Aerospike 3 servers only.
	 */
	public static Value getAsMap(Map<?,?> value) {
		return (value == null)? new NullValue() : new MapValue(value);
	}

	/**
	 * Get null value instance.
	 */
	public static Value getAsNull() {
		return new NullValue();
	}
	
	/**
	 * Determine value given generic object.
	 * This is the slowest of the Value get() methods.
	 */
	public static Value get(Object value) {
		if (value == null)
			return new NullValue();
		
		if (value instanceof String)
        	return new StringValue((String)value);
			
        if (value instanceof byte[])
        	return new BytesValue((byte[])value);
        
		if (value instanceof Integer)
        	return new IntegerValue((Integer)value);
			
		if (value instanceof Long)
        	return new LongValue((Long)value);
		
		if (value instanceof Value) {
			return (Value)value;
		}	    	
		return new BlobValue(value);
	}
	
	/**
	 * Calculate number of bytes necessary to serialize the value in the wire protocol.
	 */
	public abstract int estimateSize() throws AerospikeException;

	/**
	 * Serialize the value in the wire protocol.
	 */
	public abstract int write(byte[] buffer, int offset) throws AerospikeException;
	
	/**
	 * Serialize the value using MessagePack.
	 */
	public abstract void pack(Packer packer) throws IOException;

	/**
	 * Get wire protocol value type.
	 */
	public abstract int getType();
	
	/**
	 * Return original value as an Object.
	 */
	public abstract Object getObject();
	
	/**
	 * Return value as an Object.
	 */
	public abstract LuaValue getLuaValue();

	/**
	 * Empty value.
	 */
	public static final class NullValue extends Value {
		@Override
		public int estimateSize() {
			return 0;
		}

		@Override
		public int write(byte[] buffer, int offset) {	
			return 0;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packNil();
		}

		@Override
		public int getType() {
			return ParticleType.NULL;
		}
		
		@Override
		public Object getObject() {
			return null;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaNil.NIL;
		}

		@Override
		public String toString() {
			return null;
		}
	}
	
	/**
	 * Byte array value.
	 */
	public static final class BytesValue extends Value {		
		private final byte[] bytes;

		public BytesValue(byte[] bytes) {
			this.bytes = bytes;
		}
		
		@Override
		public int estimateSize() {
			return bytes.length;

		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packBytes(bytes);
		}

		@Override
		public int getType() {
			return ParticleType.BLOB;
		}
		
		@Override
		public Object getObject() {
			return bytes;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaString.valueOf(bytes);
		}

		@Override
		public String toString() {
			return Buffer.bytesToHexString(bytes);
		}
	}

	/**
	 * Byte segment value.
	 */
	public static final class ByteSegmentValue extends Value {		
		private final byte[] bytes;
		private final int offset;
		private final int length;

		public ByteSegmentValue(byte[] bytes, int offset, int length) {
			this.bytes = bytes;
			this.offset = offset;
			this.length = length;
		}
		
		@Override
		public int estimateSize() {
			return length;

		}
		
		@Override
		public int write(byte[] buffer, int targetOffset) {
			System.arraycopy(bytes, offset, buffer, targetOffset, length);
			return length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packBytes(bytes, offset, length);
		}

		@Override
		public int getType() {
			return ParticleType.BLOB;
		}
		
		@Override
		public Object getObject() {
			return this;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaString.valueOf(bytes, offset, length);
		}

		@Override
		public String toString() {
			return Buffer.bytesToHexString(bytes, offset, length);
		}
		
		public byte[] getBytes() {
			return bytes;
		}

		public int getOffset() {
			return offset;
		}

		public int getLength() {
			return length;
		}
	}

	/**
	 * String value.
	 */
	public static final class StringValue extends Value {		
		private final String value;

		public StringValue(String value) {
			this.value = value;
		}

		@Override
		public int estimateSize() {
			return Buffer.estimateSizeUtf8(value);
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			return Buffer.stringToUtf8(value, buffer, offset);
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packString(value);
		}

		@Override
		public int getType() {
			return ParticleType.STRING;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaString.valueOf(value);
		}

		@Override
		public String toString() {
			return value;
		}
	}
	
	/**
	 * Integer value.
	 */
	public static final class IntegerValue extends Value {		
		private final int value;

		public IntegerValue(int value) {
			this.value = value;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(value, buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packInt(value);
		}

		@Override
		public int getType() {
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaInteger.valueOf(value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	/**
	 * Long value.
	 */
	public static final class LongValue extends Value {		
		private final long value;

		public LongValue(long value) {
			this.value = value;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(value, buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packLong(value);
		}

		@Override
		public int getType() {
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaInteger.valueOf(value);
		}

		@Override
		public String toString() {
			return Long.toString(value);
		}
	}

	/**
	 * Blob value.
	 */
	public static final class BlobValue extends Value {
		private final Object object;
		private byte[] bytes;

		public BlobValue(Object object) {
			this.object = object;
		}
		
		@Override
		public int estimateSize() throws AerospikeException.Serialize {
			try {
				ByteArrayOutputStream bstream = new ByteArrayOutputStream();
				ObjectOutputStream ostream = new ObjectOutputStream(bstream);
				ostream.writeObject(object);
				ostream.close();
				bytes = bstream.toByteArray();
				return bytes.length;
			}
			catch (Exception e) {
				throw new AerospikeException.Serialize(e);
			}
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packBlob(object);
		}

		@Override
		public int getType() {
			return ParticleType.JBLOB;
		}
		
		@Override
		public Object getObject() {
			return object;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return LuaString.valueOf(bytes);
		}

		@Override
		public String toString() {
			return Buffer.bytesToHexString(bytes);
		}
	}
	
	/**
	 * Value array.
	 * Supported by Aerospike 3 servers only.
	 */
	public static final class ValueArray extends Value {
		private final Value[] array;
		private byte[] bytes;

		public ValueArray(Value[] array) {
			this.array = array;
		}
		
		@Override
		public int estimateSize() throws AerospikeException {
			bytes = Packer.pack(array);
			return bytes.length;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packValueArray(array);
		}

		@Override
		public int getType() {
			return ParticleType.LIST;
		}
		
		@Override
		public Object getObject() {
			return array;
		}

		@Override
		public LuaValue getLuaValue() {
			return null;
		}

		@Override
		public String toString() {
			return Arrays.toString(array);
		}
	}

	/**
	 * List value.
	 * Supported by Aerospike 3 servers only.
	 */
	public static final class ListValue extends Value {
		private final List<?> list;
		private byte[] bytes;

		public ListValue(List<?> list) {
			this.list = list;
		}
		
		@Override
		public int estimateSize() throws AerospikeException {
			bytes = Packer.pack(list);
			return bytes.length;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packList(list);
		}

		@Override
		public int getType() {
			return ParticleType.LIST;
		}
		
		@Override
		public Object getObject() {
			return list;
		}

		@Override
		public LuaValue getLuaValue() {
			return null;
		}
		
		@Override
		public String toString() {
			return list.toString();
		}
	}

	/**
	 * Map value.
	 * Supported by Aerospike 3 servers only.
	 */
	public static final class MapValue extends Value {
		private final Map<?,?> map;
		private byte[] bytes;

		public MapValue(Map<?,?> map)  {
			this.map = map;
		}
		
		@Override
		public int estimateSize() throws AerospikeException {
			bytes = Packer.pack(map);
			return bytes.length;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packMap(map);
		}

		@Override
		public int getType() {
			return ParticleType.MAP;
		}
		
		@Override
		public Object getObject() {
			return map;
		}
		
		@Override
		public LuaValue getLuaValue() {
			return null;
		}
		
		@Override
		public String toString() {
			return map.toString();
		}
	}
}
