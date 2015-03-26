/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNil;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaBytes;
import com.aerospike.client.lua.LuaInstance;
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
	 * Get double value instance.
	 */
	public static Value get(double value) {
		return new DoubleValue(value);
	}

	/**
	 * Get float value instance.
	 */
	public static Value get(float value) {
		return new FloatValue(value);
	}

	/**
	 * Get boolean value instance.
	 */
	public static Value get(boolean value) {
		return new BooleanValue(value);
	}

	/**
	 * Get list or null value instance.
	 * Supported by Aerospike 3 servers only.
	 */
	public static Value get(List<?> value) {
		return (value == null)? new NullValue() : new ListValue(value);
	}

	/**
	 * Get map or null value instance.
	 * Supported by Aerospike 3 servers only.
	 */
	public static Value get(Map<?,?> value) {
		return (value == null)? new NullValue() : new MapValue(value);
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
	 * @deprecated Use {@link #get(List value)} instead. 
	 */
	@Deprecated
	public static Value getAsList(List<?> value) {
		return (value == null)? new NullValue() : new ListValue(value);
	}
	
	/**
	 * Get map or null value instance.
	 * @deprecated Use {@link #get(Map value)} instead. 
	 */
	@Deprecated
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
		if (value == null) {
			return new NullValue();
		}
		
		if (value instanceof Value) {
			return (Value)value;
		}

		if (value instanceof byte[]) {
        	return new BytesValue((byte[])value);
		}
		
		if (value instanceof String) {
        	return new StringValue((String)value);
		}
		
		if (value instanceof Integer) {
        	return new IntegerValue((Integer)value);
		}
		
		if (value instanceof Long) {
        	return new LongValue((Long)value);
		}
		
		if (value instanceof Double) {
        	return new DoubleValue((Double)value);
		}
		
		if (value instanceof Float) {
        	return new FloatValue((Float)value);
		}
		
		if (value instanceof Boolean) {
        	return new BooleanValue((Boolean)value);
		}
		
		/* Do not enable this code because it will break clients that use Aerospike 2 servers.
		 * Aerospike 2 servers do not support list/map natively, so the default java runtime 
		 * serialization must be used instead.  If the user chooses the right bin constructor, 
		 * this code is not necessary anyhow.
		 * 
		if (value instanceof List<?>) {
        	return new ListValue((List<?>)value);
		}
		
		if (value instanceof Map<?,?>) {
        	return new MapValue((Map<?,?>)value);
		}
		*/
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
	 * Validate if value type can be used as a key.
	 * @throws AerospikeException	if type can't be used as a key.
	 */
	public void validateKeyType() throws AerospikeException {	
	}
	
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
	public abstract LuaValue getLuaValue(LuaInstance instance);

	/**
	 * Return value as an integer.
	 */
	public int toInteger() {
		return 0;
	}
	
	/**
	 * Return value as a long.
	 */
	public long toLong() {
		return 0;
	}

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
		public void validateKeyType() {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: null");
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
		public LuaValue getLuaValue(LuaInstance instance) {
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return new LuaBytes(instance, bytes);
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
		public LuaValue getLuaValue(LuaInstance instance) {
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
		public LuaValue getLuaValue(LuaInstance instance) {
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return LuaInteger.valueOf(value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
		
		@Override
		public int toInteger() {
			return value;
		}

		@Override
		public long toLong() {
			return value;
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return LuaInteger.valueOf(value);
		}

		@Override
		public String toString() {
			return Long.toString(value);
		}
		
		@Override
		public int toInteger() {
			return (int)value;
		}

		@Override
		public long toLong() {
			return value;
		}
	}

	/**
	 * Double value.
	 */
	public static final class DoubleValue extends Value {		
		private final double value;

		public DoubleValue(double value) {
			this.value = value;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(Double.doubleToLongBits(value), buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packDouble(value);
		}

		@Override
		public int getType() {
			// The server does not natively handle doubles, so store as long (8 byte integer).
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue(LuaInstance instance) {
			return LuaDouble.valueOf(value);
		}

		@Override
		public String toString() {
			return Double.toString(value);
		}
		
		@Override
		public int toInteger() {
			return (int)value;
		}

		@Override
		public long toLong() {
			return (long)value;
		}
	}

	/**
	 * Float value.
	 */
	public static final class FloatValue extends Value {		
		private final float value;

		public FloatValue(float value) {
			this.value = value;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(Double.doubleToLongBits(value), buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packFloat(value);
		}

		@Override
		public int getType() {
			// The server does not natively handle floats, so store as long (8 byte integer).
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue(LuaInstance instance) {
			return LuaDouble.valueOf(value);
		}

		@Override
		public String toString() {
			return Float.toString(value);
		}
		
		@Override
		public int toInteger() {
			return (int)value;
		}

		@Override
		public long toLong() {
			return (long)value;
		}
	}

	/**
	 * Boolean value.  
	 */
	public static final class BooleanValue extends Value {		
		private final boolean value;

		public BooleanValue(boolean value) {
			this.value = value;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(value? 1L : 0L, buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packBoolean(value);
		}

		@Override
		public int getType() {
			// The server does not natively handle boolean, so store as long (8 byte integer).
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
		}
		
		@Override
		public LuaValue getLuaValue(LuaInstance instance) {
			return LuaBoolean.valueOf(value);
		}

		@Override
		public String toString() {
			return Boolean.toString(value);
		}
		
		@Override
		public int toInteger() {
			return value? 1 : 0;
		}

		@Override
		public long toLong() {
			return value? 1L : 0L;
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
		public void validateKeyType() {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: jblob");
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
		public LuaValue getLuaValue(LuaInstance instance) {
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
		public void validateKeyType() {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: value[]");
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return instance.getLuaList(array);
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
		public void validateKeyType() {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: list");
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return instance.getLuaList(list);
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
		public void validateKeyType() {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key type: map");
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
		public LuaValue getLuaValue(LuaInstance instance) {
			return instance.getLuaMap(map);
		}
		
		@Override
		public String toString() {
			return map.toString();
		}
	}
	
	/**
	 * checks if to Values are equal
	 * @param otherValue
	 */
	@Override
	public boolean equals(Object otherValue) {
		return (otherValue != null
				&& this.getClass().equals(otherValue.getClass())
				&& ((Value)otherValue).toString().equals(this.toString()));
	}
}
