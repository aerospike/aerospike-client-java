/*
 * Copyright 2012-2015 Aerospike, Inc.
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
	 * Should the client use the new double floating point particle type supported by Aerospike
	 * server versions >= 3.5.15.  It's important that all server nodes and XDR be upgraded before
	 * enabling this feature.
	 * <p>
	 * If false, the old method using an long particle type is used instead.
	 * <p>
	 * The current default is false.  Eventually, this default will be changed to true in a future client version.
	 */
	public static boolean UseDoubleType = false;
	
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
		
		/* Store double/float values as a java serialized blob when an Object argument is used
		 * instead of the direct double argument (Value.get(double value)).
		 * Therefore, disable this code block.
		if (value instanceof Double) {
        	return new DoubleValue((Double)value);
		}
		
		if (value instanceof Float) {
        	return new FloatValue((Float)value);
		}
		*/
		
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
	 * Get value from Record object. Useful when copying records from one cluster to another.
	 * Since map/list are converted, this method should only be called when using
	 * Aerospike 3 servers.
	 */
	public static Value getFromRecordObject(Object value) {
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
		
		if (value instanceof List<?>) {
        	return new ListValue((List<?>)value);
		}
		
		if (value instanceof Map<?,?>) {
        	return new MapValue((Map<?,?>)value);
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
		
		@Override
		public boolean equals(Object other) {
			if (other == null) {
				return true;
			}			
			return this.getClass().equals(other.getClass());
		}

		@Override
		public final int hashCode() {
			return 0;
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

		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				Arrays.equals(this.bytes, ((BytesValue)other).bytes));
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(bytes);
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
		
		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			
			if (! this.getClass().equals(obj.getClass())) {
				return false;
			}
			ByteSegmentValue other = (ByteSegmentValue)obj;
			
			if (this.length != other.length) {
				return false;
			}
			
			for (int i = 0; i < length; i++) {
				if (this.bytes[this.offset + i] != other.bytes[other.offset + i]) {
					return false;
				}
			}
			return true;
		}

		@Override
		public int hashCode() {
	        int result = 1;
	        for (int i = 0; i < length; i++) {
	            result = 31 * result + bytes[offset+i];
	        }
	        return result;
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
		
		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value.equals(((StringValue)other).value));
		}

		@Override
		public int hashCode() {
	        return value.hashCode();
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
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value == ((IntegerValue)other).value);
		}

		@Override
		public int hashCode() {
	        return value;
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
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value == ((LongValue)other).value);
		}

		@Override
		public int hashCode() {
	        return (int)(value ^ (value >>> 32));
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
			Buffer.doubleToBytes(value, buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packDouble(value);
		}

		@Override
		public int getType() {
			return UseDoubleType? ParticleType.DOUBLE : ParticleType.INTEGER;
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
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value == ((DoubleValue)other).value);
		}

		@Override
		public int hashCode() {
	        long bits = Double.doubleToLongBits(value);
	        return (int)(bits ^ (bits >>> 32));
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
			Buffer.doubleToBytes(value, buffer, offset);
			return 8;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.packFloat(value);
		}

		@Override
		public int getType() {
			return UseDoubleType? ParticleType.DOUBLE : ParticleType.INTEGER;
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
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value == ((FloatValue)other).value);
		}

		@Override
		public int hashCode() {
	        return Float.floatToIntBits(value);
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
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.value == ((BooleanValue)other).value);
		}

		@Override
		public int hashCode() {
	        return value ? 1231 : 1237;
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
		
		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.object.equals(((BlobValue)other).object));
		}

		@Override
		public int hashCode() {
	        return object.hashCode();
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
		
		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				Arrays.equals(this.array, ((ValueArray)other).array));
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(array);
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
		
		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.list.equals(((ListValue)other).list));
		}
		
		@Override
		public int hashCode() {
	        return list.hashCode();
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
		
		@Override
		public boolean equals(Object other) {
			return (other != null &&
				this.getClass().equals(other.getClass()) &&
				this.map.equals(((MapValue)other).map));
		}
		
		@Override
		public int hashCode() {
	        return map.hashCode();
		}	
	}
}
