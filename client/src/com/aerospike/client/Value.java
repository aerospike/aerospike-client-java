/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.MsgPack;

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
	 * Get blob or null value instance.
	 */
	public static Value getAsBlob(Object value) {
		return (value == null)? new NullValue() : new BlobValue(value);
	}

	/**
	 * Get list or null value instance.
	 * Support by 3.0 servers only.
	 */
	public static Value getAsList(List<?> value) {
		return (value == null)? new NullValue() : new ListValue(value);
	}
	
	/**
	 * Get map or null value instance.
	 * Support by 3.0 servers only.
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
	 * Get wire protocol value type.
	 */
	public abstract int getType();
	
	/**
	 * Return original value as an Object.
	 */
	public abstract Object getObject();
	
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
		public int getType() {
			return ParticleType.NULL;
		}
		
		@Override
		public Object getObject() {
			return null;
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
		public int getType() {
			return ParticleType.BLOB;
		}
		
		@Override
		public Object getObject() {
			return bytes;
		}
		
		@Override
		public String toString() {
			return Buffer.bytesToHexString(bytes);
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
		public int getType() {
			return ParticleType.STRING;
		}
		
		@Override
		public Object getObject() {
			return value;
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
		public int getType() {
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
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
		public int getType() {
			return ParticleType.INTEGER;
		}
		
		@Override
		public Object getObject() {
			return value;
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
		public int getType() {
			return ParticleType.JBLOB;
		}
		
		@Override
		public Object getObject() {
			return object;
		}
		
		@Override
		public String toString() {
			return Buffer.bytesToHexString(bytes);
		}
	}
	
	/**
	 * List value.
	 * Supported by 3.0 servers only.
	 */
	public static final class ListValue extends Value {
		private final List<?> list;
		private byte[] bytes;

		public ListValue(List<?> list) {
			this.list = list;
		}
		
		@Override
		public int estimateSize() throws AerospikeException {
			bytes = MsgPack.packObject(list);
			return bytes.length;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
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
		public String toString() {
			return list.toString();
		}
	}

	/**
	 * Map value.
	 * Supported by 3.0 servers only.
	 */
	public static final class MapValue extends Value {
		private final Map<?,?> map;
		private byte[] bytes;

		public MapValue(Map<?,?> map)  {
			this.map = map;
		}
		
		@Override
		public int estimateSize() throws AerospikeException {
			bytes = MsgPack.packObject(map);
			return bytes.length;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
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
		public String toString() {
			return map.toString();
		}
	}
}
