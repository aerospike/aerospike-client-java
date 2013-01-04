/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import com.aerospike.client.AerospikeException;

public abstract class Value {
	public abstract int write(byte[] buffer, int offset) throws AerospikeException;
	public abstract int estimateSize();
	public abstract int getType();
	
	public static Value getValue(Object value) throws AerospikeException {
		if (value == null)
			return new NullValue();
		    	
        if (value instanceof byte[])
        	return new BytesValue((byte[])value);
        
		if (value instanceof String)
        	return new StringValue((String)value);
			
		if (value instanceof Integer)
        	return new LongValue((Integer)value);
			
		if (value instanceof Long)
        	return new LongValue((Long)value);
		
		return new BlobValue(value);
	}
	
	public static final class NullValue extends Value {
		@Override
		public int write(byte[] buffer, int offset) {	
			return 0;
		}
		
		@Override
		public int estimateSize() {
			return 0;
		}

		@Override
		public int getType() {
			return ParticleType.NULL;
		}
	}
	
	public static final class BytesValue extends Value {		
		private final byte[] bytes;

		public BytesValue(byte[] bytes) {
			this.bytes = bytes;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			System.arraycopy(bytes, 0, buffer, offset, bytes.length);
			return bytes.length;
		}
		
		@Override
		public int estimateSize() {
			return bytes.length;

		}
		
		@Override
		public int getType() {
			return ParticleType.BLOB;
		}
	}

	public static final class StringValue extends Value {		
		private final String value;

		public StringValue(String value) {
			this.value = value;
		}

		@Override
		public int write(byte[] buffer, int offset) throws AerospikeException {
			return Buffer.stringToUtf8(value, buffer, offset);
		}
		
		@Override
		public int estimateSize() {
			return Buffer.estimateSizeUtf8(value);
		}
		
		@Override
		public int getType() {
			return ParticleType.STRING;
		}
	}
	
	public static final class LongValue extends Value {		
		private final long value;

		public LongValue(long value) {
			this.value = value;
		}
		
		@Override
		public int write(byte[] buffer, int offset) {
			Buffer.longToBytes(value, buffer, offset);
			return 8;
		}
		
		@Override
		public int estimateSize() {
			return 8;
		}
		
		@Override
		public int getType() {
			return ParticleType.INTEGER;
		}
	}

	public static final class BlobValue extends Value {		
		private final byte[] bytes;

		public BlobValue(Object value) throws AerospikeException.Serialize {
			try {
				ByteArrayOutputStream bstream = new ByteArrayOutputStream();
				ObjectOutputStream ostream = new ObjectOutputStream(bstream);
				ostream.writeObject(value);
				ostream.close();
				bytes = bstream.toByteArray();
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
		public int estimateSize() {
			return bytes.length;
		}
		
		@Override
		public int getType() {
			return ParticleType.JBLOB;
		}
	}
}
