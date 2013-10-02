/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.unpacker.BufferUnpacker;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

public abstract class MsgUnpacker<T> {

	public static List<Object> parseObjectList(byte[] buf, int offset, int len) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker();
		return unpacker.parseList(buf, offset, len);
	}

	public static Map<Object,Object> parseObjectMap(byte[] buf, int offset, int len) throws AerospikeException {
		ObjectUnpacker unpacker = new ObjectUnpacker();
		return unpacker.parseMap(buf, offset, len);
	}

	public List<T> parseList(byte[] buf, int offset, int len) throws AerospikeException {
		if (len <= 0) {
			return new ArrayList<T>(0);
		}
		
		try {
			BufferUnpacker unpacker = MsgPacker.msgpack.createBufferUnpacker(buf, offset, len);
			return unpackList(unpacker.readValue());
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}

	public Map<T,T> parseMap(byte[] buf, int offset, int len) throws AerospikeException {
		if (len <= 0) {
			return new HashMap<T,T>(0);
		}

		try {
			BufferUnpacker unpacker = MsgPacker.msgpack.createBufferUnpacker(buf, offset, len);
			return unpackMap(unpacker.readValue());
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}

	private List<T> unpackList(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		ArrayList<T> out = new ArrayList<T>();
		
		if (! in.isArrayValue()) {
			return out;
		}
		
		org.msgpack.type.Value[] elements = in.asArrayValue().getElementArray();
		
		for (org.msgpack.type.Value v : elements) {
			out.add(unpackObject(v));
		}
		return out;
	}
	
	private Map<T,T> unpackMap(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		HashMap<T,T> out = new HashMap<T,T>();
		
		if (! in.isMapValue()) {
			return out;
		}
		
		org.msgpack.type.Value[] elements = in.asMapValue().getKeyValueArray();
		
		for (int i = 0; i < elements.length; i++) {
			T key = unpackObject(elements[i]);
			T val = unpackObject(elements[++i]);
			out.put(key, val);
		}
		return out;
	}
	
	@SuppressWarnings("unchecked")
	protected T unpackObject(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		switch (in.getType()) {
			case INTEGER:
				return getLong(in.asIntegerValue().getLong());
				
			case RAW:
				return unpackBlob(in);
				
			case MAP:
				return (T)unpackMap(in);
				
			case ARRAY:
				return (T)unpackList(in);
				
			default:
				return null;
		}
	}

	private T unpackBlob(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		byte[] raw = in.asRawValue().getByteArray();
		
		switch (raw[0]) {
		case ParticleType.STRING:
			return getString(Buffer.utf8ToString(raw, 1, raw.length - 1));

		case ParticleType.JBLOB:
			ByteArrayInputStream bastream = new ByteArrayInputStream(raw, 1, raw.length - 1);
			ObjectInputStream oistream = new ObjectInputStream(bastream);
			return getJavaBlob(oistream.readObject());
			
		default:
			return getBlob(Arrays.copyOfRange(raw, 1, raw.length));
		}
	}

	protected abstract T getMap(Map<T,T> value);
	protected abstract T getList(List<T> value);
	protected abstract T getJavaBlob(Object value);
	protected abstract T getBlob(byte[] value);
	protected abstract T getString(String value);
	protected abstract T getLong(long value);

	private static class ObjectUnpacker extends MsgUnpacker<Object> {
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
	}
}
