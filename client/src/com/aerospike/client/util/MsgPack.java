/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;

import com.aerospike.client.AerospikeException;

public final class MsgPack {
	private static final MessagePack msgpack = new MessagePack();
		
	public static byte[] packObject(Object object) throws AerospikeException {
		try {
			BufferPacker packer = msgpack.createBufferPacker();
			packer.write(object);		
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] packArray(Object[] objects) throws AerospikeException {		
		try {
			BufferPacker packer = msgpack.createBufferPacker();
			packer.write(Arrays.asList(objects));	
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}
	
	public static Object parseList(byte[] buf, int offset, int len) throws AerospikeException {
		try {
			BufferUnpacker unpacker = msgpack.createBufferUnpacker(buf, offset, len);
			return unpackList(unpacker.readValue());
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}

	private static List<Object> unpackList(Value in) throws IOException {
		ArrayList<Object> out = new ArrayList<Object>();
		if ( !in.isArrayValue() ) return out;
		Value[] elements = in.asArrayValue().getElementArray();
		for ( Value v : elements ) {
			out.add( unpackObject(v) );
		}
		return out;
	}

	public static Object parseMap(byte[] buf, int offset, int len) throws AerospikeException {
		try {
			BufferUnpacker unpacker = msgpack.createBufferUnpacker(buf, offset, len);
			return unpackMap(unpacker.readValue());
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
	}
	
	private static Map<Object,Object> unpackMap(Value in) throws IOException {
		HashMap<Object,Object> out = new HashMap<Object,Object>();
		if ( !in.isMapValue() ) return out;
		Value[] elements = in.asMapValue().getKeyValueArray();
		for ( int i = 0; i < elements.length; i++ ) {
			Object key = unpackObject(elements[i]);
			Object val = unpackObject(elements[++i]);
			out.put( key, val );
		}
		return out;
	}

	private static Object unpackObject(Value in) throws IOException {
		switch( in.getType() ) {
			case INTEGER:
				return in.asIntegerValue().getLong();
			case RAW:
				return in.asRawValue().getString();
			case MAP:
				return unpackMap(in);
			case ARRAY:
				return unpackList(in);
			default:
				return null;
		}
	}	
}
