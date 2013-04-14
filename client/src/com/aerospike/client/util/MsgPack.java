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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.BufferUnpacker;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

public final class MsgPack {
	private static final MessagePack msgpack = new MessagePack();

	//-------------------------------------------------------
	// Pack methods
	//-------------------------------------------------------

	public static byte[] pack(com.aerospike.client.Value[] val) throws AerospikeException {
		try {
			BufferPacker packer = msgpack.createBufferPacker();
			packValueArray(packer, val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] pack(List<?> val) throws AerospikeException {
		try {
			BufferPacker packer = msgpack.createBufferPacker();
			packList(packer, val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}
	
	public static byte[] pack(Map<?,?> val) throws AerospikeException {
		try {
			BufferPacker packer = msgpack.createBufferPacker();
			packMap(packer, val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}
	
	public static void packBytes(Packer packer, byte[] val) throws IOException {
        byte[] buf = new byte[val.length + 1];		
		buf[0] = ParticleType.BLOB;
		System.arraycopy(val, 0, buf, 1, val.length);
		packer.write(buf);
	}

	public static void packString(Packer packer, String val) throws IOException {
        int size = Buffer.estimateSizeUtf8(val);
        byte[] buf = new byte[size + 1];		
		buf[0] = ParticleType.STRING;
		Buffer.stringToUtf8(val, buf, 1);
		packer.write(buf);
	}
	
	public static void packValueArray(Packer packer, com.aerospike.client.Value[] values) throws IOException {
		packer.writeArrayBegin(values.length);
		for (com.aerospike.client.Value value : values) {
			value.pack(packer);
		}
		packer.writeArrayEnd();
	}
	
	public static void packList(Packer packer, List<?> list) throws IOException {
		packer.writeArrayBegin(list.size());
		for (Object obj : list) {
			packObject(packer, obj);
		}
		packer.writeArrayEnd();
	}
	
	public static void packMap(Packer packer, Map<?,?> map) throws IOException {
		packer.writeMapBegin(map.size());
		for (Entry<?,?> entry : map.entrySet()) {
			packObject(packer, entry.getKey());
			packObject(packer, entry.getValue());
		}
		packer.writeMapEnd();
	}

	public static void packBlob(Packer packer, Object val) throws IOException {
		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		ObjectOutputStream ostream = new ObjectOutputStream(bstream);
		ostream.writeObject(val);
		ostream.close();
		byte[] buf = bstream.toByteArray();
		buf[0] = ParticleType.JBLOB;
		System.arraycopy(val, 0, buf, 1, buf.length);
		packer.write(buf);
	}

	private static void packObject(Packer packer, Object obj) throws IOException {
		if (obj == null) {
			packer.writeNil();
			return;
		}
		
		if (obj instanceof String) {
			packString(packer, (String) obj);
			return;
		}
		
		if (obj instanceof byte[]) {
			packBytes(packer, (byte[]) obj);
			return;
		}
		
		if (obj instanceof Integer) {
			packer.write((Integer) obj);
			return;
		}
		
		if (obj instanceof Long) {
			packer.write((Long) obj);
			return;
		}

		if (obj instanceof com.aerospike.client.Value) {
			com.aerospike.client.Value value = (com.aerospike.client.Value) obj;
			value.pack(packer);
			return;
		}
		
		if (obj instanceof List<?>) {
			packList(packer, (List<?>) obj);
			return;
		}
		
		if (obj instanceof Map<?,?>) {
			packMap(packer, (Map<?,?>) obj);
			return;
		}
		
		packBlob(packer, obj);
	}

	//-------------------------------------------------------
	// Unpack methods
	//-------------------------------------------------------

	public static Object parseList(byte[] buf, int offset, int len) throws AerospikeException {
		try {
			BufferUnpacker unpacker = msgpack.createBufferUnpacker(buf, offset, len);
			return unpackList(unpacker.readValue());
		} 
		catch (Exception e) {
    		throw new AerospikeException.Serialize(e);
		}
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
	
	private static Object unpackObject(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		switch (in.getType()) {
			case INTEGER:
				return in.asIntegerValue().getLong();
				
			case RAW:
				return unpackBlob(in);
				
			case MAP:
				return unpackMap(in);
				
			case ARRAY:
				return unpackList(in);
				
			default:
				return null;
		}
	}
	
	private static List<Object> unpackList(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		ArrayList<Object> out = new ArrayList<Object>();
		
		if (! in.isArrayValue()) {
			return out;
		}
		
		org.msgpack.type.Value[] elements = in.asArrayValue().getElementArray();
		
		for (org.msgpack.type.Value v : elements) {
			out.add(unpackObject(v));
		}
		return out;
	}
	
	private static Map<Object,Object> unpackMap(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		HashMap<Object,Object> out = new HashMap<Object,Object>();
		
		if (! in.isMapValue()) {
			return out;
		}
		
		org.msgpack.type.Value[] elements = in.asMapValue().getKeyValueArray();
		
		for (int i = 0; i < elements.length; i++) {
			Object key = unpackObject(elements[i]);
			Object val = unpackObject(elements[++i]);
			out.put(key, val);
		}
		return out;
	}
	
	private static Object unpackBlob(org.msgpack.type.Value in) throws IOException, ClassNotFoundException {
		byte[] raw = in.asRawValue().getByteArray();
		
		switch (raw[0]) {
		case ParticleType.STRING:
			return Buffer.utf8ToString(raw, 1, raw.length - 1);

		case ParticleType.JBLOB:
			ByteArrayInputStream bastream = new ByteArrayInputStream(raw, 1, raw.length - 1);
			ObjectInputStream oistream = new ObjectInputStream(bastream);
			return oistream.readObject();
			
		default:
			return Arrays.copyOfRange(raw, 1, raw.length - 1);
		}
	}
}
