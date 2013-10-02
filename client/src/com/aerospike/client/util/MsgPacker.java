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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.packer.Packer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

public final class MsgPacker {
	protected static final MessagePack msgpack = new MessagePack();

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
		byte[] src = bstream.toByteArray();
        byte[] trg = new byte[src.length + 1];		
		trg[0] = ParticleType.JBLOB;
		System.arraycopy(src, 0, trg, 1, src.length);
		packer.write(trg);
	}

	private static void packObject(Packer packer, Object obj) throws IOException {
		if (obj == null) {
			packer.writeNil();
			return;
		}
		
		if (obj instanceof com.aerospike.client.Value) {
			com.aerospike.client.Value value = (com.aerospike.client.Value) obj;
			value.pack(packer);
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
}
