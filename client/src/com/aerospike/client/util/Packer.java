/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;

/**
 * Serialize collection objects using MessagePack format specification:
 * 
 * https://github.com/msgpack/msgpack/blob/master/spec.md
 */
public final class Packer {
	
	public static byte[] pack(Value[] val) throws AerospikeException {
		try {
			Packer packer = new Packer();
			packer.packValueArray(val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}

	public static byte[] pack(List<?> val) throws AerospikeException {
		try {
			Packer packer = new Packer();
			packer.packList(val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}
	
	public static byte[] pack(Map<?,?> val) throws AerospikeException {
		try {
			Packer packer = new Packer();
			packer.packMap(val);
			return packer.toByteArray();
		}
		catch (Exception e) {
			throw new AerospikeException.Serialize(e);
		}
	}
	
	private byte[] buffer;
	private int offset;
	private ArrayList<BufferItem> bufferList;
	
	public Packer() {
		this.buffer = ThreadLocalData.getBuffer();
	}
	
	public void packValueArray(Value[] values) {
		packArrayBegin(values.length);
		for (Value value : values) {
			value.pack(this);
		}
	}
	
	public void packValueList(List<Value> list) {
		packArrayBegin(list.size());
		for (Value value : list) {
			value.pack(this);
		}
	}

	public void packList(List<?> list) {
		packArrayBegin(list.size());
		for (Object obj : list) {
			packObject(obj);
		}
	}

	public void packArrayBegin(int size) {
        if (size < 16) {
        	packByte(0x90 | size);
        } 
        else if (size < 65536) {
        	packShort(0xdc, size);
        } 
        else {
        	packInt(0xdd, size);
        }
    }

	public void packValueMap(Map<Value,Value> map) {
		packMapBegin(map.size());
		for (Entry<Value,Value> entry : map.entrySet()) {
			entry.getKey().pack(this);
			entry.getValue().pack(this);
		}
	}

	public void packMap(Map<?,?> map) {
		packMapBegin(map.size());
		for (Entry<?,?> entry : map.entrySet()) {
			packObject(entry.getKey());
			packObject(entry.getValue());
		}
	}

	public void packMapBegin(int size) {
        if (size < 16) {
        	packByte(0x80 | size);
        }
        else if (size < 65536) {
        	packShort(0xde, size);
        }
        else {
        	packInt(0xdf, size);
        }
    }

	public void packBytes(byte[] b) {
    	packByteArrayBegin(b.length + 1);
    	packByte(ParticleType.BLOB);
    	packByteArray(b, 0, b.length);
    }
	
	public void packBytes(byte[] b, int offset, int length) {
    	packByteArrayBegin(length + 1);
    	packByte(ParticleType.BLOB);
    	packByteArray(b, offset, length);
	}
	
	public void packBlob(Object val) throws AerospikeException {
		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		
		try {
			ObjectOutputStream ostream = new ObjectOutputStream(bstream);
			ostream.writeObject(val);
			ostream.close();
		}
		catch (IOException ioe) {
			throw new AerospikeException.Serialize(ioe);
		}
		
		byte[] bytes = bstream.toByteArray();		
        packByteArrayBegin(bytes.length + 1);
    	packByte(ParticleType.JBLOB);
    	packByteArray(bytes, 0, bytes.length);
	}

	public void packGeoJSON(String val) {
		byte[] buffer = Buffer.stringToUtf8(val);
		packByteArrayBegin(buffer.length + 1);
		packByte(ParticleType.GEOJSON);
		packByteArray(buffer, 0, buffer.length);
	}
	
	private void packByteArrayBegin(int size) {
		// Continue to pack byte arrays as strings until all servers/clients
		// have been upgraded to handle new message pack binary type.
		if (size < 32) {
			packByte(0xa0 | size);
		}
		else if (size < 65536) {
			packShort(0xda, size);
		}
		else {
			packInt(0xdb, size);
		}

		// TODO: Replace with this code after all servers/clients
		// have been upgraded to handle new message pack binary type.
		/*
		if (size < 32) {
			packByte(0xa0 | size);
		}
		else if (size < 256) {
			packByte(0xc4, size);
		}
		else if (size < 65536) {
			packShort(0xc5, size);
		}
		else {
			packInt(0xc6, size);
		}*/
	}

	public void packObject(Object obj) {
		if (obj == null) {
			packNil();
			return;
		}
		
		if (obj instanceof Value) {
			Value value = (Value) obj;
			value.pack(this);
			return;
		}
		
		if (obj instanceof byte[]) {
			packBytes((byte[]) obj);
			return;
		}
		
		if (obj instanceof String) {
			packString((String) obj);
			return;
		}
		
		if (obj instanceof Integer) {
			packInt((Integer) obj);
			return;
		}
		
		if (obj instanceof Long) {
			packLong((Long) obj);
			return;
		}

		if (obj instanceof Double) {
			packDouble((Double) obj);
			return;
		}
		
		if (obj instanceof Float) {
			packFloat((Float) obj);
			return;
		}
		
		if (obj instanceof Boolean) {
			packBoolean((Boolean)obj);
			return;
		}

		if (obj instanceof List<?>) {
			packList((List<?>) obj);
			return;
		}
		
		if (obj instanceof Map<?,?>) {
			packMap((Map<?,?>) obj);
			return;
		}
		
		packBlob(obj);
	}

	public void packLong(long val) {	
    	if (val >= 0L) { 		
        	if (val < 128L) {
        		packByte((int)val);
        		return;
        	}
        	
        	if (val < 256L) {       		
        		packByte(0xcc, (int)val);
        		return;
        	}
        	
        	if (val < 65536L) {
        		packShort(0xcd, (int)val);
        		return;
        	}
        	
        	if (val < 4294967296L) {
        		packInt(0xce, (int)val);
        		return;
        	}
        	packLong(0xcf, val);
    	}
    	else {  		
        	if (val >= -32) {
        		packByte(0xe0 | ((int)val + 32));
        		return;
        	}
        	
        	if (val >= Byte.MIN_VALUE) {
                packByte(0xd0, (int)val);
                return;
        	}
        	
        	if (val >= Short.MIN_VALUE) {
                packShort(0xd1, (int)val);
                return;
        	}
        	
        	if (val >= Integer.MIN_VALUE) {
        		packInt(0xd2, (int)val);
                return;
        	}
        	packLong(0xd3, val);
    	}
    }
    
    public void packInt(int val) {
    	if (val >= 0) { 		
        	if (val < 128) {
        		packByte(val);
        		return;
        	}
        	
        	if (val < 256) {       		
        		packByte(0xcc, val);
        		return;
        	}
        	
        	if (val < 65536) {
        		packShort(0xcd, val);
        		return;
        	}
        	packInt(0xce, val);
    	}
    	else {  		
        	if (val >= -32) {
        		packByte(0xe0 | (val + 32));
        		return;
        	}
        	
        	if (val >= Byte.MIN_VALUE) {
                packByte(0xd0, val);
                return;
        	}
        	
        	if (val >= Short.MIN_VALUE) {
                packShort(0xd1, val);
                return;
        	}    	
        	packInt(0xd2, val);
    	}
    }

	public void packString(String val) {     	
		int size = Buffer.estimateSizeUtf8(val) + 1;
    
		if (size < 32) {
			packByte(0xa0 | size);
		}
		// TODO: Enable this code after all servers/clients
		// have been upgraded to handle 8 bit string length format.		
		/*
		else if (size < 256) {
			packByte(0xd9, size);
		}
		*/
		else if (size < 65536) {
			packShort(0xda, size);
		}
		else {
			packInt(0xdb, size);
		}

		if (offset + size > buffer.length) {
			resize(size);
		}
		buffer[offset++] = (byte)ParticleType.STRING;
		offset += Buffer.stringToUtf8(val, buffer, offset);
	}

	private void packByteArray(byte[] src, int srcOffset, int srcLength) {
       	if (offset + srcLength > buffer.length) {
    		resize(srcLength);
    	}
 		System.arraycopy(src, srcOffset, buffer, offset, srcLength);
		offset += srcLength;
    }

    public void packDouble(double val) {
    	if (offset + 9 > buffer.length) {
    		resize(9);
    	}
    	buffer[offset++] = (byte)0xcb;
    	Buffer.longToBytes(Double.doubleToLongBits(val), buffer, offset);
    	offset += 8;
    }
    
    public void packFloat(float val) {
    	if (offset + 5 > buffer.length) {
    		resize(5);
    	}
    	buffer[offset++] = (byte)0xca;
    	Buffer.intToBytes(Float.floatToIntBits(val), buffer, offset);
    	offset += 4;
    }

    private void packLong(int type, long val) {
    	if (offset + 9 > buffer.length) {
    		resize(9);
    	}
    	buffer[offset++] = (byte)type;
    	Buffer.longToBytes(val, buffer, offset);
    	offset += 8;
    }

    private void packInt(int type, int val) {
    	if (offset + 5 > buffer.length) {
    		resize(5);
    	}
    	buffer[offset++] = (byte)type;
    	Buffer.intToBytes(val, buffer, offset);
    	offset += 4;
    }
    
    private void packShort(int type, int val) {
    	if (offset + 3 > buffer.length) {
    		resize(3);
    	}
    	buffer[offset++] = (byte)type;
    	Buffer.shortToBytes(val, buffer, offset);
    	offset += 2;
    }

    public void packRawShort(int val) {
    	// WARNING. This method is not compatible with message pack standard.
    	if (offset + 2 > buffer.length) {
    		resize(2);
    	}
    	Buffer.shortToBytes(val, buffer, offset);
    	offset += 2;
    }

    private void packByte(int type, int val) {
    	if (offset + 2 > buffer.length) {
    		resize(2);
    	}
    	buffer[offset++] = (byte)type;
    	buffer[offset++] = (byte)val;
    }

    public void packBoolean(boolean val) {
    	if (offset + 1 > buffer.length) {
    		resize(1);
    	}
    	
    	if (val) {
    		buffer[offset++] = (byte)0xc3;
    	}
    	else {
    		buffer[offset++] = (byte)0xc2;
    	}
    }
    
    public void packNil() {
    	if (offset >= buffer.length) {
    		resize(1);
    	}
    	buffer[offset++] = (byte)0xc0;
    }

    public void packByte(int val) {
    	if (offset >= buffer.length) {
    		resize(1);
    	}
    	buffer[offset++] = (byte)val;
    }
    
    private void resize(int size) {
    	if (bufferList == null) {
    		bufferList = new ArrayList<BufferItem>();
    	}
    	bufferList.add(new BufferItem(buffer, offset));
    	
    	if (size < buffer.length) {
    		size = buffer.length;
    	}
    	buffer = new byte[size];    		
    	offset = 0;
    }
    
    public byte[] toByteArray() {
    	if (bufferList != null) {
        	int size = offset;        	
    		for (BufferItem item : bufferList) {
    			size += item.length;
    		}
        	
    		byte[] target = new byte[size];
    		size = 0;
    		for (BufferItem item : bufferList) {
            	System.arraycopy(item.buffer, 0, target, size, item.length);
    			size += item.length;
    		}

    		System.arraycopy(buffer, 0, target, size, offset);
        	return target;
    	}
    	else {
        	byte[] target = new byte[offset];
        	System.arraycopy(buffer, 0, target, 0, offset);
        	return target;
    	}
	}
    
    private static final class BufferItem {
    	private byte[] buffer;
    	private int length;
    	
    	private BufferItem(byte[] buffer, int length) {
    		this.buffer = buffer;
    		this.length = length;
    	}
    }
}
