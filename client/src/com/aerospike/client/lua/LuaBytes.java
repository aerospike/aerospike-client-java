/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
package com.aerospike.client.lua;

import org.luaj.vm2.LuaUserdata;

import com.aerospike.client.command.Buffer;

public final class LuaBytes extends LuaUserdata implements LuaData {

	private byte[] bytes;
	private int size;
	private int type;

	public LuaBytes(LuaInstance instance, int capacity) {
		super(new byte[capacity]);
		bytes = (byte[])super.m_instance;
		setmetatable(instance.getPackage("Bytes"));
	}
	
	public LuaBytes(LuaInstance instance, byte[] bytes) {
		super(bytes);
		this.bytes = bytes;
		this.size = bytes.length;
		setmetatable(instance.getPackage("Bytes"));
	}

	public void appendBigInt16(short value) {
		setBigInt16(value, size);
	}

	public void appendLittleInt16(short value) {
		setLittleInt16(value, size);
	}

	public void appendBigInt32(int value) {
		setBigInt32(value, size);
	}

	public void appendLittleInt32(int value) {
		setLittleInt32(value, size);
	}

	public void appendBigInt64(long value) {
		setBigInt64(value, size);
	}

	public void appendLittleInt64(long value) {
		setLittleInt64(value, size);
	}

	public int appendVarInt(int value) {
		return setVarInt(value, size);
	}
	
	public void appendString(String value) {
		setString(value, size);
	}

	public void appendBytes(LuaBytes value, int length) {
		setBytes(value, size, length);
	}
	
	public void appendByte(byte value) {
		setByte(value, size);
	}

	public void setBigInt16(short value, int offset) {
		int capacity = offset + 2;
		ensureCapacity(capacity);
		Buffer.shortToBytes(value, bytes, offset);
		resetSize(capacity);
	}
	
	public void setLittleInt16(short value, int offset) {
		int capacity = offset + 2;
		ensureCapacity(capacity);
		Buffer.shortToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setBigInt32(int value, int offset) {
		int capacity = offset + 4;
		ensureCapacity(capacity);
		Buffer.intToBytes(value, bytes, offset);
		resetSize(capacity);
	}
	
	public void setLittleInt32(int value, int offset) {
		int capacity = offset + 4;
		ensureCapacity(capacity);
		Buffer.intToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setBigInt64(long value, int offset) {
		int capacity = offset + 8;
		ensureCapacity(capacity);
		Buffer.longToBytes(value, bytes, offset);
		resetSize(capacity);
	}
	
	public void setLittleInt64(long value, int offset) {
		int capacity = offset + 8;
		ensureCapacity(capacity);
		Buffer.longToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public int setVarInt(int value, int offset) {
		ensureCapacity(offset + 5);
		int len = Buffer.intToVarBytes(value, bytes, offset);
		resetSize(offset + len);
		return len;
	}
	
	public int setString(String value, int offset) {
		int len = Buffer.estimateSizeUtf8(value);
		ensureCapacity(offset + len);
		len = Buffer.stringToUtf8(value, bytes, offset);
		resetSize(offset + len);
		return len;
	}

	public void setBytes(LuaBytes value, int offset, int length) {
		if (length == 0 || length > value.size) {
			length = value.size;
		}
		int capacity = offset + length;
		ensureCapacity(capacity);
		System.arraycopy(value.bytes, 0, bytes, offset, length);
		resetSize(capacity);
	}
	
	public void setByte(byte value, int offset) {	
		int capacity = offset + 1;
		ensureCapacity(capacity);
		bytes[offset] = value;
		resetSize(capacity);
	}
	
	public byte getByte(int offset) {
		return bytes[offset];
	}
	
	public int getBigInt16(int offset) {
		return Buffer.bytesToShort(bytes, offset);
	}

	public int getLittleInt16(int offset) {
		return Buffer.littleBytesToShort(bytes, offset);
	}

	public int getBigInt32(int offset) {
		return Buffer.bytesToInt(bytes, offset);
	}

	public int getLittleInt32(int offset) {
		return Buffer.littleBytesToInt(bytes, offset);
	}

	public long getBigInt64(int offset) {
		return Buffer.bytesToLong(bytes, offset);
	}

	public long getLittleInt64(int offset) {
		return Buffer.littleBytesToLong(bytes, offset);
	}

	public int[] getVarInt(int offset) {
		return Buffer.varBytesToInt(bytes, offset);
	}

	public String getString(int offset, int length) {
		return Buffer.utf8ToString(bytes, offset, length);
	}
	
	public byte[] getBytes(int offset, int length) {
		byte[] target = new byte[length];
		System.arraycopy(bytes, offset, target, 0, length);
		return target;
	}
	
	private void ensureCapacity(int capacity) {
		if (capacity > bytes.length) {
			int len = bytes.length * 2;
			
			if (capacity > len) {
				len = capacity;
			}
			
			byte[] target = new byte[len];
			System.arraycopy(bytes, 0, target, 0, size);
			bytes = target;
		}
	}

	private void resetSize(int capacity) {	
		if (capacity > size) {
			size = capacity;
		}
	}

	public void setCapacity(int capacity) {
		if (bytes.length == capacity) {
			return;
		}
		
		byte[] target = new byte[capacity];
		
		if (size > capacity) {
			size = capacity;
		}
		System.arraycopy(bytes, 0, target, 0, size);
		bytes = target;
	}

	public Object luaToObject() {
		return bytes;
	}
	
	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}

	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return Buffer.bytesToHexString(bytes, 0, size);
	}
}
