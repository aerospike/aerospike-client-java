/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.client.lua;

import org.luaj.vm2.LuaUserdata;

import com.aerospike.client.command.Buffer;

public final class LuaBytes extends LuaUserdata implements LuaData {

	private LuaInstance instance;
	private byte[] bytes;
	private int length;
	private int type;

	public LuaBytes(LuaInstance instance, int capacity) {
		super(new byte[capacity]);
		this.instance = instance;
		bytes = (byte[])super.m_instance;
		setmetatable(instance.getPackage("Bytes"));
	}

	public LuaBytes(LuaInstance instance, byte[] bytes) {
		super(bytes);
		this.bytes = bytes;
		this.length = bytes.length;
		setmetatable(instance.getPackage("Bytes"));
	}

	public int size() {
		return length;
	}

	@Override
	public String toString() {
		return Buffer.bytesToHexString(bytes, 0, length);
	}

	public byte getByte(int offset) {
		return (offset >= 0 && offset < length) ? bytes[offset] : (byte)0;
	}

	public void setByte(int offset, byte b) {
		int capacity = offset + 1;
		ensureCapacity(capacity);
		bytes[offset] = b;
		resetSize(capacity);
	}

	public void setCapacity(int capacity) {
		if (bytes.length == capacity) {
			return;
		}

		byte[] target = new byte[capacity];

		if (length > capacity) {
			length = capacity;
		}
		System.arraycopy(bytes, 0, target, 0, length);
		bytes = target;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getString(int offset, int len) {
		if (offset < 0 || offset >= this.length) {
			return "";
		}

		if (offset + len > this.length) {
			len = this.length - offset;
		}
		return Buffer.utf8ToString(bytes, offset, len);
	}

	public LuaBytes getBytes(int offset, int len) {
		if (offset < 0 || offset >= this.length) {
			return new LuaBytes(instance, new byte[0]);
		}

		if (offset + len > this.length) {
			len = this.length - offset;
		}
		byte[] target = new byte[len];
		System.arraycopy(bytes, offset, target, 0, len);
		return new LuaBytes(instance, target);
	}

	public int getBigInt16(int offset) {
		if (offset < 0 || offset > this.length) {
			return 0;
		}
		return Buffer.bytesToShort(bytes, offset);
	}

	public int getLittleInt16(int offset) {
		if (offset < 0 || offset > this.length) {
			return 0;
		}
		return Buffer.littleBytesToShort(bytes, offset);
	}

	public int getBigInt32(int offset) {
		if (offset < 0 || offset + 4 > this.length) {
			return 0;
		}
		return Buffer.bytesToInt(bytes, offset);
	}

	public int getLittleInt32(int offset) {
		if (offset < 0 || offset + 4 > this.length) {
			return 0;
		}
		return Buffer.littleBytesToInt(bytes, offset);
	}

	public long getBigInt64(int offset) {
		if (offset < 0 || offset + 8 > this.length) {
			return 0;
		}
		return Buffer.bytesToLong(bytes, offset);
	}

	public long getLittleInt64(int offset) {
		if (offset < 0 || offset + 8 > this.length) {
			return 0;
		}
		return Buffer.littleBytesToLong(bytes, offset);
	}

	public int[] getVarInt(int offset) {
		if (offset < 0 || offset > bytes.length) {
			return new int[] {0, 0};
		}
		return Buffer.varBytesToInt(bytes, offset);
	}

	public void setString(int offset, String value) {
		int len = Buffer.estimateSizeUtf8(value);
		ensureCapacity(offset + len);
		len = Buffer.stringToUtf8(value, bytes, offset);
		resetSize(offset + len);
	}

	public void setBytes(int offset, LuaBytes src, int len) {
		if (len == 0 || len > src.length) {
			len = src.length;
		}
		int capacity = offset + length;
		ensureCapacity(capacity);
		System.arraycopy(src.bytes, 0, bytes, offset, len);
		resetSize(capacity);
	}

	public void setBigInt16(int offset, int value) {
		int capacity = offset + 2;
		ensureCapacity(capacity);
		Buffer.shortToBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setLittleInt16(int offset, int value) {
		int capacity = offset + 2;
		ensureCapacity(capacity);
		Buffer.shortToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setBigInt32(int offset, int value) {
		int capacity = offset + 4;
		ensureCapacity(capacity);
		Buffer.intToBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setLittleInt32(int offset, int value) {
		int capacity = offset + 4;
		ensureCapacity(capacity);
		Buffer.intToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setBigInt64(int offset, long value) {
		int capacity = offset + 8;
		ensureCapacity(capacity);
		Buffer.longToBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public void setLittleInt64(int offset, long value) {
		int capacity = offset + 8;
		ensureCapacity(capacity);
		Buffer.longToLittleBytes(value, bytes, offset);
		resetSize(capacity);
	}

	public int setVarInt(int offset, int value) {
		ensureCapacity(offset + 5);
		int len = Buffer.intToVarBytes(value, bytes, offset);
		resetSize(offset + len);
		return len;
	}

	public void appendString(String value) {
		setString(length, value);
	}

	public void appendBigInt16(int value) {
		setBigInt16(length, value);
	}

	public void appendLittleInt16(int value) {
		setLittleInt16(length, value);
	}

	public void appendBigInt32(int value) {
		setBigInt32(length, value);
	}

	public void appendLittleInt32(int value) {
		setLittleInt32(length, value);
	}

	public void appendBigInt64(long value) {
		setBigInt64(length, value);
	}

	public void appendLittleInt64(long value) {
		setLittleInt64(length, value);
	}

	public int appendVarInt(int value) {
		return setVarInt(length, value);
	}

	public void appendBytes(LuaBytes source, int len) {
		setBytes(length, source, len);
	}

	public void appendByte(byte value) {
		setByte(length, value);
	}

	private void ensureCapacity(int capacity) {
		if (capacity > bytes.length) {
			int len = bytes.length * 2;

			if (capacity > len) {
				len = capacity;
			}

			byte[] target = new byte[len];
			System.arraycopy(bytes, 0, target, 0, length);
			bytes = target;
		}
	}

	private void resetSize(int capacity) {
		if (capacity > length) {
			length = capacity;
		}
	}

	public Object luaToObject() {
		return bytes;
	}
}
