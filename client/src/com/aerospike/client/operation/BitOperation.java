/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.operation;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * Bit operations. Create bit operations used by client operate command.
 * Offset orientation is left-to-right.  Negative offsets are supported.
 * If the offset is negative, the offset starts backwards from end of the bitmap.
 * If an offset is out of bounds, a parameter error will be returned.
 * <p>
 * Bit operations on bitmap items nested in lists/maps are not currently
 * supported by the server.
 */
public final class BitOperation {
	private static final int RESIZE = 0;
	private static final int INSERT = 1;
	private static final int REMOVE = 2;
	private static final int SET = 3;
	private static final int OR = 4;
	private static final int XOR = 5;
	private static final int AND = 6;
	private static final int NOT = 7;
	private static final int LSHIFT = 8;
	private static final int RSHIFT = 9;
	private static final int ADD = 10;
	private static final int SUBTRACT = 11;
	private static final int SET_INT = 12;
	private static final int GET = 50;
	private static final int COUNT = 51;
	private static final int LSCAN = 52;
	private static final int RSCAN = 53;
	private static final int GET_INT = 54;

	private static final int INT_FLAGS_SIGNED = 1;

	/**
	 * Create byte "resize" operation.
	 * Server resizes byte[] to byteSize according to resizeFlags (See {@link BitResizeFlags}).
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010]</li>
	 * <li>byteSize = 4</li>
	 * <li>resizeFlags = 0</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]</li>
	 * </ul>
	 */
	public static Operation resize(BitPolicy policy, String binName, int byteSize, int resizeFlags) {
		byte[] bytes = Pack.pack(BitOperation.RESIZE, byteSize, policy.flags, resizeFlags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create byte "insert" operation.
	 * Server inserts value bytes into byte[] bin at byteOffset.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>byteOffset = 1</li>
	 * <li>value = [0b11111111, 0b11000111]</li>
	 * <li>bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation insert(BitPolicy policy, String binName, int byteOffset, byte[] value) {
		byte[] bytes = Pack.pack(BitOperation.INSERT, byteOffset, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create byte "remove" operation.
	 * Server removes bytes from byte[] bin at byteOffset for byteSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>byteOffset = 2</li>
	 * <li>byteSize = 3</li>
	 * <li>bin result = [0b00000001, 0b01000010]</li>
	 * </ul>
	 */
	public static Operation remove(BitPolicy policy, String binName, int byteOffset, int byteSize) {
		byte[] bytes = Pack.pack(BitOperation.REMOVE, byteOffset, byteSize, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "set" operation.
	 * Server sets value on byte[] bin at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 13</li>
	 * <li>bitSize = 3</li>
	 * <li>value = [0b11100000]</li>
	 * <li>bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation set(BitPolicy policy, String binName, int bitOffset, int bitSize, byte[] value) {
		byte[] bytes = Pack.pack(BitOperation.SET, bitOffset, bitSize, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "or" operation.
	 * Server performs bitwise "or" on value and byte[] bin at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 17</li>
	 * <li>bitSize = 6</li>
	 * <li>value = [0b10101000]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation or(BitPolicy policy, String binName, int bitOffset, int bitSize, byte[] value) {
		byte[] bytes = Pack.pack(BitOperation.OR, bitOffset, bitSize, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "exclusive or" operation.
	 * Server performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 17</li>
	 * <li>bitSize = 6</li>
	 * <li>value = [0b10101100]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation xor(BitPolicy policy, String binName, int bitOffset, int bitSize, byte[] value) {
		byte[] bytes = Pack.pack(BitOperation.XOR, bitOffset, bitSize, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "and" operation.
	 * Server performs bitwise "and" on value and byte[] bin at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 23</li>
	 * <li>bitSize = 9</li>
	 * <li>value = [0b00111100, 0b10000000]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation and(BitPolicy policy, String binName, int bitOffset, int bitSize, byte[] value) {
		byte[] bytes = Pack.pack(BitOperation.AND, bitOffset, bitSize, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "not" operation.
	 * Server negates byte[] bin starting at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 25</li>
	 * <li>bitSize = 6</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation not(BitPolicy policy, String binName, int bitOffset, int bitSize) {
		byte[] bytes = Pack.pack(BitOperation.NOT, bitOffset, bitSize, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "left shift" operation.
	 * Server shifts left byte[] bin starting at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 32</li>
	 * <li>bitSize = 8</li>
	 * <li>shift = 3</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]</li>
	 * </ul>
	 */
	public static Operation lshift(BitPolicy policy, String binName, int bitOffset, int bitSize, int shift) {
		byte[] bytes = Pack.pack(BitOperation.LSHIFT, bitOffset, bitSize, shift, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "right shift" operation.
	 * Server shifts right byte[] bin starting at bitOffset for bitSize.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 0</li>
	 * <li>bitSize = 9</li>
	 * <li>shift = 1</li>
	 * <li>bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation rshift(BitPolicy policy, String binName, int bitOffset, int bitSize, int shift) {
		byte[] bytes = Pack.pack(BitOperation.RSHIFT, bitOffset, bitSize, shift, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "add" operation.
	 * Server adds value to byte[] bin starting at bitOffset for bitSize. BitSize must be &lt;= 64.
	 * Signed indicates if bits should be treated as a signed number.
	 * If add overflows/underflows, {@link BitOverflowAction} is used.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 16</li>
	 * <li>value = 128</li>
	 * <li>signed = false</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]</li>
	 * </ul>
	 */
	public static Operation add(
		BitPolicy policy,
		String binName,
		int bitOffset,
		int bitSize,
		long value,
		boolean signed,
		BitOverflowAction action
	) {
		byte[] bytes = BitOperation.packMath(BitOperation.ADD, policy, bitOffset, bitSize, value, signed, action);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "subtract" operation.
	 * Server subtracts value from byte[] bin starting at bitOffset for bitSize. BitSize must be &lt;= 64.
	 * Signed indicates if bits should be treated as a signed number.
	 * If add overflows/underflows, {@link BitOverflowAction} is used.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 16</li>
	 * <li>value = 128</li>
	 * <li>signed = false</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]</li>
	 * </ul>
	 */
	public static Operation subtract(
		BitPolicy policy,
		String binName,
		int bitOffset,
		int bitSize,
		long value,
		boolean signed,
		BitOverflowAction action
	) {
		byte[] bytes = BitOperation.packMath(BitOperation.SUBTRACT, policy, bitOffset, bitSize, value, signed, action);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "setInt" operation.
	 * Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be &lt;= 64.
	 * Server does not return a value.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 1</li>
	 * <li>bitSize = 8</li>
	 * <li>value = 127</li>
	 * <li>bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Operation setInt(BitPolicy policy, String binName, int bitOffset, int bitSize, long value) {
		byte[] bytes = Pack.pack(BitOperation.SET_INT, bitOffset, bitSize, value, policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create bit "get" operation.
	 * Server returns bits from byte[] bin starting at bitOffset for bitSize.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 9</li>
	 * <li>bitSize = 5</li>
	 * <li>returns [0b10000000]</li>
	 * </ul>
	 */
	public static Operation get(String binName, int bitOffset, int bitSize) {
		byte[] bytes = Pack.pack(BitOperation.GET, bitOffset, bitSize);
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create bit "count" operation.
	 * Server returns integer count of set bits from byte[] bin starting at bitOffset for bitSize.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 20</li>
	 * <li>bitSize = 4</li>
	 * <li>returns 2</li>
	 * </ul>
	 */
	public static Operation count(String binName, int bitOffset, int bitSize) {
		byte[] bytes = Pack.pack(BitOperation.COUNT, bitOffset, bitSize);
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create bit "left scan" operation.
	 * Server returns integer bit offset of the first specified value bit in byte[] bin
	 * starting at bitOffset for bitSize.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 8</li>
	 * <li>value = true</li>
	 * <li>returns 5</li>
	 * </ul>
	 */
	public static Operation lscan(String binName, int bitOffset, int bitSize, boolean value) {
		byte[] bytes = Pack.pack(BitOperation.LSCAN, bitOffset, bitSize, value);
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create bit "right scan" operation.
	 * Server returns integer bit offset of the last specified value bit in byte[] bin
	 * starting at bitOffset for bitSize.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 32</li>
	 * <li>bitSize = 8</li>
	 * <li>value = true</li>
	 * <li>returns 7</li>
	 * </ul>
	 */
	public static Operation rscan(String binName, int bitOffset, int bitSize, boolean value) {
		byte[] bytes = Pack.pack(BitOperation.RSCAN, bitOffset, bitSize, value);
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create bit "get integer" operation.
	 * Server returns integer from byte[] bin starting at bitOffset for bitSize.
	 * Signed indicates if bits should be treated as a signed number.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 8</li>
	 * <li>bitSize = 16</li>
	 * <li>signed = false</li>
	 * <li>returns 16899</li>
	 * </ul>
	 */
	public static Operation getInt(String binName, int bitOffset, int bitSize, boolean signed) {
		byte[] bytes = BitOperation.packGetInt(bitOffset, bitSize, signed);
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(bytes));
	}

	private static byte[] packMath(
		int command,
		BitPolicy policy,
		int bitOffset,
		int bitSize,
		long value,
		boolean signed,
		BitOverflowAction action
	) {
		Packer packer = new Packer();
		// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
		// Pack.init(packer, ctx);
		packer.packArrayBegin(6);
		packer.packInt(command);
		packer.packInt(bitOffset);
		packer.packInt(bitSize);
		packer.packLong(value);
		packer.packInt(policy.flags);

		int flags = action.flags;

		if (signed) {
			flags |= INT_FLAGS_SIGNED;
		}
		packer.packInt(flags);
		return packer.toByteArray();
	}

	private static byte[] packGetInt(int bitOffset, int bitSize, boolean signed) {
		Packer packer = new Packer();
		// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
		// Pack.init(packer, ctx);
		packer.packArrayBegin(signed ? 4 : 3);
		packer.packInt(GET_INT);
		packer.packInt(bitOffset);
		packer.packInt(bitSize);

		if (signed) {
			packer.packInt(INT_FLAGS_SIGNED);
		}
		return packer.toByteArray();
	}
}
