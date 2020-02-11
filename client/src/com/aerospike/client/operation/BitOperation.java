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
package com.aerospike.client.operation;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
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
		return createOperation(RESIZE, Operation.Type.BIT_MODIFY, binName, byteSize, policy.flags, resizeFlags);
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
		Packer packer = new Packer();
		init(packer, INSERT, 3);
		packer.packInt(byteOffset);
		packer.packBytes(value);
		packer.packInt(policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(packer.toByteArray()));
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
		return createOperation(REMOVE, Operation.Type.BIT_MODIFY, binName, byteOffset, byteSize, policy.flags);
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
		return createOperation(SET, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
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
		return createOperation(OR, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
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
		return createOperation(XOR, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
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
		return createOperation(AND, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, value, policy.flags);
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
		return createOperation(NOT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, policy.flags);
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
		return createOperation(LSHIFT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, shift, policy.flags);
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
		return createOperation(RSHIFT, Operation.Type.BIT_MODIFY, binName, bitOffset, bitSize, shift, policy.flags);
	}

	/**
	 * Create bit "add" operation.
	 * Server adds value to byte[] bin starting at bitOffset for bitSize. BitSize must be <= 64.
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
		return createMathOperation(ADD, policy, binName, bitOffset, bitSize, value, signed, action);
	}

	/**
	 * Create bit "subtract" operation.
	 * Server subtracts value from byte[] bin starting at bitOffset for bitSize. BitSize must be <= 64.
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
		return createMathOperation(SUBTRACT, policy, binName, bitOffset, bitSize, value, signed, action);
	}

	/**
	 * Create bit "setInt" operation.
	 * Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be <= 64.
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
		Packer packer = new Packer();
		init(packer, SET_INT, 4);
		packer.packInt(bitOffset);
		packer.packInt(bitSize);
		packer.packLong(value);
		packer.packInt(policy.flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(packer.toByteArray()));
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
		return createOperation(GET, Operation.Type.BIT_READ, binName, bitOffset, bitSize);
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
		return createOperation(COUNT, Operation.Type.BIT_READ, binName, bitOffset, bitSize);
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
		return createOperation(LSCAN, Operation.Type.BIT_READ, binName, bitOffset, bitSize, value);
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
		return createOperation(RSCAN, Operation.Type.BIT_READ, binName, bitOffset, bitSize, value);
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
		Packer packer = new Packer();
		init(packer, GET_INT, signed ? 3 : 2);
		packer.packInt(bitOffset);
		packer.packInt(bitSize);

		if (signed) {
			packer.packInt(INT_FLAGS_SIGNED);
		}
		return new Operation(Operation.Type.BIT_READ, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createOperation(
		int command,
		Operation.Type type,
		String binName,
		int v1,
		int v2
	) {
		Packer packer = new Packer();
		init(packer, command, 2);
		packer.packInt(v1);
		packer.packInt(v2);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createOperation(
		int command,
		Operation.Type type,
		String binName,
		int v1,
		int v2,
		boolean v3
	) {
		Packer packer = new Packer();
		init(packer, command, 3);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packBoolean(v3);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createOperation(
		int command,
		Operation.Type type,
		String binName,
		int v1,
		int v2,
		int v3
	) {
		Packer packer = new Packer();
		init(packer, command, 3);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createOperation(
		int command,
		Operation.Type type,
		String binName,
		int v1,
		int v2,
		int v3,
		int v4
	) {
		Packer packer = new Packer();
		init(packer, command, 4);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);
		packer.packInt(v4);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createOperation(
		int command,
		Operation.Type type,
		String binName,
		int v1,
		int v2,
		byte[] v3,
		int v4
	) {
		Packer packer = new Packer();
		init(packer, command, 4);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packBytes(v3);
		packer.packInt(v4);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	private static Operation createMathOperation(
		int command,
		BitPolicy policy,
		String binName,
		int bitOffset,
		int bitSize,
		long value,
		boolean signed,
		BitOverflowAction action
	) {
		Packer packer = new Packer();
		init(packer, command, 5);
		packer.packInt(bitOffset);
		packer.packInt(bitSize);
		packer.packLong(value);
		packer.packInt(policy.flags);

		int flags = action.flags;

		if (signed) {
			flags |= INT_FLAGS_SIGNED;
		}
		packer.packInt(flags);
		return new Operation(Operation.Type.BIT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	private static void init(Packer packer, int command, int count) {
		packer.packArrayBegin(count + 1);
		packer.packInt(command);
	}
}
