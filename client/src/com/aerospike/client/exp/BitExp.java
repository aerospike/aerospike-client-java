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
package com.aerospike.client.exp;

import com.aerospike.client.operation.BitOverflowAction;
import com.aerospike.client.operation.BitPolicy;
import com.aerospike.client.operation.BitResizeFlags;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * Bit expression generator. See {@link com.aerospike.client.exp.Exp}.
 * <p>
 * The bin expression argument in these methods can be a reference to a bin or the
 * result of another expression. Expressions that modify bin values are only used
 * for temporary expression evaluation and are not permanently applied to the bin.
 * Bit modify expressions return the blob bin's value.
 * <p>
 * Offset orientation is left-to-right.  Negative offsets are supported.
 * If the offset is negative, the offset starts backwards from end of the bitmap.
 * If an offset is out of bounds, a parameter error will be returned.
 */
public final class BitExp {
	private static final int MODULE = 1;
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
	 * Create expression that resizes byte[] to byteSize according to resizeFlags (See {@link BitResizeFlags})
	 * and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010]</li>
	 * <li>byteSize = 4</li>
	 * <li>resizeFlags = 0</li>
	 * <li>returns [0b00000001, 0b01000010, 0b00000000, 0b00000000]</li>
	 * </ul>
	 * <pre>{@code
	 * // Resize bin "a" and compare bit count
	 * Exp.eq(
	 *   BitExp.count(Exp.val(0), Exp.val(3),
	 *     BitExp.resize(BitPolicy.Default, Exp.val(4), 0, Exp.blobBin("a"))),
	 *   Exp.val(2))
	 * }</pre>
	 */
	public static Exp resize(BitPolicy policy, Exp byteSize, int resizeFlags, Exp bin) {
		byte[] bytes = Pack.pack(RESIZE, byteSize, policy.flags, resizeFlags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that inserts value bytes into byte[] bin at byteOffset and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>byteOffset = 1</li>
	 * <li>value = [0b11111111, 0b11000111]</li>
	 * <li>bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 * <pre>{@code
	 * // Insert bytes into bin "a" and compare bit count
	 * Exp.eq(
	 *   BitExp.count(Exp.val(0), Exp.val(3),
	 *     BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin("a"))),
	 *   Exp.val(2))
	 * }</pre>
	 */
	public static Exp insert(BitPolicy policy, Exp byteOffset, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(INSERT, byteOffset, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that removes bytes from byte[] bin at byteOffset for byteSize and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>byteOffset = 2</li>
	 * <li>byteSize = 3</li>
	 * <li>bin result = [0b00000001, 0b01000010]</li>
	 * </ul>
	 * <pre>{@code
	 * // Remove bytes from bin "a" and compare bit count
	 * Exp.eq(
	 *   BitExp.count(Exp.val(0), Exp.val(3),
	 *     BitExp.remove(BitPolicy.Default, Exp.val(2), Exp.val(3), Exp.blobBin("a"))),
	 *   Exp.val(2))
	 * }</pre>
	 */
	public static Exp remove(BitPolicy policy, Exp byteOffset, Exp byteSize, Exp bin) {
		byte[] bytes = Pack.pack(REMOVE, byteOffset, byteSize, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that sets value on byte[] bin at bitOffset for bitSize and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 13</li>
	 * <li>bitSize = 3</li>
	 * <li>value = [0b11100000]</li>
	 * <li>bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 * <pre>{@code
	 * // Set bytes in bin "a" and compare bit count
	 * Exp.eq(
	 *   BitExp.count(Exp.val(0), Exp.val(3),
	 *     BitExp.set(BitPolicy.Default, Exp.val(13), Exp.val(3), Exp.val(bytes), Exp.blobBin("a"))),
	 *   Exp.val(2))
	 * }</pre>
	 */
	public static Exp set(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(SET, bitOffset, bitSize, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that performs bitwise "or" on value and byte[] bin at bitOffset for bitSize
	 * and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 17</li>
	 * <li>bitSize = 6</li>
	 * <li>value = [0b10101000]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp or(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(OR, bitOffset, bitSize, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize
	 * and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 17</li>
	 * <li>bitSize = 6</li>
	 * <li>value = [0b10101100]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp xor(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(XOR, bitOffset, bitSize, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that performs bitwise "and" on value and byte[] bin at bitOffset for bitSize
	 * and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 23</li>
	 * <li>bitSize = 9</li>
	 * <li>value = [0b00111100, 0b10000000]</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp and(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(AND, bitOffset, bitSize, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that negates byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 25</li>
	 * <li>bitSize = 6</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp not(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp bin) {
		byte[] bytes = Pack.pack(NOT, bitOffset, bitSize, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that shifts left byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 32</li>
	 * <li>bitSize = 8</li>
	 * <li>shift = 3</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]</li>
	 * </ul>
	 */
	public static Exp lshift(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp shift, Exp bin) {
		byte[] bytes = Pack.pack(LSHIFT, bitOffset, bitSize, shift, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that shifts right byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 0</li>
	 * <li>bitSize = 9</li>
	 * <li>shift = 1</li>
	 * <li>bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp rshift(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp shift, Exp bin) {
		byte[] bytes = Pack.pack(RSHIFT, bitOffset, bitSize, shift, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that adds value to byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * BitSize must be <= 64. Signed indicates if bits should be treated as a signed number.
	 * If add overflows/underflows, {@link BitOverflowAction} is used.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 16</li>
	 * <li>value = 128</li>
	 * <li>signed = false</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]</li>
	 * </ul>
	 */
	public static Exp add(
		BitPolicy policy,
		Exp bitOffset,
		Exp bitSize,
		Exp value,
		boolean signed,
		BitOverflowAction action,
		Exp bin
	) {
		byte[] bytes = packMath(ADD, policy, bitOffset, bitSize, value, signed, action);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that subtracts value from byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * BitSize must be <= 64. Signed indicates if bits should be treated as a signed number.
	 * If add overflows/underflows, {@link BitOverflowAction} is used.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 16</li>
	 * <li>value = 128</li>
	 * <li>signed = false</li>
	 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]</li>
	 * </ul>
	 */
	public static Exp subtract(
		BitPolicy policy,
		Exp bitOffset,
		Exp bitSize,
		Exp value,
		boolean signed,
		BitOverflowAction action,
		Exp bin
	) {
		byte[] bytes = packMath(SUBTRACT, policy, bitOffset, bitSize, value, signed, action);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that sets value to byte[] bin starting at bitOffset for bitSize and returns byte[].
	 * BitSize must be <= 64.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 1</li>
	 * <li>bitSize = 8</li>
	 * <li>value = 127</li>
	 * <li>bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]</li>
	 * </ul>
	 */
	public static Exp setInt(BitPolicy policy, Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(SET_INT, bitOffset, bitSize, value, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that returns bits from byte[] bin starting at bitOffset for bitSize.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 9</li>
	 * <li>bitSize = 5</li>
	 * <li>returns [0b10000000]</li>
	 * </ul>
	 * <pre>{@code
	 * // Bin "a" bits = [0b10000000]
	 * Exp.eq(
	 *   BitExp.get(Exp.val(9), Exp.val(5), Exp.blobBin("a")),
	 *   Exp.val(new byte[] {(byte)0b10000000}))
	 * }</pre>
	 */
	public static Exp get(Exp bitOffset, Exp bitSize, Exp bin) {
		byte[] bytes = Pack.pack(GET, bitOffset, bitSize);
		return addRead(bin, bytes, Exp.Type.BLOB);
	}

	/**
	 * Create expression that returns integer count of set bits from byte[] bin starting at
	 * bitOffset for bitSize.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 20</li>
	 * <li>bitSize = 4</li>
	 * <li>returns 2</li>
	 * </ul>
	 * <pre>{@code
	 * // Bin "a" bit count <= 2
	 * Exp.le(BitExp.count(Exp.val(0), Exp.val(5), Exp.blobBin("a")), Exp.val(2))
	 * }</pre>
	 */
	public static Exp count(Exp bitOffset, Exp bitSize, Exp bin) {
		byte[] bytes = Pack.pack(COUNT, bitOffset, bitSize);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns integer bit offset of the first specified value bit in byte[] bin
	 * starting at bitOffset for bitSize.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 24</li>
	 * <li>bitSize = 8</li>
	 * <li>value = true</li>
	 * <li>returns 5</li>
	 * </ul>
	 * <pre>{@code
	 * // lscan(a) == 5
	 * Exp.eq(BitExp.lscan(Exp.val(24), Exp.val(8), Exp.val(true), Exp.blobBin("a")), Exp.val(5))
	 * }</pre>
	 *
	 * @param bitOffset		offset int expression
	 * @param bitSize		size int expression
	 * @param value			boolean expression
	 * @param bin			bin or blob value expression
	 */
	public static Exp lscan(Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(LSCAN, bitOffset, bitSize, value);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns integer bit offset of the last specified value bit in byte[] bin
	 * starting at bitOffset for bitSize.
	 * Example:
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 32</li>
	 * <li>bitSize = 8</li>
	 * <li>value = true</li>
	 * <li>returns 7</li>
	 * </ul>
	 * <pre>{@code
	 * // rscan(a) == 7
	 * Exp.eq(BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin("a")), Exp.val(7))
	 * }</pre>
	 *
	 * @param bitOffset		offset int expression
	 * @param bitSize		size int expression
	 * @param value			boolean expression
	 * @param bin			bin or blob value expression
	 */
	public static Exp rscan(Exp bitOffset, Exp bitSize, Exp value, Exp bin) {
		byte[] bytes = Pack.pack(RSCAN, bitOffset, bitSize, value);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns integer from byte[] bin starting at bitOffset for bitSize.
	 * Signed indicates if bits should be treated as a signed number.
	 * <ul>
	 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
	 * <li>bitOffset = 8</li>
	 * <li>bitSize = 16</li>
	 * <li>signed = false</li>
	 * <li>returns 16899</li>
	 * </ul>
	 * <pre>{@code
	 * // getInt(a) == 16899
	 * Exp.eq(BitExp.getInt(Exp.val(8), Exp.val(16), false, Exp.blobBin("a")), Exp.val(16899))
	 * }</pre>
	 */
	public static Exp getInt(Exp bitOffset, Exp bitSize, boolean signed, Exp bin) {
		byte[] bytes = packGetInt(bitOffset, bitSize, signed);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	private static byte[] packMath(
		int command,
		BitPolicy policy,
		Exp bitOffset,
		Exp bitSize,
		Exp value,
		boolean signed,
		BitOverflowAction action
	) {
		Packer packer = new Packer();
		// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
		// Pack.init(packer, ctx);
		packer.packArrayBegin(6);
		packer.packInt(command);
		bitOffset.pack(packer);
		bitSize.pack(packer);
		value.pack(packer);
		packer.packInt(policy.flags);

		int flags = action.flags;

		if (signed) {
			flags |= INT_FLAGS_SIGNED;
		}
		packer.packInt(flags);
		return packer.toByteArray();
	}

	private static byte[] packGetInt(Exp bitOffset, Exp bitSize, boolean signed) {
		Packer packer = new Packer();
		// Pack.init() only required when CTX is used and server does not support CTX for bit operations.
		// Pack.init(packer, ctx);
		packer.packArrayBegin(signed ? 4 : 3);
		packer.packInt(GET_INT);
		bitOffset.pack(packer);
		bitSize.pack(packer);

		if (signed) {
			packer.packInt(INT_FLAGS_SIGNED);
		}
		return packer.toByteArray();
	}

	private static Exp addWrite(Exp bin, byte[] bytes) {
		return new Exp.Module(bin, bytes, Exp.Type.BLOB.code, MODULE | Exp.MODIFY);
	}

	private static Exp addRead(Exp bin, byte[] bytes, Exp.Type retType) {
		return new Exp.Module(bin, bytes, retType.code, MODULE);
	}
}
