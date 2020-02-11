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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.operation.BitOperation;
import com.aerospike.client.operation.BitOverflowAction;
import com.aerospike.client.operation.BitPolicy;
import com.aerospike.client.operation.BitResizeFlags;
import com.aerospike.client.operation.BitWriteFlags;
import com.aerospike.test.sync.TestSync;

public class TestOperateBit extends TestSync {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static final String binName = "opbbin";
	private static final Key key = new Key(args.namespace, args.set, "opbkey1");

	public void assertBitModifyOperations(byte[] initial, byte[] expected,
		Operation... operations) {
		client.delete(null, key);

		if (initial != null) {
			client.put(null, key, new Bin(binName, initial));
		}

		client.operate(null, key, operations);
		Record record = client.get(null, key, binName);

		//System.out.println("Record  : " + record);

		byte[] actual = (byte[])record.getValue(binName);

		//System.out.println("Initial : " + (initial != null ?
		//		DatatypeConverter.printHexBinary(initial) : "null"));
		//System.out.println("Expected: " +
		//	DatatypeConverter.printHexBinary(expected));
		//System.out.println("Actual  : " +
		//	DatatypeConverter.printHexBinary(actual));

		assertArrayEquals(expected, actual);
	}

	public void assertThrows(Class<?> eclass, int eresult, Operation... ops) {
		try {
			client.operate(null, key, ops);
			assert(false);
		}
		catch (AerospikeException e) {
			assertEquals(eclass, e.getClass());
			assertEquals(eresult, e.getResultCode());
		}
	}

	@Test
	public void operateBitBin() {
		if (! args.validateBit()) {
			return;
		}

		byte[] bit0 = new byte[] {(byte)0x80};
		BitPolicy putMode = BitPolicy.Default;
		BitPolicy updateMode = new BitPolicy(BitWriteFlags.UPDATE_ONLY);

		assertBitModifyOperations(
			new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			new byte[] {0x51, 0x02, 0x03, 0x04, 0x05, 0x06},
			BitOperation.set(putMode, binName, 1, 1, bit0),
			BitOperation.set(updateMode, binName, 3, 1, bit0),
			BitOperation.remove(updateMode, binName, 6, 2)
		);

		BitPolicy addMode = new BitPolicy(BitWriteFlags.CREATE_ONLY);
		byte[] bytes1 = new byte[] {0x0A};

		assertBitModifyOperations(
			null, new byte[] {0x00, 0x0A},
			BitOperation.insert(addMode, binName, 1, bytes1)
		);

		assertThrows(AerospikeException.class, 17,
			BitOperation.set(putMode, "b", 1, 1, bit0));

		assertThrows(AerospikeException.class, 4,
			BitOperation.set(addMode, binName, 1, 1, bit0));
	}

	@Test
	public void operateBitSet() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();
		byte[] bit0 = new byte[] {(byte)0x80};
		byte[] bits1 = new byte[] {0x11, 0x22, 0x33};

		assertBitModifyOperations(
			new byte[] {0x01, 0x12, 0x02, 0x03, 0x04, 0x05, 0x06,
						0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
						0x0E, 0x0F, 0x10, 0x11, 0x41},
			new byte[] {0x41,
						0x13,
						0x11, 0x22, 0x33,
						0x11, 0x22, 0x33,
						0x08,
						0x08, (byte)0x91, 0x1B,
						0X01, 0x12, 0x23,
						0x11, 0x22, 0x11,
						(byte)0xc1},
			BitOperation.set(putMode, binName, 1, 1, bit0),
			BitOperation.set(putMode, binName, 15, 1, bit0),
			// SUM Offest Size
			BitOperation.set(putMode, binName, 16, 24, bits1), //  Y    Y      Y
			BitOperation.set(putMode, binName, 40, 22, bits1), //  N    Y      N
			BitOperation.set(putMode, binName, 73, 21, bits1), //  N    N      N
			BitOperation.set(putMode, binName, 100, 20, bits1),//  Y    N      N
			BitOperation.set(putMode, binName, 120, 17, bits1),//  N    Y      N

			BitOperation.set(putMode, binName, 144, 1, bit0)
		);
	}

	@Test
	public void operateBitLShift() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {0x01, 0x01, 0x00, (byte)0x80,
						(byte)0xFF, 0x01, 0x01,
						0x18, 0x01},
			new byte[] {(byte)0x02, 0x40, 0x01, 0x00,
						(byte)0xF8, 0x08, 0x01,
						0x28, 0x01},
			BitOperation.lshift(putMode, binName, 0, 8, 1),
			BitOperation.lshift(putMode, binName, 9, 7, 6),
			BitOperation.lshift(putMode, binName, 23, 2, 1),

			BitOperation.lshift(putMode, binName, 37, 18, 3),

			BitOperation.lshift(putMode, binName, 58, 2, 1),
			BitOperation.lshift(putMode, binName, 64, 4, 7)
		);

		assertBitModifyOperations(
			new byte[] {(byte)0xFF, (byte)0xFF, (byte)0xFF},
			new byte[] {(byte)0xF8, 0x00, 0x0F},
			BitOperation.lshift(putMode, binName, 0, 20, 15)
		);
	}

	@Test
	public void operateBitRShift() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {(byte)0x80, 0x40, 0x01, 0x00,
						(byte)0xFF, 0x01, 0x01,
						0x18, (byte)0x80},
			new byte[] {0x40, 0x01, 0x00, (byte)0x80,
						(byte)0xF8, (byte)0xE0, 0x21,
						0x14, (byte)0x80},
			BitOperation.rshift(putMode, binName, 0, 8, 1),
			BitOperation.rshift(putMode, binName, 9, 7, 6),
			BitOperation.rshift(putMode, binName, 23, 2, 1),

			BitOperation.rshift(putMode, binName, 37, 18, 3),

			BitOperation.rshift(putMode, binName, 60, 2, 1),
			BitOperation.rshift(putMode, binName, 68, 4, 7)
		);
	}

	@Test
	public void operateBitOr() {
		if (! args.validateBit()) {
			return;
		}

		byte[] bits1 = new byte[] {0x11, 0x22, 0x33};
		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {(byte)0x80, 0x40, 0x01, 0x00, 0x00,
						0x01, 0x02, 0x03},
			new byte[] {(byte)0x90, (byte)0x48, 0x01, 0x20, 0x11,
						(byte)0x11, (byte)0x22, 0x33},
			BitOperation.or(putMode, binName, 0, 5, bits1),
			BitOperation.or(putMode, binName, 9, 7, bits1),
			BitOperation.or(putMode, binName, 23, 6, bits1),
			BitOperation.or(putMode, binName, 32, 8, bits1),

			BitOperation.or(putMode, binName, 40, 24, bits1)
		);
	}

	@Test
	public void operateBitXor() {
		if (! args.validateBit()) {
			return;
		}

		byte[] bits1 = new byte[] {0x11, 0x22, 0x33};
		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {(byte)0x80, 0x40, 0x01, 0x00, 0x00,
						0x01, 0x02, 0x03},
			new byte[] {(byte)0x90, 0x48, 0x01, 0x20, 0x11,0x10, 0x20,
						0x30},
			BitOperation.xor(putMode, binName, 0, 5, bits1),
			BitOperation.xor(putMode, binName, 9, 7, bits1),
			BitOperation.xor(putMode, binName, 23, 6, bits1),
			BitOperation.xor(putMode, binName, 32, 8, bits1),

			BitOperation.xor(putMode, binName, 40, 24, bits1)
		);
	}

	@Test
	public void operateBitAnd() {
		if (! args.validateBit()) {
			return;
		}

		byte[] bits1 = new byte[] {0x11, 0x22, 0x33};
		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {(byte)0x80, 0x40, 0x01, 0x00, 0x00,
						0x01, 0x02, 0x03},
			new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03},
			BitOperation.and(putMode, binName, 0, 5, bits1),
			BitOperation.and(putMode, binName, 9, 7, bits1),
			BitOperation.and(putMode, binName, 23, 6, bits1),
			BitOperation.and(putMode, binName, 32, 8, bits1),

			BitOperation.and(putMode, binName, 40, 24, bits1)
		);
	}

	@Test
	public void operateBitNot() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {(byte)0x80, 0x40, 0x01, 0x00, 0x00,
						0x01, 0x02, 0x03},
			new byte[] {0x78, 0x3F, 0x00, (byte)0xF8, (byte)0xFF,
						(byte)0xFE, (byte)0xFD, (byte)0xFC},
			BitOperation.not(putMode, binName, 0, 5),
			BitOperation.not(putMode, binName, 9, 7),
			BitOperation.not(putMode, binName, 23, 6),
			BitOperation.not(putMode, binName, 32, 8),

			BitOperation.not(putMode, binName, 40, 24)
		);
	}

	@Test
	public void operateBitAdd() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {0x38, 0x1F, 0x00, (byte)0xE8, 0x7F,
						0x00, 0x00, 0x00,
						0x01, 0x01, 0x01,
						0x01, 0x01, 0x01,
						0x02, 0x02, 0x02,
						0x03, 0x03, 0x03},
			new byte[] {0x40, 0x20, 0x01, (byte)0xF0, (byte)0x80,
						0x7F, 0x7F, 0x7F,
						0x02, 0x02, 0x01,
						0x02, 0x02, 0x02,
						0x03, 0x03, 0x06,
						0x07, 0x07, 0x07},
			BitOperation.add(putMode, binName, 0, 5, 1, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 9, 7, 1, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 23, 6, 0x21, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 32, 8, 1, false, BitOverflowAction.FAIL),

			BitOperation.add(putMode, binName, 40, 24, 0x7F7F7F, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 64, 20, 0x01010, false, BitOverflowAction.FAIL),

			BitOperation.add(putMode, binName, 92, 20, 0x10101, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 113, 22, 0x8082, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 136, 23, 0x20202, false, BitOverflowAction.FAIL)
		);

		byte[] initial = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
		int i = 0;

		assertBitModifyOperations(
			initial,
			new byte[] {(byte)0xFE, (byte)0xFE, 0x7F, (byte)0xFF, 0x7F,
						(byte)0x80},
			BitOperation.add(putMode, binName, 8 * i, 8, 0xFF, false, BitOverflowAction.WRAP),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0xFF, false, BitOverflowAction.WRAP),

			BitOperation.add(putMode, binName, 8 * i, 8, 0x7F, true, BitOverflowAction.WRAP),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0x7F, true, BitOverflowAction.WRAP),

			BitOperation.add(putMode, binName, 8 * i, 8, 0x80, true, BitOverflowAction.WRAP),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0xFF, true, BitOverflowAction.WRAP),

			BitOperation.add(putMode, binName, 8 * i, 8, 0x80, false, BitOverflowAction.SATURATE),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0x80, false, BitOverflowAction.SATURATE),

			BitOperation.add(putMode, binName, 8 * i, 8, 0x77, true, BitOverflowAction.SATURATE),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0x77, true, BitOverflowAction.SATURATE),

			BitOperation.add(putMode, binName, 8 * i, 8, 0x8F, true, BitOverflowAction.SATURATE),
			BitOperation.add(putMode, binName, 8 * i++, 8, 0x8F, true, BitOverflowAction.SATURATE)
		);

		client.put(null, key, new Bin(binName, initial));

		assertThrows(AerospikeException.class, 26,
			BitOperation.add(putMode, binName, 0, 8, 0xFF, false, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 0, 8, 0xFF, false, BitOverflowAction.FAIL)
		);

		assertThrows(AerospikeException.class, 26,
			BitOperation.add(putMode, binName, 0, 8, 0x7F, true, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 0, 8, 0x02, true, BitOverflowAction.FAIL)
		);

		assertThrows(AerospikeException.class, 26,
			BitOperation.add(putMode, binName, 0, 8, 0x81, true, BitOverflowAction.FAIL),
			BitOperation.add(putMode, binName, 0, 8, 0xFE, true, BitOverflowAction.FAIL)
		);
	}

	@Test
	public void operateBitSub() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {0x38, 0x1F, 0x00, (byte)0xE8, 0x7F,

						(byte)0x80, (byte)0x80, (byte)0x80,
						0x01, 0x01, 0x01,

						0x01, 0x01, 0x01,
						0x02, 0x02, 0x02,
						0x03, 0x03, 0x03},
			new byte[] {0x30, 0x1E, 0x00, (byte)0xD0, 0x7E,

						0x7F, 0x7F, 0x7F,
						0x00, (byte)0xF0, (byte)0xF1,

						0x00, 0x00, 0x00,
						0x01, (byte)0xFD, (byte)0xFE,
						0x00, (byte)0xE0, (byte)0xE1},
			BitOperation.subtract(putMode, binName, 0, 5, 0x01, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 9, 7, 0x01, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 23, 6, 0x03, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 32, 8, 0x01, false, BitOverflowAction.FAIL),

			BitOperation.subtract(putMode, binName, 40, 24, 0x10101, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 64, 20, 0x101, false, BitOverflowAction.FAIL),

			BitOperation.subtract(putMode, binName, 92, 20, 0x10101, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 113, 21, 0x101, false, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 136, 23, 0x11111, false, BitOverflowAction.FAIL)
		);

		byte[] initial = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
		int i = 0;

		assertBitModifyOperations(
			initial,
			new byte[] {(byte)0xFF, (byte)0xF6, 0x7F, 0x00, (byte)0x80, 0x7F},
			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0x01, false, BitOverflowAction.WRAP),

			BitOperation.subtract(putMode, binName, 8 * i, 8, 0x80, true, BitOverflowAction.WRAP),
			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0x8A, true, BitOverflowAction.WRAP),

			BitOperation.subtract(putMode, binName, 8 * i, 8, 0x7F, true, BitOverflowAction.WRAP),
			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0x02, true, BitOverflowAction.WRAP),

			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0xAA, false, BitOverflowAction.SATURATE),

			BitOperation.subtract(putMode, binName, 8 * i, 8, 0x77, true, BitOverflowAction.SATURATE),
			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0x77, true, BitOverflowAction.SATURATE),

			BitOperation.subtract(putMode, binName, 8 * i, 8, 0x81, true, BitOverflowAction.SATURATE),
			BitOperation.subtract(putMode, binName, 8 * i++, 8, 0x8F, true, BitOverflowAction.SATURATE)
		);

		client.put(null, key, new Bin(binName, initial));

		assertThrows(AerospikeException.class, 26,
			BitOperation.subtract(putMode, binName, 0, 8, 1, false, BitOverflowAction.FAIL)
		);

		assertThrows(AerospikeException.class, 26,
			BitOperation.subtract(putMode, binName, 0, 8, 0x7F, true, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 0, 8, 0x02, true, BitOverflowAction.FAIL)
		);

		assertThrows(AerospikeException.class, 26,
			BitOperation.subtract(putMode, binName, 0, 8, 0x81, true, BitOverflowAction.FAIL),
			BitOperation.subtract(putMode, binName, 0, 8, 0xFE, true, BitOverflowAction.FAIL)
		);
	}

	@Test
	public void operateBitSetInt() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy putMode = new BitPolicy();

		assertBitModifyOperations(
			new byte[] {0x38, 0x1F, 0x00, (byte)0xE8, 0x7F,

						(byte)0x80, (byte)0x80, (byte)0x80,
						0x01, 0x01, 0x01,

						0x01, 0x01, 0x01,
						0x02, 0x02, 0x02,
						0x03, 0x03, 0x03},
			new byte[] {0x08, 0x01, 0x00, 0x18, 0x01,

						0x01, 0x01, 0x01,
						0x00, 0x10, 0x11,

						0x01, 0x01, 0x01,
						0x00, 0x04, 0x06,
						0x02, 0x22, 0x23},
			BitOperation.setInt(putMode, binName, 0, 5, 0x01),
			BitOperation.setInt(putMode, binName, 9, 7, 0x01),
			BitOperation.setInt(putMode, binName, 23, 6, 0x03),
			BitOperation.setInt(putMode, binName, 32, 8, 0x01),

			BitOperation.setInt(putMode, binName, 40, 24, 0x10101),
			BitOperation.setInt(putMode, binName, 64, 20, 0x101),

			BitOperation.setInt(putMode, binName, 92, 20, 0x10101),
			BitOperation.setInt(putMode, binName, 113, 21, 0x101),
			BitOperation.setInt(putMode, binName, 136, 23, 0x11111)
		);
	}

	// READS

	@Test
	public void operateBitGet() {
		if (! args.validateBit()) {
			return;
		}

		client.delete(null, key);

		byte[] bytes = new byte[] {(byte)0xC1, (byte)0xAA, (byte)0xAA};
		client.put(null, key, new Bin(binName, bytes));

		Record record = client.operate(null, key,
			BitOperation.get(binName, 0, 1),
			BitOperation.get(binName, 1, 1),
			BitOperation.get(binName, 7, 1),
			BitOperation.get(binName, 0, 8),

			BitOperation.get(binName, 8, 16),
			BitOperation.get(binName, 9, 15),
			BitOperation.get(binName, 9, 14)
		);

		byte[][] expected = new byte[][] {
			new byte[] {(byte)0x80},
			new byte[] {(byte)0x80},
			new byte[] {(byte)0x80},
			new byte[] {(byte)0xC1},

			new byte[] {(byte)0xAA, (byte)0xAA},
			new byte[] {(byte)0x55, (byte)0x54},
			new byte[] {(byte)0x55, (byte)0x54}
		};

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> result_list = record.getList(binName);
		byte[][] results = new byte[expected.length][];

		for (int i = 0; i < expected.length; i++) {
			results[i] = (byte[])result_list.get(i);
		}

		assertArrayEquals(expected, results);
	}


	public void assertBitReadOperation(byte[] initial, Long[] expected,
		Operation... operations) {
		client.delete(null, key);
		client.put(null, key, new Bin(binName, initial));

		Record record = client.operate(null, key, operations);

		//System.out.println("Record: " + record);

		List<?> result_list = record.getList(binName);
		Long[] actual = new Long[expected.length];

		for (int i = 0; i < expected.length; i++) {
			actual[i] = (Long)result_list.get(i);
		}

		//System.out.println("Initial : " + Arrays.toString(initial));
		//System.out.println("Expected: " + Arrays.toString(expected));
		//System.out.println("Actual  : " + Arrays.toString(actual));

		assertArrayEquals(expected, actual);
	}

	@Test
	public void operateBitCount() {
		if (! args.validateBit()) {
			return;
		}

		assertBitReadOperation(
			new byte[] {(byte)0xC1, (byte)0xAA, (byte)0xAB},
			new Long[] {1l, 1l, 1l, 3l,
						9l, 8l, 7l},
			BitOperation.count(binName, 0, 1),
			BitOperation.count(binName, 1, 1),
			BitOperation.count(binName, 7, 1),
			BitOperation.count(binName, 0, 8),

			BitOperation.count(binName, 8, 16),
			BitOperation.count(binName, 9, 15),
			BitOperation.count(binName, 9, 14)
		);
	}

	@Test
	public void operateBitLScan() {
		if (! args.validateBit()) {
			return;
		}

		assertBitReadOperation(
			new byte[] {(byte)0xFF, (byte)0xFF, (byte)0xFF,
						(byte)0xFF, 0x00, 0x00, 0x00, 0x00, 0x01},
			new Long[] {0l, 0l, 0l,
						0l, -1l, -1l,
						39l, -1l, 0l, 0l,
						0l, 32l,
						0l, -1l},
			BitOperation.lscan(binName, 0, 1, true),
			BitOperation.lscan(binName, 0, 8, true),
			BitOperation.lscan(binName, 0, 9, true),

			BitOperation.lscan(binName, 0, 32, true),
			BitOperation.lscan(binName, 0, 32, false),
			BitOperation.lscan(binName, 1, 30, false),

			BitOperation.lscan(binName, 32, 40, true),
			BitOperation.lscan(binName, 33, 38, true),
			BitOperation.lscan(binName, 32, 40, false),
			BitOperation.lscan(binName, 33, 38, false),

			BitOperation.lscan(binName, 0, 72, true),
			BitOperation.lscan(binName, 0, 72, false),

			BitOperation.lscan(binName, -1, 1, true),
			BitOperation.lscan(binName, -1, 1, false)
		);
	}

	@Test
	public void operateBitRScan() {
		if (! args.validateBit()) {
			return;
		}

		assertBitReadOperation(
			new byte[] {(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
						0x00, 0x00, 0x00, 0x00, 0x01},
			new Long[] {0l, 7l, 8l,
						31l, -1l, -1l,
						39l, -1l, 38l, 37l,
						71l, 70l,
						0l, -1l},
			BitOperation.rscan(binName, 0, 1, true),
			BitOperation.rscan(binName, 0, 8, true),
			BitOperation.rscan(binName, 0, 9, true),

			BitOperation.rscan(binName, 0, 32, true),
			BitOperation.rscan(binName, 0, 32, false),
			BitOperation.rscan(binName, 1, 30, false),

			BitOperation.rscan(binName, 32, 40, true),
			BitOperation.rscan(binName, 33, 38, true),
			BitOperation.rscan(binName, 32, 40, false),
			BitOperation.rscan(binName, 33, 38, false),

			BitOperation.rscan(binName, 0, 72, true),
			BitOperation.rscan(binName, 0, 72, false),

			BitOperation.rscan(binName, -1, 1, true),
			BitOperation.rscan(binName, -1, 1, false)
		);
	}

	@Test
	public void operateBitGetInt() {
		if (! args.validateBit()) {
			return;
		}

		assertBitReadOperation(
			new byte[] {0x0F, 0x0F, 0x00},
			new Long[] {15l, -1l,
						15l, 15l,
						8l, -8l,
						3840l, 3840l,
						3840l, 3840l,
						1920l, 1920l,
						115648l, -15424l,
						15l, -1l},
			BitOperation.getInt(binName, 4, 4, false),
			BitOperation.getInt(binName, 4, 4, true),

			BitOperation.getInt(binName, 0, 8, false),
			BitOperation.getInt(binName, 0, 8, true),

			BitOperation.getInt(binName, 7, 4, false),
			BitOperation.getInt(binName, 7, 4, true),

			BitOperation.getInt(binName, 8, 16, false),
			BitOperation.getInt(binName, 8, 16, true),

			BitOperation.getInt(binName, 9, 15, false),
			BitOperation.getInt(binName, 9, 15, true),

			BitOperation.getInt(binName, 9, 14, false),
			BitOperation.getInt(binName, 9, 14, true),

			BitOperation.getInt(binName, 5, 17, false),
			BitOperation.getInt(binName, 5, 17, true),

			BitOperation.getInt(binName, -12, 4, false),
			BitOperation.getInt(binName, -12, 4, true)
		);
	}

	// Exhaustive modify tests verified with all read ops.

	public void assertBitModifyRegion(int bin_sz, int offset, int set_sz,
		byte[] expected, boolean is_insert, Operation... operations) {
		client.delete(null, key);

		byte[] initial = new byte[bin_sz];

		for (int i = 0; i < bin_sz; i++) {
			initial[i] = (byte)0xFF;
		}

		client.put(null, key, new Bin(binName, initial));

		int int_sz = 64;

		if (set_sz < int_sz) {
			int_sz = set_sz;
		}

		int bin_bit_sz = bin_sz * 8;

		if (is_insert) {
			bin_bit_sz += set_sz;
		}

		Operation[] full_ops = Arrays.copyOf(operations, operations.length + 7);

		full_ops[full_ops.length - 7] = BitOperation.lscan(binName, offset,
			set_sz, true);
		full_ops[full_ops.length - 6] = BitOperation.rscan(binName, offset,
			set_sz, true);
		full_ops[full_ops.length - 5] = BitOperation.getInt(binName, offset,
			int_sz, false);
		full_ops[full_ops.length - 4] = BitOperation.count(binName, offset,
			set_sz);
		full_ops[full_ops.length - 3] = BitOperation.lscan(binName, 0,
			bin_bit_sz, false);
		full_ops[full_ops.length - 2] = BitOperation.rscan(binName, 0,
			bin_bit_sz, false);
		full_ops[full_ops.length - 1] = BitOperation.get(binName, offset,
			set_sz);

		Record record = client.operate(null, key, full_ops);

		List<?> result_list = record.getList(binName);
		long lscan1_result = (Long)result_list.get(result_list.size() - 7);
		long rscan1_result = (Long)result_list.get(result_list.size() - 6);
		long getint_result = (Long)result_list.get(result_list.size() - 5);
		long count_result = (Long)result_list.get(result_list.size() - 4);
		long lscan_result = (Long)result_list.get(result_list.size() - 3);
		long rscan_result = (Long)result_list.get(result_list.size() - 2);
		byte[] actual = (byte[])result_list.get(result_list.size() - 1);
		String err_output = String.format("bin_sz %d offset %d set_sz %d",
			bin_sz, offset, set_sz);

		assertEquals("lscan1 - " + err_output, -1, lscan1_result);
		assertEquals("rscan1 - " + err_output, -1, rscan1_result);
		assertEquals("getint - " + err_output, 0, getint_result);
		assertEquals("count - " + err_output, 0, count_result);
		assertEquals("lscan - " + err_output, offset, lscan_result);
		assertEquals("rscan - " + err_output, offset + set_sz - 1,
			rscan_result);
		assertArrayEquals("op - " + err_output, expected, actual);
	}

	public void assertBitModifyRegion(int bin_sz, int offset, int set_sz,
		byte[] expected, Operation... operations) {
		assertBitModifyRegion(bin_sz, offset, set_sz, expected, false,
			operations);
	}

	public void assertBitModifyInsert(int bin_sz, int offset, int set_sz,
		byte[] expected, Operation... operations) {
		assertBitModifyRegion(bin_sz, offset, set_sz, expected, true,
			operations);
	}


	@Test
	public void operateBitSetEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 80; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
					BitOperation.set(policy, binName, offset, set_sz,
						set_data));
			}
		}
	}

	@Test
	public void operateBitLShiftEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 80; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				int limit = set_sz < 16 ? set_sz + 1 : 16;

				for (int n_bits = 0; n_bits <= limit; n_bits++) {
					assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
						BitOperation.set(policy, binName, offset, set_sz,
							set_data),
						BitOperation.lshift(policy, binName, offset, set_sz,
							n_bits));
				}

				for (int n_bits = 63; n_bits <= set_sz; n_bits++) {
					assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
						BitOperation.set(policy, binName, offset, set_sz,
							set_data),
						BitOperation.lshift(policy, binName, offset, set_sz,
							n_bits));
				}
			}
		}
	}

	@Test
	public void operateBitRShiftEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		BitPolicy partial_policy = new BitPolicy(BitWriteFlags.PARTIAL);
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 80; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				int limit = set_sz < 16 ? set_sz + 1 : 16;

				for (int n_bits = 0; n_bits <= limit; n_bits++) {
					assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
						BitOperation.set(policy, binName, offset, set_sz,
							set_data),
						BitOperation.rshift(policy, binName, offset, set_sz,
							n_bits));
				}

				for (int n_bits = 63; n_bits <= set_sz; n_bits++) {
					assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
						BitOperation.set(policy, binName, offset, set_sz,
							set_data),
						BitOperation.rshift(policy, binName, offset, set_sz,
							n_bits));
				}
			}

			// Test Partial
			int n_bits = 1;

			for (int offset = bin_bit_sz - set_sz + 1; offset < bin_bit_sz;
				 offset++) {
				int actual_set_sz = bin_bit_sz - offset;
				byte[] actual_set_data = new byte[(actual_set_sz + 7) / 8];

				assertBitModifyRegion(bin_sz, offset, actual_set_sz,
					actual_set_data,
					BitOperation.set(partial_policy, binName, offset, set_sz,
						set_data),
					BitOperation.rshift(partial_policy, binName, offset, set_sz,
						n_bits));
			}
		}
	}

	@Test
	public void operateBitAndEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 80; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
					BitOperation.and(policy, binName, offset, set_sz,
						set_data));
			}
		}
	}

	@Test
	public void operateBitNotEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 80; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
					BitOperation.not(policy, binName, offset, set_sz));
			}
		}
	}

	@Test
	public void operateBitInsertEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;

		for (int set_sz = 1; set_sz <= 10; set_sz++) {
			byte[] set_data = new byte[set_sz];

			for (int offset = 0; offset <= bin_sz; offset++) {
				assertBitModifyInsert(bin_sz, offset * 8, set_sz * 8, set_data,
					BitOperation.insert(policy, binName, offset, set_data));
			}
		}
	}

	@Test
	public void operateBitAddEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 64; set_sz++) {
			byte[] set_data = new byte[(set_sz + 7) / 8];

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				assertBitModifyRegion(bin_sz, offset, set_sz, set_data,
					BitOperation.add(policy, binName, offset, set_sz, 1,
						false, BitOverflowAction.WRAP));
			}
		}
	}

	@Test
	public void operateBitSubEx() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		int bin_sz = 15;
		int bin_bit_sz = bin_sz * 8;

		for (int set_sz = 1; set_sz <= 64; set_sz++) {
			byte[] expected = new byte[(set_sz + 7) / 8];
			long value = 0xFFFFffffFFFFffffl >> (64 - set_sz);

			for (int offset = 0; offset <= (bin_bit_sz - set_sz); offset++) {
				assertBitModifyRegion(bin_sz, offset, set_sz, expected,
					BitOperation.subtract(policy, binName, offset, set_sz,
						value, false, BitOverflowAction.WRAP));
			}
		}
	}

	@Test
	public void operateBitNullBlob() {
		if (! args.validateBit()) {
			return;
		}

		BitPolicy policy = new BitPolicy();
		byte[] initial = new byte[]{};
		byte[] buf = new byte[] {(byte)0x80};

		client.delete(null, key);
		client.put(null, key, new Bin(binName, initial));

		assertThrows(AerospikeException.class, 26,
			BitOperation.set(policy, binName, 0, 1, buf));
		assertThrows(AerospikeException.class, 26,
			BitOperation.or(policy, binName, 0, 1, buf));
		assertThrows(AerospikeException.class, 26,
			BitOperation.xor(policy, binName, 0, 1, buf));
		assertThrows(AerospikeException.class, 26,
			BitOperation.and(policy, binName, 0, 1, buf));
		assertThrows(AerospikeException.class, 26,
			BitOperation.not(policy, binName, 0, 1));
		assertThrows(AerospikeException.class, 26,
			BitOperation.lshift(policy, binName, 0, 1, 1));
		assertThrows(AerospikeException.class, 26,
			BitOperation.rshift(policy, binName, 0, 1, 1));
		// OK for insert.
		assertThrows(AerospikeException.class, 4,
			BitOperation.remove(policy, binName, 0, 1));
		assertThrows(AerospikeException.class, 26,
			BitOperation.add(policy, binName, 0, 1, 1, false, BitOverflowAction.FAIL));
		assertThrows(AerospikeException.class, 26,
			BitOperation.subtract(policy, binName, 0, 1, 1, false, BitOverflowAction.FAIL));
		assertThrows(AerospikeException.class, 26,
			BitOperation.setInt(policy, binName, 0, 1, 1));

		assertThrows(AerospikeException.class, 26,
			BitOperation.get(binName, 0, 1));
		assertThrows(AerospikeException.class, 26,
			BitOperation.count(binName, 0, 1));
		assertThrows(AerospikeException.class, 26,
			BitOperation.lscan(binName, 0, 1, true));
		assertThrows(AerospikeException.class, 26,
			BitOperation.rscan(binName, 0, 1, true));
		assertThrows(AerospikeException.class, 26,
			BitOperation.getInt(binName, 0, 1, false));
	}

	@Test
	public void operateBitResize() {
		if (! args.validateBit()) {
			return;
		}

		client.delete(null, key);

		BitPolicy policy = new BitPolicy();
		BitPolicy noFail = new BitPolicy(BitWriteFlags.NO_FAIL);
		Record record = client.operate(null, key,
			BitOperation.resize(policy, binName, 20, BitResizeFlags.DEFAULT),
			BitOperation.get(binName, 19 * 8, 8),
			BitOperation.resize(noFail, binName, 10, BitResizeFlags.GROW_ONLY),
			BitOperation.get(binName, 19 * 8, 8),
			BitOperation.resize(policy, binName, 10, BitResizeFlags.SHRINK_ONLY),
			BitOperation.get(binName, 9 * 8, 8),
			BitOperation.resize(noFail, binName, 30, BitResizeFlags.SHRINK_ONLY),
			BitOperation.get(binName, 9 * 8, 8),
			BitOperation.resize(policy, binName, 19, BitResizeFlags.GROW_ONLY),
			BitOperation.get(binName, 18 * 8, 8),
			BitOperation.resize(noFail, binName, 0, BitResizeFlags.GROW_ONLY),
			BitOperation.resize(policy, binName, 0, BitResizeFlags.SHRINK_ONLY)
		);

		//System.out.println("Record: " + record);

		List<?> result_list = record.getList(binName);
		byte[] get0 = (byte[])result_list.get(1);
		byte[] get1 = (byte[])result_list.get(3);
		byte[] get2 = (byte[])result_list.get(5);
		byte[] get3 = (byte[])result_list.get(7);
		byte[] get4 = (byte[])result_list.get(9);

		assertArrayEquals(new byte[] {0x00}, get0);
		assertArrayEquals(new byte[] {0x00}, get1);
		assertArrayEquals(new byte[] {0x00}, get2);
		assertArrayEquals(new byte[] {0x00}, get3);
		assertArrayEquals(new byte[] {0x00}, get4);
	}
}
