/*
 * Copyright 2012-2026 Aerospike, Inc.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.VectorValue;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaBytes;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.lua.LuaList;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker;
import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.Vector.ElementType;

/**
 * Boundary and malformed-input unit tests for {@link Vector} and its serialization glue that are
 * not covered by {@link TestVector}'s happy-path tests: empty/oversized vectors, rejected NaN and
 * infinite elements, finite special floating-point values (-0.0/subnormals/extremes), adversarial
 * {@code Vector.from()}
 * input, and additional nesting/wrapper permutations. Pure client-side (no cluster required);
 * registered in {@code SuiteUnit}.
 */
public class TestVectorEdgeCases {
	//-------------------------------------------------------
	// Empty vectors (dimensions == 0)
	//-------------------------------------------------------

	@Test
	public void emptyVectorRejectedAllTypes() {
		assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat16(new short[0]));
		assertThrows(IllegalArgumentException.class, () -> Vector.ofInt32(new int[0]));
		assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat32(new float[0]));
		assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat64(new double[0]));
	}

	//-------------------------------------------------------
	// Large vector
	//-------------------------------------------------------

	@Test
	public void largeVectorRoundTrip() {
		int dims = 4096;
		float[] data = new float[dims];

		for (int i = 0; i < dims; i++) {
			data[i] = i * 0.5f;
		}

		Vector v = Vector.ofFloat32(data);

		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		Vector parsed = Vector.from(buffer, 0, buffer.length);
		assertEquals(v, parsed);
		assertArrayEquals(data, parsed.getFloat32Data(), 0.0f);
	}

	//-------------------------------------------------------
	// Integer extremes
	//-------------------------------------------------------

	@Test
	public void int32Extremes() {
		int[] data = new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1};
		Vector v = Vector.ofInt32(data);

		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		Vector parsed = Vector.from(buffer, 0, buffer.length);
		assertArrayEquals(data, parsed.getInt32Data());
	}

	//-------------------------------------------------------
	// Special float32 / float64 values (exact bit preservation)
	//-------------------------------------------------------

	@Test
	public void nonFiniteElementsRejected() {
		for (float f : new float[] {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
			assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat32(new float[] {1.0f, f}));
		}

		for (double d : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
			assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat64(new double[] {1.0, d}));
		}

		// +Inf, -Inf, quiet NaN, and the NaN with the smallest mantissa.
		for (short bits : new short[] {(short)0x7c00, (short)0xfc00, (short)0x7e00, (short)0x7c01}) {
			assertThrows(IllegalArgumentException.class, () -> Vector.ofFloat16(new short[] {0x3c00, bits}));
		}

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
			() -> Vector.ofFloat32(new float[] {1.0f, 2.0f, Float.NaN}));
		assertTrue(e.getMessage(), e.getMessage().contains("element 2 is NaN"));
	}

	@Test
	public void float32SpecialValuesRoundTrip() {
		float[] data = new float[] {
			-0.0f, 0.0f, Float.MIN_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE
		};
		Vector v = Vector.ofFloat32(data);

		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		Vector parsed = Vector.from(buffer, 0, buffer.length);
		float[] out = parsed.getFloat32Data();

		for (int i = 0; i < data.length; i++) {
			assertEquals(Float.floatToRawIntBits(data[i]), Float.floatToRawIntBits(out[i]));
		}
	}

	@Test
	public void float64SpecialValuesRoundTrip() {
		double[] data = new double[] {
			-0.0, 0.0, Double.MIN_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE
		};
		Vector v = Vector.ofFloat64(data);

		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		Vector parsed = Vector.from(buffer, 0, buffer.length);
		double[] out = parsed.getFloat64Data();

		for (int i = 0; i < data.length; i++) {
			assertEquals(Double.doubleToRawLongBits(data[i]), Double.doubleToRawLongBits(out[i]));
		}
	}

	@Test
	public void equalsUsesBitSemanticsForFloats() {
		// Arrays.equals(float[]) compares bits, so -0.0f != 0.0f even though -0.0f == 0.0f.
		assertNotEquals(Vector.ofFloat32(new float[] {-0.0f}), Vector.ofFloat32(new float[] {0.0f}));
		assertNotEquals(Vector.ofFloat64(new double[] {-0.0}), Vector.ofFloat64(new double[] {0.0}));
	}

	@Test
	public void float16SpecialBitPatternsRoundTrip() {
		short[] data = new short[] {
			(short)0x7bff, // largest finite
			(short)0xfbff, // most negative finite
			(short)0x0000, // +0
			(short)0x8000, // -0
			(short)0x0001  // smallest subnormal
		};
		Vector v = Vector.ofFloat16(data);

		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		Vector parsed = Vector.from(buffer, 0, buffer.length);
		assertArrayEquals(data, parsed.getFloat16Data());
	}

	//-------------------------------------------------------
	// getElementBytes (headerless wire format used by VectorExp)
	//-------------------------------------------------------

	@Test
	public void getElementBytesMatchesWriteToWithoutHeader() {
		assertElementBytes(Vector.ofFloat16(new short[] {0x3c00, (short)0xbc00, 0x4000}));
		assertElementBytes(Vector.ofInt32(new int[] {-1, 0, 1, 12345}));
		assertElementBytes(Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.14159f}));
		assertElementBytes(Vector.ofFloat64(new double[] {1.5, -2.25, 3.14159}));
	}

	private static void assertElementBytes(Vector v) {
		int dataSize = v.dimensions * v.elementType.getByteSize();
		byte[] elements = v.getElementBytes();
		assertEquals(dataSize, elements.length);

		byte[] full = new byte[v.getWireSize()];
		v.writeTo(full, 0);

		byte[] expected = Arrays.copyOfRange(full, Vector.HEADER_SIZE, full.length);
		assertArrayEquals(expected, elements);
	}

	//-------------------------------------------------------
	// from() robustness on malformed input
	//-------------------------------------------------------

	@Test
	public void fromNegativeDimensionsThrowsIllegalArgument() {
		byte[] buffer = new byte[Vector.HEADER_SIZE];
		buffer[0] = Vector.VERSION;
		buffer[1] = ElementType.FLOAT32.getCode();
		// dimensions = -1 (0xFFFFFFFF), little-endian.
		buffer[2] = (byte)0xff;
		buffer[3] = (byte)0xff;
		buffer[4] = (byte)0xff;
		buffer[5] = (byte)0xff;

		try {
			Vector.from(buffer, 0, buffer.length);
			fail("Expected IllegalArgumentException for negative dimensions");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	@Test
	public void fromZeroDimensionsThrowsIllegalArgument() {
		// A bare header is otherwise self-consistent: its length matches zero elements.
		byte[] buffer = new byte[Vector.HEADER_SIZE];
		buffer[0] = Vector.VERSION;
		buffer[1] = ElementType.FLOAT32.getCode();

		assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, 0, buffer.length));
	}

	@Test
	public void fromHugeDimensionsThrowsIllegalArgument() {
		// dimensions * byteSize overflows int and would otherwise trigger a huge allocation.
		byte[] buffer = new byte[Vector.HEADER_SIZE];
		buffer[0] = Vector.VERSION;
		buffer[1] = ElementType.FLOAT32.getCode();
		Buffer.intToLittleBytes(Integer.MAX_VALUE, buffer, 2);

		try {
			Vector.from(buffer, 0, buffer.length);
			fail("Expected IllegalArgumentException for oversized dimensions");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	@Test
	public void fromUnknownVersionThrowsIllegalArgument() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		for (byte version : new byte[] {0, 2, (byte)0xff}) {
			buffer[0] = version;

			try {
				Vector.from(buffer, 0, buffer.length);
				fail("Expected IllegalArgumentException for version " + version);
			}
			catch (IllegalArgumentException e) {
				// expected
			}
		}
	}

	@Test
	public void fromLengthPastBufferThrowsIllegalArgument() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);

		assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, 1, buffer.length));
		assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, -1, buffer.length));
		assertThrows(IllegalArgumentException.class,
			() -> Vector.from(buffer, Integer.MAX_VALUE, buffer.length));
	}

	@Test
	public void wireUnknownVersionThrowsParse() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		byte[] buffer = new byte[v.getWireSize()];
		v.writeTo(buffer, 0);
		buffer[0] = 2;

		AerospikeException ae = assertThrows(AerospikeException.Parse.class,
			() -> Buffer.bytesToVector(buffer, 0, buffer.length));
		assertEquals(ResultCode.PARSE_ERROR, ae.getResultCode());
	}

	@Test
	public void wireUnknownVersionInListThrowsParse() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		byte[] wire = new byte[v.getWireSize()];
		v.writeTo(wire, 0);

		byte[] list = Packer.pack(Collections.singletonList(v));
		int start = indexOf(list, wire);
		list[start] = 2;

		AerospikeException ae = assertThrows(AerospikeException.Parse.class,
			() -> Unpacker.unpackObjectList(list, 0, list.length));
		assertEquals(ResultCode.PARSE_ERROR, ae.getResultCode());
	}

	private static int indexOf(byte[] buffer, byte[] target) {
		for (int i = 0; i <= buffer.length - target.length; i++) {
			if (Arrays.equals(buffer, i, i + target.length, target, 0, target.length)) {
				return i;
			}
		}
		throw new AssertionError("Vector wire bytes not found in packed list");
	}

	//-------------------------------------------------------
	// Record.getVector
	//-------------------------------------------------------

	@Test
	public void recordGetVector() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		Map<String, Object> bins = new HashMap<>();
		bins.put("vecbin", v);
		Record record = new Record(bins, 1, 0);

		assertSame(v, record.getVector("vecbin"));
		assertNull(record.getVector("missing"));
	}

	//-------------------------------------------------------
	// VectorValue.getLuaValue
	//-------------------------------------------------------

	@Test
	public void vectorValueGetLuaValueIsVectorBytes() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		LuaInstance instance = new LuaInstance();

		assertVectorBytes(v, Value.get(v).getLuaValue(instance));
		assertVectorBytes(v, first(instance.getLuaValue(Collections.singletonList(v))));

		byte[] packed = Packer.pack(Collections.singletonList(v));
		assertVectorBytes(v, first(instance.getLuaValue(ParticleType.LIST, packed, 0, packed.length)));
	}

	private static LuaValue first(LuaValue list) {
		return ((LuaList)list).get(LuaInteger.valueOf(1));
	}

	private static void assertVectorBytes(Vector expected, LuaValue value) {
		LuaBytes bytes = (LuaBytes)value;
		assertEquals(ParticleType.VECTOR, bytes.getType());
		assertArrayEquals(expected.getWireBytes(), (byte[])bytes.luaToObject());
	}

	//-------------------------------------------------------
	// Value.get((Object)) for every element type
	//-------------------------------------------------------

	@Test
	public void valueGetObjectAllTypes() {
		assertObjectWrap(Vector.ofFloat16(new short[] {1, 2}));
		assertObjectWrap(Vector.ofInt32(new int[] {1, 2}));
		assertObjectWrap(Vector.ofFloat32(new float[] {1.0f, 2.0f}));
		assertObjectWrap(Vector.ofFloat64(new double[] {1.0, 2.0}));
	}

	private static void assertObjectWrap(Vector v) {
		Value value = Value.get((Object)v);
		assertEquals(VectorValue.class, value.getClass());

		byte[] expected = new byte[v.getWireSize()];
		v.writeTo(expected, 0);

		byte[] actual = new byte[value.estimateSize()];
		int written = value.write(actual, 0);

		assertEquals(expected.length, written);
		assertArrayEquals(expected, actual);
	}

	//-------------------------------------------------------
	// Vector nested inside a map
	//-------------------------------------------------------

	@Test
	public void vectorAsMapValueRoundTrips() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		Map<String, Value> map = Collections.singletonMap("k", Value.get(v));

		Packer packer = new Packer();
		packer.packMap(map);
		packer.createBuffer();
		packer.packMap(map);
		byte[] packed = packer.getBuffer();

		Object unpacked = Unpacker.unpackObjectMap(packed, 0, packed.length);
		Map<?, ?> out = (Map<?, ?>)unpacked;

		assertEquals(1, out.size());
		Object value = out.get("k");
		assertEquals(Vector.class, value.getClass());
		assertEquals(v, value);
	}

	@Test
	public void rawVectorInMapBinPacksSuccessfully() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		Bin bin = new Bin("vecmap", Collections.singletonMap("k", v));

		byte[] buffer = new byte[bin.value.estimateSize()];
		int written = bin.value.write(buffer, 0);

		assertEquals(buffer.length, written);
	}

	@Test
	public void nestedListOfMapOfVectorRoundTrips() {
		Vector v = Vector.ofFloat32(new float[] {1.5f, -2.5f});
		Map<String, Value> inner = Collections.singletonMap("v", Value.get(v));
		List<Object> list = Collections.singletonList(inner);

		byte[] packed = Packer.pack(list);
		Object unpacked = Unpacker.unpackObjectList(packed, 0, packed.length);

		List<?> outList = (List<?>)unpacked;
		Map<?, ?> outMap = (Map<?, ?>)outList.get(0);
		assertEquals(v, outMap.get("v"));
	}
}
