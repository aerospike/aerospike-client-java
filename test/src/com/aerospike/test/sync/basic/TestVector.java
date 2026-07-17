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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.VectorValue;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Packer;
import com.aerospike.client.vector.Vector;
import com.aerospike.client.vector.Vector.ElementType;
import com.aerospike.test.util.TestBase;

public class TestVector extends TestBase {
	//-------------------------------------------------------
	// ElementType
	//-------------------------------------------------------

	@Test
	public void elementTypeCodes() {
		assertEquals(0x01, ElementType.FLOAT16.getCode());
		assertEquals(0x02, ElementType.INT32.getCode());
		assertEquals(0x03, ElementType.FLOAT32.getCode());
		assertEquals(0x04, ElementType.FLOAT64.getCode());
	}

	@Test
	public void elementTypeByteSizes() {
		assertEquals(2, ElementType.FLOAT16.getByteSize());
		assertEquals(4, ElementType.INT32.getByteSize());
		assertEquals(4, ElementType.FLOAT32.getByteSize());
		assertEquals(8, ElementType.FLOAT64.getByteSize());
	}

	@Test
	public void elementTypeFromCode() {
		for (ElementType type : ElementType.values()) {
			assertSame(type, ElementType.fromCode(type.getCode()));
		}
	}

	@Test
	public void elementTypeFromInvalidCode() {
		try {
			ElementType.fromCode((byte)0x7f);
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	//-------------------------------------------------------
	// Vector construction and accessors
	//-------------------------------------------------------

	@Test
	public void constructFloat16() {
		short[] data = new short[] {0x3c00, (short)0xbc00, 0x4000};
		Vector v = Vector.ofFloat16(data);

		assertEquals(Vector.VERSION, v.version);
		assertSame(ElementType.FLOAT16, v.elementType);
		assertEquals(3, v.dimensions);
		assertArrayEquals(data, v.getFloat16Data());
	}

	@Test
	public void constructInt32() {
		int[] data = new int[] {-1, 0, 1, Integer.MAX_VALUE};
		Vector v = Vector.ofInt32(data);

		assertEquals(Vector.VERSION, v.version);
		assertSame(ElementType.INT32, v.elementType);
		assertEquals(4, v.dimensions);
		assertArrayEquals(data, v.getInt32Data());
	}

	@Test
	public void constructFloat32() {
		float[] data = new float[] {1.5f, -2.25f, 0.0f, 3.14159f, Float.MAX_VALUE};
		Vector v = Vector.ofFloat32(data);

		assertEquals(Vector.VERSION, v.version);
		assertSame(ElementType.FLOAT32, v.elementType);
		assertEquals(5, v.dimensions);
		assertArrayEquals(data, v.getFloat32Data(), 0.0f);
	}

	@Test
	public void constructFloat64() {
		double[] data = new double[] {1.5, -2.25, Double.MAX_VALUE};
		Vector v = Vector.ofFloat64(data);

		assertEquals(Vector.VERSION, v.version);
		assertSame(ElementType.FLOAT64, v.elementType);
		assertEquals(3, v.dimensions);
		assertArrayEquals(data, v.getFloat64Data(), 0.0);
	}

	//-------------------------------------------------------
	// Immutability (defensive copies)
	//-------------------------------------------------------

	@Test
	public void constructorCopiesInput() {
		float[] data = new float[] {1.0f, 2.0f, 3.0f};
		Vector v = Vector.ofFloat32(data);

		data[0] = 99.0f;

		assertEquals(1.0f, v.getFloat32Data()[0], 0.0f);
	}

	@Test
	public void getterReturnsCopy() {
		Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

		float[] first = v.getFloat32Data();
		first[0] = 99.0f;

		assertEquals(1.0f, v.getFloat32Data()[0], 0.0f);
	}

	@Test
	public void wrongTypeGetterThrows() {
		Vector v = Vector.ofFloat32(new float[] {1.0f});

		try {
			v.getInt32Data();
			fail("Expected IllegalStateException");
		}
		catch (IllegalStateException e) {
			// expected
		}
	}

	//-------------------------------------------------------
	// Wire size
	//-------------------------------------------------------

	@Test
	public void wireSize() {
		assertEquals(8 + 3 * 2, Vector.ofFloat16(new short[3]).getWireSize());
		assertEquals(8 + 4 * 4, Vector.ofInt32(new int[4]).getWireSize());
		assertEquals(8 + 5 * 4, Vector.ofFloat32(new float[5]).getWireSize());
		assertEquals(8 + 2 * 8, Vector.ofFloat64(new double[2]).getWireSize());
	}

	//-------------------------------------------------------
	// equals / hashCode / toString
	//-------------------------------------------------------

	@Test
	public void equalsAndHashCode() {
		Vector a = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
		Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

		assertEquals(a, b);
		assertEquals(a.hashCode(), b.hashCode());
	}

	@Test
	public void notEqualsDifferentData() {
		Vector a = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
		Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 4.0f});

		assertNotEquals(a, b);
	}

	@Test
	public void notEqualsDifferentType() {
		Vector a = Vector.ofInt32(new int[] {1, 2, 3});
		Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

		assertNotEquals(a, b);
	}

	@Test
	public void toStringContainsData() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		assertEquals("[1, 2, 3]", v.toString());
	}

	//-------------------------------------------------------
	// writeTo (wire format)
	//-------------------------------------------------------

	@Test
	public void writeToFloat32() {
		float[] data = new float[] {1.5f, -2.25f, 3.14159f};
		Vector v = Vector.ofFloat32(data);

		byte[] buffer = new byte[v.getWireSize()];
		int written = v.writeTo(buffer, 0);

		assertEquals(v.getWireSize(), written);
		assertHeader(buffer, ElementType.FLOAT32, data.length);

		for (int i = 0; i < data.length; i++) {
			int bits = decodeIntLE(buffer, 8 + i * 4);
			assertEquals(data[i], Float.intBitsToFloat(bits), 0.0f);
		}
	}

	@Test
	public void writeToFloat64() {
		double[] data = new double[] {1.5, -2.25, 3.14159};
		Vector v = Vector.ofFloat64(data);

		byte[] buffer = new byte[v.getWireSize()];
		int written = v.writeTo(buffer, 0);

		assertEquals(v.getWireSize(), written);
		assertHeader(buffer, ElementType.FLOAT64, data.length);

		for (int i = 0; i < data.length; i++) {
			long bits = decodeLongLE(buffer, 8 + i * 8);
			assertEquals(data[i], Double.longBitsToDouble(bits), 0.0);
		}
	}

	@Test
	public void writeToInt32() {
		int[] data = new int[] {-1, 0, 1, 12345};
		Vector v = Vector.ofInt32(data);

		byte[] buffer = new byte[v.getWireSize()];
		int written = v.writeTo(buffer, 0);

		assertEquals(v.getWireSize(), written);
		assertHeader(buffer, ElementType.INT32, data.length);

		for (int i = 0; i < data.length; i++) {
			assertEquals(data[i], decodeIntLE(buffer, 8 + i * 4));
		}
	}

	@Test
	public void writeToFloat16() {
		short[] data = new short[] {0x3c00, (short)0xbc00, 0x4000};
		Vector v = Vector.ofFloat16(data);

		byte[] buffer = new byte[v.getWireSize()];
		int written = v.writeTo(buffer, 0);

		assertEquals(v.getWireSize(), written);
		assertHeader(buffer, ElementType.FLOAT16, data.length);

		for (int i = 0; i < data.length; i++) {
			assertEquals(data[i], decodeShortLE(buffer, 8 + i * 2));
		}
	}

	@Test
	public void writeToRespectsOffset() {
		Vector v = Vector.ofInt32(new int[] {7, 8});

		byte[] buffer = new byte[4 + v.getWireSize()];
		int written = v.writeTo(buffer, 4);

		assertEquals(v.getWireSize(), written);
		// Leading bytes untouched.
		assertEquals(0, buffer[0]);
		assertEquals(0, buffer[1]);
		assertEquals(0, buffer[2]);
		assertEquals(0, buffer[3]);
		assertEquals(Vector.VERSION, buffer[4]);
	}

	//-------------------------------------------------------
	// VectorValue
	//-------------------------------------------------------

	@Test
	public void getAsVector() {
		Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f});
		Value value = Value.getAsVector(v);

		assertEquals(ParticleType.VECTOR, value.getType());
		assertSame(v, value.getObject());
		assertSame(v, ((VectorValue)value).getVector());
	}

	@Test
	public void getAsVectorNull() {
		assertSame(Value.getAsNull(), Value.getAsVector(null));
	}

	@Test
	public void valueEstimateSizeMatchesWireSize() {
		Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
		Value value = Value.getAsVector(v);

		assertEquals(v.getWireSize(), value.estimateSize());
	}

	@Test
	public void valueWriteMatchesVectorWriteTo() {
		Vector v = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
		Value value = Value.getAsVector(v);

		byte[] expected = new byte[v.getWireSize()];
		v.writeTo(expected, 0);

		byte[] actual = new byte[value.estimateSize()];
		int written = value.write(actual, 0);

		assertEquals(expected.length, written);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void valueEquals() {
		Value a = Value.getAsVector(Vector.ofInt32(new int[] {1, 2, 3}));
		Value b = Value.getAsVector(Vector.ofInt32(new int[] {1, 2, 3}));

		assertEquals(a, b);
		assertEquals(a.hashCode(), b.hashCode());
	}

	@Test
	public void validateKeyTypeThrows() {
		Value value = Value.getAsVector(Vector.ofInt32(new int[] {1, 2, 3}));

		try {
			value.validateKeyType();
			fail("Expected AerospikeException");
		}
		catch (AerospikeException e) {
			assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
		}
	}

	@Test
	public void packProducesParticleBytes() {
		Vector v = Vector.ofInt32(new int[] {1, 2, 3});
		Value value = Value.getAsVector(v);

		// Two-pass pack: size estimate then write.
		Packer packer = new Packer();
		value.pack(packer);
		packer.createBuffer();
		value.pack(packer);
		byte[] packed = packer.getBuffer();

		// Packed blob = msgpack byte-array header + particle type byte + wire bytes.
		// The trailing bytes must contain the particle type followed by the vector wire format.
		byte[] wire = new byte[v.getWireSize()];
		v.writeTo(wire, 0);

		int payloadStart = packed.length - wire.length;
		assertEquals(ParticleType.VECTOR, packed[payloadStart - 1] & 0xff);

		byte[] payload = new byte[wire.length];
		System.arraycopy(packed, payloadStart, payload, 0, wire.length);
		assertArrayEquals(wire, payload);
	}

	//-------------------------------------------------------
	// Helpers
	//-------------------------------------------------------

	private static void assertHeader(byte[] buffer, ElementType type, int dimensions) {
		assertEquals(Vector.VERSION, buffer[0]);
		assertEquals(type.getCode(), buffer[1]);
		assertEquals(dimensions, decodeIntLE(buffer, 2));
		assertEquals(0, buffer[6]);
		assertEquals(0, buffer[7]);
	}

	// Vector wire format is little-endian to match the server.
	private static int decodeIntLE(byte[] b, int offset) {
		return (b[offset] & 0xff) |
			((b[offset + 1] & 0xff) << 8) |
			((b[offset + 2] & 0xff) << 16) |
			((b[offset + 3] & 0xff) << 24);
	}

	private static long decodeLongLE(byte[] b, int offset) {
		long result = 0;
		for (int i = 7; i >= 0; i--) {
			result = (result << 8) | (b[offset + i] & 0xff);
		}
		return result;
	}

	private static short decodeShortLE(byte[] b, int offset) {
		return (short)((b[offset] & 0xff) | ((b[offset + 1] & 0xff) << 8));
	}
}
