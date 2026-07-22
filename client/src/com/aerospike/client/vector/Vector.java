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
package com.aerospike.client.vector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.aerospike.client.command.Buffer;

/**
 * Vector of numeric elements, used for vector similarity search. A vector is
 * defined by the following wire format:
 * <pre>
 * Offset  Size (bytes)  Field         Description
 * 0       1             version       The version of the vector format.
 * 1       1             element_type  Enum identifying the element type (see {@link ElementType}).
 * 2       4             dimensions    Number of dimensions (max depends on element_type).
 * 6       2             reserved      Reserved for future use &amp; padding (8-byte alignment).
 * 8       variable      data          Contiguous array of elements.
 * </pre>
 */
public final class Vector {
	/**
	 * Current vector format version.
	 */
	public static final byte VERSION = 1;

	/**
	 * Size in bytes of the fixed header (version + element_type + dimensions + reserved).
	 */
	public static final int HEADER_SIZE = 8;

	/**
	 * Vector element type.
	 */
	public enum ElementType {
		/**
		 * float16: high-density vectors (IEEE 754 half).
		 */
		FLOAT16((byte)0x01, 2),

		/**
		 * int32: integer-based embeddings.
		 */
		INT32((byte)0x02, 4),

		/**
		 * float (fp32): standard FP32.
		 */
		FLOAT32((byte)0x03, 4),

		/**
		 * double (fp64): high-precision FP64.
		 */
		FLOAT64((byte)0x04, 8);

		private final byte code;
		private final int byteSize;

		ElementType(final byte code, final int byteSize) {
			this.code = code;
			this.byteSize = byteSize;
		}

		/**
		 * Return wire protocol code for this element type.
		 */
		public byte getCode() {
			return code;
		}

		/**
		 * Return the number of bytes used to encode a single element of this type.
		 */
		public int getByteSize() {
			return byteSize;
		}

		/**
		 * Lookup element type from its wire protocol code.
		 */
		public static ElementType fromCode(final byte code) {
			for (final ElementType type : values()) {
				if (type.code == code) {
					return type;
				}
			}
			throw new IllegalArgumentException("Unknown vector element type code: " + code);
		}
	}

	/**
	 * Vector format version.
	 */
	public final byte version;

	/**
	 * Vector element type.
	 */
	public final ElementType elementType;

	/**
	 * Number of dimensions (elements) in this vector.
	 */
	public final int dimensions;

	private final Object data;

	private Integer wireSize;
	private Integer hash;

	private Vector(final byte version, final ElementType elementType, final int dimensions, final Object data) {
		this.version = version;
		this.elementType = elementType;
		this.dimensions = dimensions;
		this.data = data;
	}

	/**
	 * Return the number of bytes needed to serialize this vector in wire format
	 * (header plus element data). Computed lazily and cached on first call.
	 */
	public int getWireSize() {
		if (wireSize == null) {
			wireSize = HEADER_SIZE + (dimensions * elementType.getByteSize());
		}
		return wireSize;
	}

	/**
	 * Serialize this vector into the wire format at the given buffer offset.
	 * Operates directly on the internal data array (no defensive copy) since
	 * this is used on the record write hot path.
	 *
	 * @return number of bytes written, equal to {@link #getWireSize()}
	 */
	public int writeTo(final byte[] buffer, final int offset) {
		int pos = offset;

		// Vector wire format is little-endian to match the server.
		buffer[pos++] = version;
		buffer[pos++] = elementType.getCode();
		Buffer.intToLittleBytes(dimensions, buffer, pos);
		pos += 4;
		buffer[pos++] = 0; // reserved
		buffer[pos++] = 0; // reserved

		final int dataSize = dimensions * elementType.getByteSize();
		final ByteBuffer view = ByteBuffer.wrap(buffer, pos, dataSize).order(ByteOrder.LITTLE_ENDIAN);

		switch (elementType) {
			case FLOAT16:
				view.asShortBuffer().put((short[])data);
				break;

			case INT32:
				view.asIntBuffer().put((int[])data);
				break;

			case FLOAT32:
				view.asFloatBuffer().put((float[])data);
				break;

			case FLOAT64:
				view.asDoubleBuffer().put((double[])data);
				break;

			default:
				throw new IllegalStateException("Unsupported vector element type: " + elementType);
		}
		pos += dataSize;

		return pos - offset;
	}

	/**
	 * Return the raw element array in little-endian wire byte order, without the
	 * 8-byte header. This is the layout expected as the query vector of a vector
	 * distance expression (see {@link com.aerospike.client.exp.VectorExp#distance}),
	 * where the server reinterprets the bytes using the stored bin's element type.
	 * <p>
	 * TODO(vector-exp-envelope): the server team's frozen contract for the vector
	 * distance expression's query-vector argument has been decided to move to
	 * sending the complete vector wire value (header + elements, i.e. what
	 * {@link #writeTo} produces) rather than headerless elements. The server side
	 * of that change has not shipped yet, so this method (and the elements-only
	 * wire format {@link VectorExp#distance} currently sends) still matches the
	 * server behavior available today. Once the server ships the new envelope,
	 * {@link VectorExp#distance} should be updated to pack the full vector value
	 * instead of calling this method.
	 */
	public byte[] getElementBytes() {
		final int dataSize = dimensions * elementType.getByteSize();
		final byte[] bytes = new byte[dataSize];
		final ByteBuffer view = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

		switch (elementType) {
			case FLOAT16:
				view.asShortBuffer().put((short[])data);
				break;

			case INT32:
				view.asIntBuffer().put((int[])data);
				break;

			case FLOAT32:
				view.asFloatBuffer().put((float[])data);
				break;

			case FLOAT64:
				view.asDoubleBuffer().put((double[])data);
				break;

			default:
				throw new IllegalStateException("Unsupported vector element type: " + elementType);
		}

		return bytes;
	}

	/**
	 * Deserialize a vector from wire format at the given buffer offset.
	 *
	 * @param buffer	buffer containing the vector wire format
	 * @param offset	offset in buffer where the vector starts
	 * @param length	number of bytes available for this vector (must be at least
	 *                  {@link #HEADER_SIZE})
	 */
	public static Vector from(final byte[] buffer, final int offset, final int length) {
		if (length < HEADER_SIZE) {
			throw new IllegalArgumentException("Invalid vector length: " + length);
		}

		int pos = offset;

		final byte version = buffer[pos++];
		final ElementType elementType = ElementType.fromCode(buffer[pos++]);
		final int dimensions = Buffer.littleBytesToInt(buffer, pos);
		pos += 4;
		pos += 2; // reserved

		final int dataSize = dimensions * elementType.getByteSize();

		if (length < HEADER_SIZE + dataSize) {
			throw new IllegalArgumentException("Invalid vector length: " + length +
				", expected at least " + (HEADER_SIZE + dataSize));
		}

		final ByteBuffer view = ByteBuffer.wrap(buffer, pos, dataSize).order(ByteOrder.LITTLE_ENDIAN);
		final Object data;

		switch (elementType) {
			case FLOAT16: {
				final short[] arr = new short[dimensions];
				view.asShortBuffer().get(arr);
				data = arr;
				break;
			}

			case INT32: {
				final int[] arr = new int[dimensions];
				view.asIntBuffer().get(arr);
				data = arr;
				break;
			}

			case FLOAT32: {
				final float[] arr = new float[dimensions];
				view.asFloatBuffer().get(arr);
				data = arr;
				break;
			}

			case FLOAT64: {
				final double[] arr = new double[dimensions];
				view.asDoubleBuffer().get(arr);
				data = arr;
				break;
			}

			default:
				throw new IllegalStateException("Unsupported vector element type: " + elementType);
		}

		return new Vector(version, elementType, dimensions, data);
	}

	/**
	 * Create a vector of raw float16 (IEEE 754 half precision) elements.
	 * Since Java has no native float16 type, each element is passed as its
	 * raw 16-bit bit pattern.
	 */
	public static Vector ofFloat16(final short[] data) {
		return new Vector(VERSION, ElementType.FLOAT16, data.length, data.clone());
	}

	/**
	 * Create a vector of int32 elements.
	 */
	public static Vector ofInt32(final int[] data) {
		return new Vector(VERSION, ElementType.INT32, data.length, data.clone());
	}

	/**
	 * Create a vector of float (fp32) elements.
	 */
	public static Vector ofFloat32(final float[] data) {
		return new Vector(VERSION, ElementType.FLOAT32, data.length, data.clone());
	}

	/**
	 * Create a vector of double (fp64) elements.
	 */
	public static Vector ofFloat64(final double[] data) {
		return new Vector(VERSION, ElementType.FLOAT64, data.length, data.clone());
	}

	/**
	 * Return raw float16 data. Throws {@link IllegalStateException} if this
	 * vector's element type is not {@link ElementType#FLOAT16}.
	 */
	public short[] getFloat16Data() {
		validateType(ElementType.FLOAT16);
		return ((short[])data).clone();
	}

	/**
	 * Return int32 data. Throws {@link IllegalStateException} if this
	 * vector's element type is not {@link ElementType#INT32}.
	 */
	public int[] getInt32Data() {
		validateType(ElementType.INT32);
		return ((int[])data).clone();
	}

	/**
	 * Return float (fp32) data. Throws {@link IllegalStateException} if this
	 * vector's element type is not {@link ElementType#FLOAT32}.
	 */
	public float[] getFloat32Data() {
		validateType(ElementType.FLOAT32);
		return ((float[])data).clone();
	}

	/**
	 * Return double (fp64) data. Throws {@link IllegalStateException} if this
	 * vector's element type is not {@link ElementType#FLOAT64}.
	 */
	public double[] getFloat64Data() {
		validateType(ElementType.FLOAT64);
		return ((double[])data).clone();
	}

	private void validateType(final ElementType expected) {
		if (elementType != expected) {
			throw new IllegalStateException(
				"Vector element type is " + elementType + ", not " + expected);
		}
	}

	@Override
	public String toString() {
		switch (elementType) {
			case FLOAT16:
				return Arrays.toString((short[])data);
			case INT32:
				return Arrays.toString((int[])data);
			case FLOAT32:
				return Arrays.toString((float[])data);
			case FLOAT64:
				return Arrays.toString((double[])data);
			default:
				return data.toString();
		}
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Vector)) {
			return false;
		}

		final Vector other = (Vector)obj;

		if (version != other.version || elementType != other.elementType || dimensions != other.dimensions) {
			return false;
		}

		switch (elementType) {
			case FLOAT16:
				return Arrays.equals((short[])data, (short[])other.data);
			case INT32:
				return Arrays.equals((int[])data, (int[])other.data);
			case FLOAT32:
				return Arrays.equals((float[])data, (float[])other.data);
			case FLOAT64:
				return Arrays.equals((double[])data, (double[])other.data);
			default:
				return data.equals(other.data);
		}
	}

	@Override
	public int hashCode() {
		if (hash == null) {
			int h = version;
			h = 31 * h + elementType.hashCode();
			h = 31 * h + dimensions;

			switch (elementType) {
				case FLOAT16:
					h = 31 * h + Arrays.hashCode((short[])data);
					break;
				case INT32:
					h = 31 * h + Arrays.hashCode((int[])data);
					break;
				case FLOAT32:
					h = 31 * h + Arrays.hashCode((float[])data);
					break;
				case FLOAT64:
					h = 31 * h + Arrays.hashCode((double[])data);
					break;
				default:
					h = 31 * h + data.hashCode();
					break;
			}
			hash = h;
		}
		return hash;
	}
}
