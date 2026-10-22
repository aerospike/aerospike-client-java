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
 * 2       4             dimensions    Number of dimensions (at least 1; max depends on element_type).
 * 6       variable      data          Contiguous array of elements.
 * </pre>
 * Multi-byte fields are little-endian. The value length must equal the header size plus
 * {@code dimensions} times the element size.
 */
public final class Vector {
	/**
	 * Current vector format version.
	 */
	public static final byte VERSION = 1;

	private static final int VERSION_SIZE = 1;
	private static final int ELEMENT_TYPE_SIZE = 1;
	private static final int DIMENSIONS_SIZE = 4;

	/**
	 * Size in bytes of the fixed header (version + element_type + dimensions).
	 */
	public static final int HEADER_SIZE = VERSION_SIZE + ELEMENT_TYPE_SIZE + DIMENSIONS_SIZE;

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
		pos += DIMENSIONS_SIZE;

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
	 * Return the complete vector wire value, including the {@link #HEADER_SIZE}-byte header.
	 */
	public byte[] getWireBytes() {
		final byte[] bytes = new byte[getWireSize()];
		writeTo(bytes, 0);
		return bytes;
	}

	/**
	 * Return the raw element array in little-endian wire byte order, without the header.
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
	 * @param length	size in bytes of the vector value; must equal {@link #HEADER_SIZE} plus
	 *                  the element data size given by the header
	 * @throws IllegalArgumentException if the format version is not {@link #VERSION}, the
	 *                                  header has no dimensions, the header or length is
	 *                                  invalid, or offset and length run past the buffer
	 */
	public static Vector from(final byte[] buffer, final int offset, final int length) {
		if (length < HEADER_SIZE) {
			throw new IllegalArgumentException("Invalid vector length: " + length);
		}

		// Subtraction form, so a huge offset or length cannot overflow past the check.
		if (offset < 0 || offset > buffer.length - length) {
			throw new IllegalArgumentException("Vector offset " + offset + " and length " + length +
				" run past a " + buffer.length + " byte buffer");
		}

		int pos = offset;

		final byte version = buffer[pos++];

		if (version != VERSION) {
			throw new IllegalArgumentException("Unsupported vector format version " + version +
				" (expected " + VERSION + ")");
		}

		final ElementType elementType = ElementType.fromCode(buffer[pos++]);
		final int dimensions = checkDimensions(Buffer.littleBytesToInt(buffer, pos));
		pos += DIMENSIONS_SIZE;

		// Use long math so a large dimensions count cannot overflow the int size computation
		// (which could otherwise bypass the bounds check and trigger a huge allocation).
		final long dataSizeLong = (long)dimensions * elementType.getByteSize();

		// Exact, as on the server, so each vector has exactly one valid encoding.
		if (length != HEADER_SIZE + dataSizeLong) {
			throw new IllegalArgumentException("Invalid vector length: " + length +
				", expected " + (HEADER_SIZE + dataSizeLong));
		}

		// Safe to narrow: dataSizeLong == length - HEADER_SIZE, and length is an int.
		final int dataSize = (int)dataSizeLong;
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
	 *
	 * @throws IllegalArgumentException if {@code data} is empty
	 */
	public static Vector ofFloat16(final short[] data) {
		return new Vector(VERSION, ElementType.FLOAT16, checkDimensions(data.length), data.clone());
	}

	/**
	 * Create a vector of int32 elements.
	 *
	 * @throws IllegalArgumentException if {@code data} is empty
	 */
	public static Vector ofInt32(final int[] data) {
		return new Vector(VERSION, ElementType.INT32, checkDimensions(data.length), data.clone());
	}

	/**
	 * Create a vector of float (fp32) elements.
	 *
	 * @throws IllegalArgumentException if {@code data} is empty
	 */
	public static Vector ofFloat32(final float[] data) {
		return new Vector(VERSION, ElementType.FLOAT32, checkDimensions(data.length), data.clone());
	}

	/**
	 * Create a vector of double (fp64) elements.
	 *
	 * @throws IllegalArgumentException if {@code data} is empty
	 */
	public static Vector ofFloat64(final double[] data) {
		return new Vector(VERSION, ElementType.FLOAT64, checkDimensions(data.length), data.clone());
	}

	private static int checkDimensions(final int dimensions) {
		if (dimensions < 1) {
			throw new IllegalArgumentException("Vector must have at least 1 dimension: " + dimensions);
		}
		return dimensions;
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
