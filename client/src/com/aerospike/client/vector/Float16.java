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

/**
 * Conversion between float (IEEE 754 binary32) and float16 (IEEE 754 binary16)
 * bit patterns for Java versions before 20, which have no
 * {@code Float.floatToFloat16} / {@code Float.float16ToFloat}.
 */
final class Float16 {
	private static final int FLOAT_SIGN_MASK = 0x80000000;
	private static final int FLOAT_ABS_MASK = 0x7fffffff;
	private static final int FLOAT_MANTISSA_MASK = 0x007fffff;
	private static final int FLOAT_IMPLICIT_BIT = 0x00800000;
	private static final int FLOAT_INFINITY = 0x7f800000;
	private static final int FLOAT_QUIET_NAN_BIT = 0x00400000;
	private static final int FLOAT_EXPONENT_SHIFT = 23;

	private static final int HALF_SIGN_MASK = 0x8000;
	private static final int HALF_INFINITY = 0x7c00;
	private static final int HALF_QUIET_NAN = 0x7e00;
	private static final int HALF_MANTISSA_MASK = 0x03ff;
	private static final int HALF_EXPONENT_SHIFT = 10;

	// The float exponent bias is 127 and the float16 bias is 15.
	private static final int EXPONENT_BIAS_DIFFERENCE = 112;

	// Float exponents (biased) at the float16 range edges: below MIN_NORMAL_EXPONENT the
	// result is subnormal or zero; at or above OVERFLOW_EXPONENT (2^16) it is infinite.
	private static final int MIN_NORMAL_EXPONENT = 113;
	private static final int OVERFLOW_EXPONENT = 143;

	// Float keeps 13 more mantissa bits than float16.
	private static final int EXTRA_MANTISSA_BITS = 13;
	private static final int EXTRA_MANTISSA_HALF = 1 << (EXTRA_MANTISSA_BITS - 1);
	private static final int EXTRA_MANTISSA_MASK = (1 << EXTRA_MANTISSA_BITS) - 1;

	// Float16 subnormals are multiples of 2^-24; a float significand (24 bits with the
	// implicit bit) scaled by 2^(exponent - 126) gives that multiple.
	private static final int SUBNORMAL_SHIFT_BASE = 126;
	private static final int SIGNIFICAND_BITS = 24;

	private Float16() {
	}

	/**
	 * Convert a float to the nearest float16, ties to even, as IEEE 754 round to nearest
	 * requires. Values at or beyond 65520 in magnitude round to infinity. NaN converts to
	 * the quiet NaN with the input's sign.
	 */
	static short fromFloat(float f) {
		int bits = Float.floatToRawIntBits(f);
		int sign = (bits & FLOAT_SIGN_MASK) >>> 16;
		int abs = bits & FLOAT_ABS_MASK;

		if (abs >= FLOAT_INFINITY) {
			return (short)(sign | (abs == FLOAT_INFINITY ? HALF_INFINITY : HALF_QUIET_NAN));
		}

		int exponent = abs >>> FLOAT_EXPONENT_SHIFT;
		int mantissa = abs & FLOAT_MANTISSA_MASK;

		if (exponent >= OVERFLOW_EXPONENT) {
			return (short)(sign | HALF_INFINITY);
		}

		if (exponent >= MIN_NORMAL_EXPONENT) {
			// Normal float16. Dropping the extra mantissa bits truncates; add one when the
			// dropped bits are above half, or exactly half and the kept value is odd. The
			// carry out of a full mantissa correctly bumps the exponent, up to infinity.
			int half = ((exponent - EXPONENT_BIAS_DIFFERENCE) << HALF_EXPONENT_SHIFT) |
				(mantissa >>> EXTRA_MANTISSA_BITS);
			int dropped = mantissa & EXTRA_MANTISSA_MASK;

			if (dropped > EXTRA_MANTISSA_HALF || (dropped == EXTRA_MANTISSA_HALF && (half & 1) != 0)) {
				half++;
			}
			return (short)(sign | half);
		}

		// Subnormal float16 or zero. Float subnormals (exponent 0) have no implicit bit and
		// lie far below the smallest float16 subnormal, so they round to zero below.
		int significand = exponent == 0 ? mantissa : (mantissa | FLOAT_IMPLICIT_BIT);
		int shift = SUBNORMAL_SHIFT_BASE - exponent;

		if (shift > SIGNIFICAND_BITS) {
			return (short)sign;
		}

		int half = significand >>> shift;
		int dropped = significand & ((1 << shift) - 1);
		int roundHalf = 1 << (shift - 1);

		if (dropped > roundHalf || (dropped == roundHalf && (half & 1) != 0)) {
			half++;
		}
		return (short)(sign | half);
	}

	/**
	 * Convert a float16 bit pattern to the float with the same value. Every float16 value,
	 * including subnormals, infinity and NaN, is exactly representable.
	 */
	static float toFloat(short h) {
		int sign = (h & HALF_SIGN_MASK) << 16;
		int exponent = (h & 0x7fff) >>> HALF_EXPONENT_SHIFT;
		int mantissa = h & HALF_MANTISSA_MASK;

		if (exponent == 0x1f) {
			// Infinity when the mantissa is zero. A NaN keeps its payload and is quieted,
			// as float arithmetic would quiet it on first use.
			int quiet = mantissa == 0 ? 0 : FLOAT_QUIET_NAN_BIT;
			return Float.intBitsToFloat(sign | FLOAT_INFINITY | quiet | (mantissa << EXTRA_MANTISSA_BITS));
		}

		if (exponent == 0) {
			// Subnormal float16: mantissa * 2^-24, a normal (or zero) float.
			float value = mantissa * 0x1p-24f;
			return Float.intBitsToFloat(sign | Float.floatToRawIntBits(value));
		}

		return Float.intBitsToFloat(sign | ((exponent + EXPONENT_BIAS_DIFFERENCE) << FLOAT_EXPONENT_SHIFT) |
			(mantissa << EXTRA_MANTISSA_BITS));
	}
}
