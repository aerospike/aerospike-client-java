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
package com.aerospike.client.util;

import java.util.Random;

/**
 * Generate pseudo random numbers using xorshift128+ algorithm.
 * This class is not thread-safe and should be instantiated once per thread.
 */
public final class RandomShift {
	private static final ThreadLocal<RandomShift> ThreadLocalInstance = new ThreadLocal<RandomShift>() {
		@Override protected RandomShift initialValue() {
			return new RandomShift();
		}
	};

	/**
	 * Get thread local instance of RandomShift.
	 */
	public static RandomShift instance() {
		return ThreadLocalInstance.get();
	}

	private long seed0;
	private long seed1;

	/**
	 * Generate seeds using standard Random class.
	 */
	public RandomShift() {
		// Use default constructor which uses a different seed for each invocation.
		// Do not use System.currentTimeMillis() for a seed because it is often
		// the same across concurrent threads, thus causing hot keys.
		Random random = new Random();
		seed0 = random.nextLong();
		seed1 = random.nextLong();
	}

	/**
	 * Generate a random string of given size.
	 */
	public String nextString(int size) {
		char[] chars = new char[size];
		int r;

		for (int i = 0; i < size; i++) {
			r = nextInt(62);  // Lower alpha + Upper alpha + digit

			if (r < 26) {
				// Lower case
				chars[i] = (char)(r + 97);
			}
			else if (r < 52) {
				// Upper case
				chars[i] = (char)(r - 26 + 65);
			}
			else {
				// Digit
				chars[i] = (char)(r - 52 + 48);
			}
		}
		return new String(chars);
	}

	/**
	 * Generate random bytes.
	 */
	public void nextBytes(byte[] bytes) {
		int len = bytes.length;
		int i = 0;

		while (i < len) {
			long r = nextLong();
			int n = Math.min(len - i, 8);

			for (int j = 0; j < n; j++) {
                bytes[i++] = (byte)r;
				r >>= 8;
			}
		}
	}

	/**
	 * Generate random integer between 0 (inclusive) and the specified value (exclusive).
	 */
	public int nextInt(int n) {
		return (((int)nextLong()) & Integer.MAX_VALUE) % n;
	}

	/**
	 * Generate random integer which can be negative.
	 */
	public int nextInt() {
		return (int)nextLong();
	}

	/**
	 * Generate random integer between 0 (inclusive) and the specified value (exclusive).
	 */
	public long nextLong(long n) {
		return (nextLong() & Long.MAX_VALUE) % n;
	}

	/**
	 * Generate random long value which can be negative.
	 */
	public long nextLong() {
		long s1 = seed0;
		final long s0 = seed1;
		seed0 = s0;
		s1 ^= s1 << 23;
		seed1 = (s1 ^ s0 ^ (s1 >>> 18) ^ (s0 >>> 5));
		return seed1 + s0;
	}
}
