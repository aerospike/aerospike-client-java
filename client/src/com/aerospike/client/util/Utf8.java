/*
 * Copyright (C) 2013 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.util;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;

import com.aerospike.client.AerospikeException;

/**
 * Low-level, high-performance utility methods related to the {@linkplain Charsets#UTF_8 UTF-8}
 * character encoding. UTF-8 is defined in section D92 of <a
 * href="http://www.unicode.org/versions/Unicode6.2.0/ch03.pdf">The Unicode Standard Core
 * Specification, Chapter 3</a>.
 *
 * <p>The variant of UTF-8 implemented by this class is the restricted definition of UTF-8
 * introduced in Unicode 3.1. One implication of this is that it rejects <a
 * href="http://www.unicode.org/versions/corrigendum1.html">"non-shortest form"</a> byte sequences,
 * even though the JDK decoder may accept them.
 *
 * @author Martin Buchholz
 * @author Clément Roux
 * @since 16.0
 */
public final class Utf8 {
	/**
	 * Returns the number of bytes in the UTF-8-encoded form of {@code sequence}. For a string, this
	 * method is equivalent to {@code string.getBytes(UTF_8).length}, but is more efficient in both
	 * time and space.
	 *
	 * @throws IllegalArgumentException	when {@code sequence} contains ill-formed UTF-16 (unpaired
	 *     surrogates)
	 */
	public static int encodedLength(CharSequence sequence) {
		// Warning to maintainers: this implementation is highly optimized.
		int utf16Length = sequence.length();
		int utf8Length = utf16Length;
		int i = 0;

		// This loop optimizes for pure ASCII.
		while (i < utf16Length && sequence.charAt(i) < 0x80) {
			i++;
		}

		// This loop optimizes for chars less than 0x800.
		for (; i < utf16Length; i++) {
			char c = sequence.charAt(i);
			if (c < 0x800) {
				utf8Length += ((0x7f - c) >>> 31); // branch free!
			} else {
				utf8Length += encodedLengthGeneral(sequence, i);
				break;
			}
		}

		if (utf8Length < utf16Length) {
			// Necessary and sufficient condition for overflow because of maximum 3x expansion
			throw new AerospikeException("UTF-8 length does not fit in int: " + (utf8Length + (1L << 32)));
		}
		return utf8Length;
	}

	private static int encodedLengthGeneral(CharSequence sequence, int start) {
		int utf16Length = sequence.length();
		int utf8Length = 0;
		for (int i = start; i < utf16Length; i++) {
			char c = sequence.charAt(i);
			if (c < 0x800) {
				utf8Length += (0x7f - c) >>> 31; // branch free!
			} else {
				utf8Length += 2;
				// jdk7+: if (Character.isSurrogate(c)) {
				if (MIN_SURROGATE <= c && c <= MAX_SURROGATE) {
					// Check that we have a well-formed surrogate pair.
					if (Character.codePointAt(sequence, i) == c) {
						throw new AerospikeException("Unpaired surrogate at index " + i);
					}
					i++;
				}
			}
		}
		return utf8Length;
	}
}
