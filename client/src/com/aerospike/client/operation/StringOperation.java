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
package com.aerospike.client.operation;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * String operations. Create string operations used by the client operate command.
 * <p>
 * Index orientation is left-to-right with codepoint addressing. Negative indexes
 * count from the end of the string ({@code -1} = last character). Out-of-bounds
 * indexes are clamped to the valid range; no error is returned.
 * <p>
 * String operations require server version 8.1.3 or later. Operations on string
 * items nested in lists/maps are not currently supported by the server.
 */
public final class StringOperation {
	// Read ops
	private static final int STRLEN = 0;
	private static final int SUBSTR = 1;
	private static final int CHAR_AT = 2;
	private static final int FIND = 3;
	private static final int CONTAINS = 4;
	private static final int STARTS_WITH = 5;
	private static final int ENDS_WITH = 6;
	private static final int TO_INTEGER = 7;
	private static final int TO_DOUBLE = 8;
	private static final int BYTE_LENGTH = 9;
	private static final int IS_NUMERIC = 10;
	private static final int IS_UPPER = 11;
	private static final int IS_LOWER = 12;
	private static final int TO_BLOB = 13;
	private static final int SPLIT = 14;
	private static final int B64_DECODE = 15;
	private static final int REGEX_COMPARE = 16;

	// Modify ops
	private static final int INSERT = 50;
	private static final int OVERWRITE = 51;
	private static final int CONCAT = 52;
	private static final int SNIP = 53;
	private static final int REPLACE = 54;
	private static final int REPLACE_ALL = 55;
	private static final int UPPER = 56;
	private static final int LOWER = 57;
	private static final int CASE_FOLD = 58;
	private static final int NORMALIZE_NFC = 59;
	private static final int TRIM_START = 60;
	private static final int TRIM_END = 61;
	private static final int TRIM = 62;
	private static final int PAD_START = 63;
	private static final int PAD_END = 64;
	private static final int REPEAT = 65;
	private static final int REGEX_REPLACE = 66;

	//-----------------------------------------------------------------
	// Read operations
	//-----------------------------------------------------------------

	/**
	 * Create string {@code strlen} operation.
	 * Server returns the number of unicode codepoints in the string bin (int64).
	 */
	public static Operation strlen(String binName) {
		byte[] bytes = Pack.pack(STRLEN);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code substr} operation that reads from {@code start} to the end of the string.
	 * Negative indexes count from the end.
	 */
	public static Operation substr(String binName, int start) {
		byte[] bytes = Pack.pack(SUBSTR, start);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code substr} operation that reads {@code length} codepoints starting at {@code start}.
	 * Negative indexes count from the end of the string.
	 */
	public static Operation substr(String binName, int start, int length) {
		byte[] bytes = Pack.pack(SUBSTR, start, length);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code charAt} operation. Server returns the character at {@code index} as a string.
	 * Negative indexes count from the end of the string.
	 */
	public static Operation charAt(String binName, int index) {
		byte[] bytes = Pack.pack(CHAR_AT, index);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code find} operation. Server returns the codepoint index of the first
	 * occurrence of {@code needle}, or -1 if not found.
	 */
	public static Operation find(String binName, String needle) {
		byte[] bytes = Pack.pack(FIND, Value.get(needle));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code find} operation. Server returns the codepoint index of the
	 * {@code occurrence}-th match of {@code needle} (1 = first match), or -1 if not found.
	 */
	public static Operation find(String binName, String needle, int occurrence) {
		byte[] bytes = packCmdValueInt(FIND, Value.get(needle), occurrence);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code contains} operation. Server returns 1 if the bin contains
	 * {@code needle} as a substring, 0 otherwise.
	 */
	public static Operation contains(String binName, String needle) {
		byte[] bytes = Pack.pack(CONTAINS, Value.get(needle));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code startsWith} operation. Server returns 1 if the bin begins with
	 * {@code prefix}, 0 otherwise.
	 */
	public static Operation startsWith(String binName, String prefix) {
		byte[] bytes = Pack.pack(STARTS_WITH, Value.get(prefix));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code endsWith} operation. Server returns 1 if the bin ends with
	 * {@code suffix}, 0 otherwise.
	 */
	public static Operation endsWith(String binName, String suffix) {
		byte[] bytes = Pack.pack(ENDS_WITH, Value.get(suffix));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toInteger} operation. Server parses the string as an int64.
	 * Returns AEROSPIKE_ERR_PARAMETER if the bin cannot be parsed as an integer.
	 */
	public static Operation toInteger(String binName) {
		byte[] bytes = Pack.pack(TO_INTEGER);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toDouble} operation. Server parses the string as a 64-bit float.
	 * Returns AEROSPIKE_ERR_PARAMETER if the bin cannot be parsed as a double.
	 */
	public static Operation toDouble(String binName) {
		byte[] bytes = Pack.pack(TO_DOUBLE);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code byteLength} operation. Server returns the UTF-8 byte length
	 * of the string (int64).
	 */
	public static Operation byteLength(String binName) {
		byte[] bytes = Pack.pack(BYTE_LENGTH);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isNumeric} operation. Server returns 1 if the bin contains a valid
	 * integer or float, 0 otherwise.
	 */
	public static Operation isNumeric(String binName) {
		byte[] bytes = Pack.pack(IS_NUMERIC);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isNumeric} operation that filters by {@code numericType}
	 * (see {@link StringNumericType}).
	 */
	public static Operation isNumeric(String binName, int numericType) {
		byte[] bytes = Pack.pack(IS_NUMERIC, numericType);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isUpper} operation. Server returns 1 if every cased character
	 * in the bin is uppercase, 0 otherwise.
	 */
	public static Operation isUpper(String binName) {
		byte[] bytes = Pack.pack(IS_UPPER);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isLower} operation. Server returns 1 if every cased character
	 * in the bin is lowercase, 0 otherwise.
	 */
	public static Operation isLower(String binName) {
		byte[] bytes = Pack.pack(IS_LOWER);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toBlob} operation. Server returns the UTF-8 bytes of the string
	 * as a blob.
	 */
	public static Operation toBlob(String binName) {
		byte[] bytes = Pack.pack(TO_BLOB);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code split} operation that splits by Unicode codepoint
	 * (each codepoint becomes its own list element).
	 */
	public static Operation split(String binName) {
		byte[] bytes = Pack.pack(SPLIT);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code split} operation that splits by {@code separator}.
	 * Server returns a list of strings.
	 */
	public static Operation split(String binName, String separator) {
		byte[] bytes = Pack.pack(SPLIT, Value.get(separator));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code b64Decode} operation. Server base64-decodes the string and
	 * returns a blob.
	 */
	public static Operation b64Decode(String binName) {
		byte[] bytes = Pack.pack(B64_DECODE);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexCompare} operation. Server matches {@code pattern} (ICU
	 * regex syntax) against the bin and returns 1 on match, 0 otherwise.
	 */
	public static Operation regexCompare(String binName, String pattern) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, Value.get(pattern));
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexCompare} operation with {@link StringRegexFlags}.
	 */
	public static Operation regexCompare(String binName, String pattern, int regexFlags) {
		byte[] bytes = packCmdValueInt(REGEX_COMPARE, Value.get(pattern), regexFlags);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	//-----------------------------------------------------------------
	// Modify operations
	//-----------------------------------------------------------------

	/**
	 * Create string {@code insert} operation that inserts {@code value} at codepoint
	 * {@code index}. Negative indexes count from the end of the string.
	 */
	public static Operation insert(StringPolicy policy, String binName, int index, String value) {
		byte[] bytes = Pack.pack(INSERT, index, Value.get(value), policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code overwrite} operation that overwrites characters starting at
	 * codepoint {@code index} with {@code value}.
	 */
	public static Operation overwrite(StringPolicy policy, String binName, int index, String value) {
		byte[] bytes = Pack.pack(OVERWRITE, index, Value.get(value), policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code concat} operation that appends {@code value} to the bin.
	 */
	public static Operation concat(StringPolicy policy, String binName, String value) {
		List<Value> list = new ArrayList<Value>(1);
		list.add(Value.get(value));
		byte[] bytes = Pack.pack(CONCAT, list, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code concat} operation that appends every element of {@code values}
	 * to the bin in order.
	 */
	public static Operation concat(StringPolicy policy, String binName, List<String> values) {
		List<Value> list = toValueList(values);
		byte[] bytes = Pack.pack(CONCAT, list, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code snip} operation that removes characters starting at codepoint
	 * {@code start} through the end of the string.
	 */
	public static Operation snip(StringPolicy policy, String binName, int start) {
		byte[] bytes = Pack.pack(SNIP, start, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code snip} operation that removes characters from codepoint
	 * {@code start} (inclusive) to {@code end} (exclusive).
	 */
	public static Operation snip(StringPolicy policy, String binName, int start, int end) {
		byte[] bytes = Pack.pack(SNIP, start, end, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code replace} operation that replaces the first occurrence of
	 * {@code needle} with {@code replacement}.
	 */
	public static Operation replace(StringPolicy policy, String binName, String needle, String replacement) {
		List<Value> list = pair(needle, replacement);
		byte[] bytes = Pack.pack(REPLACE, list, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code replaceAll} operation that replaces every occurrence of
	 * {@code needle} with {@code replacement}.
	 */
	public static Operation replaceAll(StringPolicy policy, String binName, String needle, String replacement) {
		List<Value> list = pair(needle, replacement);
		byte[] bytes = Pack.pack(REPLACE_ALL, list, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code upper} operation that uppercases the bin in place.
	 */
	public static Operation upper(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(UPPER, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code lower} operation that lowercases the bin in place.
	 */
	public static Operation lower(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(LOWER, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code caseFold} operation. Server applies a locale-independent case
	 * fold (lowercase) to the bin.
	 */
	public static Operation caseFold(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(CASE_FOLD, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code normalizeNFC} operation. Server normalizes the bin to Unicode
	 * NFC form.
	 */
	public static Operation normalizeNFC(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(NORMALIZE_NFC, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trimStart} operation that removes whitespace from the start
	 * of the bin.
	 */
	public static Operation trimStart(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(TRIM_START, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trimEnd} operation that removes whitespace from the end of
	 * the bin.
	 */
	public static Operation trimEnd(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(TRIM_END, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trim} operation that removes whitespace from both ends of
	 * the bin.
	 */
	public static Operation trim(StringPolicy policy, String binName) {
		byte[] bytes = Pack.pack(TRIM, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code padStart} operation. Server prepends {@code padString} repeatedly
	 * until the bin reaches {@code targetLength} codepoints. No-op if already at or above
	 * target length.
	 */
	public static Operation padStart(StringPolicy policy, String binName, int targetLength, String padString) {
		byte[] bytes = Pack.pack(PAD_START, targetLength, Value.get(padString), policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code padEnd} operation. Server appends {@code padString} repeatedly
	 * until the bin reaches {@code targetLength} codepoints. No-op if already at or above
	 * target length.
	 */
	public static Operation padEnd(StringPolicy policy, String binName, int targetLength, String padString) {
		byte[] bytes = Pack.pack(PAD_END, targetLength, Value.get(padString), policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code repeat} operation that repeats the bin contents {@code count}
	 * times.
	 */
	public static Operation repeat(StringPolicy policy, String binName, int count) {
		byte[] bytes = Pack.pack(REPEAT, count, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexReplace} operation that replaces the first match of
	 * {@code pattern} with {@code replacement}. Use {@link StringRegexFlags#GLOBAL}
	 * to replace all matches.
	 */
	public static Operation regexReplace(
		StringPolicy policy,
		String binName,
		String pattern,
		String replacement,
		int regexFlags
	) {
		List<Value> list = pair(pattern, replacement);
		byte[] bytes = Pack.pack(REGEX_REPLACE, list, regexFlags, policy.flags);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	//-----------------------------------------------------------------
	// Type conversion
	//-----------------------------------------------------------------

	/**
	 * Create {@code toString} operation that converts an integer, float, string, or blob
	 * bin to its string representation. Returns AEROSPIKE_ERR_INCOMPATIBLE_TYPE for any
	 * other bin type.
	 * <p>
	 * The wire format for this op carries no payload; the bin is referenced solely by
	 * the operation header.
	 */
	public static Operation toString(String binName) {
		return new Operation(Operation.Type.TO_STRING, binName, Value.getAsNull());
	}

	//-----------------------------------------------------------------
	// Private helpers
	//-----------------------------------------------------------------

	private static List<Value> pair(String a, String b) {
		List<Value> list = new ArrayList<Value>(2);
		list.add(Value.get(a));
		list.add(Value.get(b));
		return list;
	}

	private static List<Value> toValueList(List<String> strings) {
		List<Value> list = new ArrayList<Value>(strings.size());
		for (String s : strings) {
			list.add(Value.get(s));
		}
		return list;
	}

	private static byte[] packCmdValueInt(int command, Value value, int v) {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(3);
			packer.packInt(command);
			value.pack(packer);
			packer.packInt(v);

			if (i == 0) {
				packer.createBuffer();
			}
		}
		return packer.getBuffer();
	}
}
