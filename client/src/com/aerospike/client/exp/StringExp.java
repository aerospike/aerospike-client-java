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
package com.aerospike.client.exp;

import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * String expression generator. See {@link com.aerospike.client.exp.Exp}.
 * <p>
 * The string source argument in these methods is any expression that yields a
 * string: a bin reference (e.g. {@link Exp#stringBin(String)}), a string literal
 * ({@link Exp#val(String)}), or a nested string expression. Expressions that
 * modify a string value return the modified string; the bin is not changed.
 * <p>
 * String expressions require server version 8.1.3 or later.
 */
public final class StringExp {
	private static final int MODULE = 3;       // CALL_STRING
	private static final int MODULE_REPR = 4;  // CALL_REPR

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
	private static final int B64_ENCODE = 16;
	private static final int REGEX_COMPARE = 17;

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
	// Read expressions
	//-----------------------------------------------------------------

	/**
	 * Create expression that returns the codepoint length of the source string.
	 */
	public static Exp strlen(Exp src) {
		byte[] bytes = Pack.pack(STRLEN);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns the substring from {@code start} to the end of the source.
	 */
	public static Exp substr(Exp start, Exp src) {
		byte[] bytes = Pack.pack(SUBSTR, start);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns {@code length} codepoints starting at {@code start}.
	 */
	public static Exp substr(Exp start, Exp length, Exp src) {
		byte[] bytes = Pack.pack(SUBSTR, start, length);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns the character at {@code index} as a string.
	 */
	public static Exp charAt(Exp index, Exp src) {
		byte[] bytes = Pack.pack(CHAR_AT, index);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns the codepoint index of the first match of
	 * {@code needle} in the source, or -1 if not found.
	 */
	public static Exp find(Exp needle, Exp src) {
		byte[] bytes = Pack.pack(FIND, needle);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns the codepoint index of the {@code occurrence}-th
	 * match of {@code needle}, or -1 if not found.
	 */
	public static Exp find(Exp needle, Exp occurrence, Exp src) {
		byte[] bytes = Pack.pack(FIND, needle, occurrence);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if the source contains {@code needle}, 0 otherwise.
	 */
	public static Exp contains(Exp needle, Exp src) {
		byte[] bytes = Pack.pack(CONTAINS, needle);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if the source begins with {@code prefix}, 0 otherwise.
	 */
	public static Exp startsWith(Exp prefix, Exp src) {
		byte[] bytes = Pack.pack(STARTS_WITH, prefix);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if the source ends with {@code suffix}, 0 otherwise.
	 */
	public static Exp endsWith(Exp suffix, Exp src) {
		byte[] bytes = Pack.pack(ENDS_WITH, suffix);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that parses the source as int64.
	 */
	public static Exp toInteger(Exp src) {
		byte[] bytes = Pack.pack(TO_INTEGER);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that parses the source as a 64-bit float.
	 */
	public static Exp toDouble(Exp src) {
		byte[] bytes = Pack.pack(TO_DOUBLE);
		return addRead(src, bytes, Exp.Type.FLOAT);
	}

	/**
	 * Create expression that returns the UTF-8 byte length of the source.
	 */
	public static Exp byteLength(Exp src) {
		byte[] bytes = Pack.pack(BYTE_LENGTH);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if the source parses as a number, 0 otherwise.
	 */
	public static Exp isNumeric(Exp src) {
		byte[] bytes = Pack.pack(IS_NUMERIC);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if the source parses as a number of the requested
	 * {@link com.aerospike.client.operation.StringNumericType}, 0 otherwise.
	 */
	public static Exp isNumeric(int numericType, Exp src) {
		byte[] bytes = Pack.pack(IS_NUMERIC, numericType);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if every cased character in the source is uppercase.
	 */
	public static Exp isUpper(Exp src) {
		byte[] bytes = Pack.pack(IS_UPPER);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if every cased character in the source is lowercase.
	 */
	public static Exp isLower(Exp src) {
		byte[] bytes = Pack.pack(IS_LOWER);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns the UTF-8 bytes of the source as a blob.
	 */
	public static Exp toBlob(Exp src) {
		byte[] bytes = Pack.pack(TO_BLOB);
		return addRead(src, bytes, Exp.Type.BLOB);
	}

	/**
	 * Create expression that splits the source by Unicode codepoint and returns a list of strings.
	 */
	public static Exp split(Exp src) {
		byte[] bytes = Pack.pack(SPLIT);
		return addRead(src, bytes, Exp.Type.LIST);
	}

	/**
	 * Create expression that splits the source by {@code separator} and returns a list of strings.
	 */
	public static Exp split(Exp separator, Exp src) {
		byte[] bytes = Pack.pack(SPLIT, separator);
		return addRead(src, bytes, Exp.Type.LIST);
	}

	/**
	 * Create expression that base64-decodes the source and returns a blob.
	 */
	public static Exp b64Decode(Exp src) {
		byte[] bytes = Pack.pack(B64_DECODE);
		return addRead(src, bytes, Exp.Type.BLOB);
	}

	/**
	 * Create expression that base64-encodes a blob source as a string.
	 */
	public static Exp b64Encode(Exp src) {
		byte[] bytes = Pack.pack(B64_ENCODE);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns 1 if {@code pattern} (ICU regex) matches the source, 0 otherwise.
	 */
	public static Exp regexCompare(Exp pattern, Exp src) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, pattern);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns 1 if {@code pattern} (ICU regex) matches the source under
	 * {@link StringRegexFlags}, 0 otherwise.
	 */
	public static Exp regexCompare(Exp pattern, int regexFlags, Exp src) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, pattern, regexFlags);
		return addRead(src, bytes, Exp.Type.INT);
	}

	//-----------------------------------------------------------------
	// Modify expressions
	//-----------------------------------------------------------------

	/**
	 * Create expression that inserts {@code value} at codepoint {@code index} of the source
	 * and returns the resulting string.
	 */
	public static Exp insert(StringPolicy policy, Exp index, Exp value, Exp src) {
		byte[] bytes = Pack.pack(INSERT, index, value, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that overwrites characters starting at codepoint {@code index} with
	 * {@code value} and returns the resulting string.
	 */
	public static Exp overwrite(StringPolicy policy, Exp index, Exp value, Exp src) {
		byte[] bytes = Pack.pack(OVERWRITE, index, value, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that concatenates {@code values} (a list of strings) onto the source
	 * and returns the resulting string. Single-string callers can wrap their value in a
	 * 1-element list via {@link Exp#val(java.util.List)}.
	 */
	public static Exp concat(StringPolicy policy, Exp values, Exp src) {
		byte[] bytes = Pack.pack(CONCAT, values, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that removes characters from codepoint {@code start} to the end of
	 * the source and returns the resulting string.
	 */
	public static Exp snip(StringPolicy policy, Exp start, Exp src) {
		byte[] bytes = Pack.pack(SNIP, start, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that removes characters from codepoint {@code start} (inclusive) to
	 * {@code end} (exclusive) and returns the resulting string.
	 */
	public static Exp snip(StringPolicy policy, Exp start, Exp end, Exp src) {
		byte[] bytes = Pack.pack(SNIP, start, end, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces the first occurrence of {@code needle} with {@code replacement}
	 * and returns the resulting string.
	 */
	public static Exp replace(StringPolicy policy, Exp needle, Exp replacement, Exp src) {
		byte[] bytes = packReplace(REPLACE, needle, replacement, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces every occurrence of {@code needle} with {@code replacement}
	 * and returns the resulting string.
	 */
	public static Exp replaceAll(StringPolicy policy, Exp needle, Exp replacement, Exp src) {
		byte[] bytes = packReplace(REPLACE_ALL, needle, replacement, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source uppercased.
	 */
	public static Exp upper(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(UPPER, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source lowercased.
	 */
	public static Exp lower(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(LOWER, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source case-folded (locale-independent lowercase).
	 */
	public static Exp caseFold(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(CASE_FOLD, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source normalized to Unicode NFC form.
	 */
	public static Exp normalizeNFC(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(NORMALIZE_NFC, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source with whitespace removed from the start.
	 */
	public static Exp trimStart(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM_START, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source with whitespace removed from the end.
	 */
	public static Exp trimEnd(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM_END, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source with whitespace removed from both ends.
	 */
	public static Exp trim(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that pads the start of the source to {@code targetLength} codepoints
	 * using {@code padString}.
	 */
	public static Exp padStart(StringPolicy policy, Exp targetLength, Exp padString, Exp src) {
		byte[] bytes = Pack.pack(PAD_START, targetLength, padString, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that pads the end of the source to {@code targetLength} codepoints
	 * using {@code padString}.
	 */
	public static Exp padEnd(StringPolicy policy, Exp targetLength, Exp padString, Exp src) {
		byte[] bytes = Pack.pack(PAD_END, targetLength, padString, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns the source repeated {@code count} times.
	 */
	public static Exp repeat(StringPolicy policy, Exp count, Exp src) {
		byte[] bytes = Pack.pack(REPEAT, count, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces matches of {@code pattern} (ICU regex) with
	 * {@code replacement} and returns the resulting string. Use
	 * {@link StringRegexFlags#GLOBAL} to replace all matches.
	 */
	public static Exp regexReplace(
		StringPolicy policy,
		Exp pattern,
		Exp replacement,
		int regexFlags,
		Exp src
	) {
		byte[] bytes = packRegexReplace(pattern, replacement, regexFlags, policy.flags);
		return addModify(src, bytes);
	}

	//-----------------------------------------------------------------
	// Type conversion expression
	//-----------------------------------------------------------------

	/**
	 * Create expression that returns the string representation of {@code src}, where
	 * {@code src} may be any expression yielding an integer, float, string, or blob value.
	 */
	public static Exp toString(Exp src) {
		byte[] bytes = emptyArray();
		return new Exp.Module(src, bytes, Exp.Type.STRING.code, MODULE_REPR);
	}

	//-----------------------------------------------------------------
	// Private helpers
	//-----------------------------------------------------------------

	private static Exp addRead(Exp src, byte[] bytes, Exp.Type retType) {
		return new Exp.Module(src, bytes, retType.code, MODULE);
	}

	private static Exp addModify(Exp src, byte[] bytes) {
		return new Exp.Module(src, bytes, Exp.Type.STRING.code, MODULE | Exp.MODIFY);
	}

	// [cmd, [needle, repl], flags] — needle/replacement nested inside a 2-element list.
	private static byte[] packReplace(int command, Exp needle, Exp replacement, int flags) {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(3);
			packer.packInt(command);
			packer.packArrayBegin(2);
			needle.pack(packer);
			replacement.pack(packer);
			packer.packInt(flags);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}

	// [REGEX_REPLACE, [pattern, repl], regexFlags, flags]
	private static byte[] packRegexReplace(Exp pattern, Exp replacement, int regexFlags, int flags) {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(4);
			packer.packInt(REGEX_REPLACE);
			packer.packArrayBegin(2);
			pattern.pack(packer);
			replacement.pack(packer);
			packer.packInt(regexFlags);
			packer.packInt(flags);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}

	private static byte[] emptyArray() {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(0);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}
}
