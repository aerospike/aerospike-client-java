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
package com.aerospike.examples;

import java.util.Arrays;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.operation.StringNumericType;
import com.aerospike.client.operation.StringOperation;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;

public class OperateString extends Example {

	private static final String BIN = "text";

	/**
	 * Demonstrate every {@link StringOperation} method.
	 * Requires server version 8.1.3 or later.
	 */
	@Override
	public void runExample() throws Exception {
		IAerospikeClient client = client();
		Parameters params = params();
		runReadOps(client, params);
		runModifyOps(client, params);
		runToString(client, params);
	}

	/**
	 * Read-only string operations: return information about the bin without
	 * mutating it.
	 */
	private void runReadOps(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "opstr_read");
		Record r;

		// strlen — codepoint count
		put(client, params, key, "hello world");
		r = client.operate(params.writePolicy, key, StringOperation.strlen(BIN));
		console.info("strlen(\"hello world\") = " + r.getLong(BIN));

		// substr(start) — codepoint slice to end of string
		r = client.operate(params.writePolicy, key, StringOperation.substr(BIN, 6));
		console.info("substr(6) = \"" + r.getString(BIN) + "\"");

		// substr(start, end) — half-open codepoint range
		r = client.operate(params.writePolicy, key, StringOperation.substr(BIN, 0, 5));
		console.info("substr(0, 5) = \"" + r.getString(BIN) + "\"");

		// charAt — single-codepoint slice
		r = client.operate(params.writePolicy, key, StringOperation.charAt(BIN, 6));
		console.info("charAt(6) = \"" + r.getString(BIN) + "\"");

		// find(needle) — index of first match, -1 if absent
		r = client.operate(params.writePolicy, key, StringOperation.find(BIN, "world"));
		console.info("find(\"world\") = " + r.getLong(BIN));

		// find(needle, occurrence) — index of nth match
		put(client, params, key, "ababab");
		r = client.operate(params.writePolicy, key, StringOperation.find(BIN, "ab", 2));
		console.info("find(\"ab\", occurrence=2) on \"ababab\" = " + r.getLong(BIN));

		// contains
		put(client, params, key, "hello world");
		r = client.operate(params.writePolicy, key, StringOperation.contains(BIN, "hello"));
		console.info("contains(\"hello\") = " + r.getBoolean(BIN));

		// startsWith
		r = client.operate(params.writePolicy, key, StringOperation.startsWith(BIN, "hello"));
		console.info("startsWith(\"hello\") = " + r.getBoolean(BIN));

		// endsWith
		r = client.operate(params.writePolicy, key, StringOperation.endsWith(BIN, "world"));
		console.info("endsWith(\"world\") = " + r.getBoolean(BIN));

		// toInteger — parse string as int64
		put(client, params, key, "12345");
		r = client.operate(params.writePolicy, key, StringOperation.toInteger(BIN));
		console.info("toInteger(\"12345\") = " + r.getLong(BIN));

		// toDouble — parse string as float64
		put(client, params, key, "3.14");
		r = client.operate(params.writePolicy, key, StringOperation.toDouble(BIN));
		console.info("toDouble(\"3.14\") = " + r.getDouble(BIN));

		// byteLength — UTF-8 byte count (differs from strlen for non-ASCII)
		put(client, params, key, "héllo");
		r = client.operate(params.writePolicy, key, StringOperation.byteLength(BIN));
		console.info("byteLength(\"héllo\") = " + r.getLong(BIN) + " (5 codepoints, 6 UTF-8 bytes)");

		// isNumeric — accepts integer or float
		put(client, params, key, "12345");
		r = client.operate(params.writePolicy, key, StringOperation.isNumeric(BIN));
		console.info("isNumeric(\"12345\") = " + r.getBoolean(BIN));

		// isNumeric(numericType) — restrict by StringNumericType
		put(client, params, key, "3.14");
		r = client.operate(params.writePolicy, key,
			StringOperation.isNumeric(BIN, StringNumericType.INT));
		console.info("isNumeric(\"3.14\", INT) = " + r.getBoolean(BIN));

		// FLOAT needs a '.' followed by a digit, so a pure-digit string fails it
		put(client, params, key, "12345");
		r = client.operate(params.writePolicy, key,
			StringOperation.isNumeric(BIN, StringNumericType.FLOAT));
		console.info("isNumeric(\"12345\", FLOAT) = " + r.getBoolean(BIN));

		// isUpper
		put(client, params, key, "HELLO");
		r = client.operate(params.writePolicy, key, StringOperation.isUpper(BIN));
		console.info("isUpper(\"HELLO\") = " + r.getBoolean(BIN));

		// isLower
		put(client, params, key, "hello");
		r = client.operate(params.writePolicy, key, StringOperation.isLower(BIN));
		console.info("isLower(\"hello\") = " + r.getBoolean(BIN));

		// toBlob — UTF-8 bytes as byte[]
		r = client.operate(params.writePolicy, key, StringOperation.toBlob(BIN));
		console.info("toBlob(\"hello\") = " + Arrays.toString((byte[]) r.getValue(BIN)));

		// split — one element per codepoint
		put(client, params, key, "abc");
		r = client.operate(params.writePolicy, key, StringOperation.split(BIN));
		console.info("split() = " + r.getList(BIN));

		// split(separator)
		put(client, params, key, "one,two,three");
		r = client.operate(params.writePolicy, key, StringOperation.split(BIN, ","));
		console.info("split(\",\") = " + r.getList(BIN));

		// b64Decode — decode base64 text to byte[]
		put(client, params, key, "aGVsbG8=");
		r = client.operate(params.writePolicy, key, StringOperation.b64Decode(BIN));
		console.info("b64Decode(\"aGVsbG8=\") = \"" + new String((byte[]) r.getValue(BIN)) + "\"");

		// regexCompare — ICU regex pattern match
		put(client, params, key, "Hello123World");
		r = client.operate(params.writePolicy, key, StringOperation.regexCompare(BIN, "[0-9]+"));
		console.info("regexCompare(\"[0-9]+\") = " + r.getBoolean(BIN));

		// regexCompare(flags) — case-insensitive match
		put(client, params, key, "HELLO");
		r = client.operate(params.writePolicy, key,
			StringOperation.regexCompare(BIN, "hello", StringRegexFlags.CASE_INSENSITIVE));
		console.info("regexCompare(\"hello\", CASE_INSENSITIVE) = " + r.getBoolean(BIN));
	}

	/**
	 * Modify operations: mutate the bin in place. Each call below performs the
	 * modify op then re-reads the bin to display the new value.
	 */
	private void runModifyOps(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "opstr_modify");
		StringPolicy policy = StringPolicy.Default;

		// insert — splice value at codepoint index
		put(client, params, key, "hello world");
		modifyAndShow(client, params, key, "insert(5, \" beautiful\")",
			StringOperation.insert(policy, BIN, 5, " beautiful"));

		// overwrite — replace codepoints starting at index
		put(client, params, key, "hello world");
		modifyAndShow(client, params, key, "overwrite(6, \"earth\")",
			StringOperation.overwrite(policy, BIN, 6, "earth"));

		// concat(value) — append a single string
		put(client, params, key, "hello");
		modifyAndShow(client, params, key, "concat(\"!\")",
			StringOperation.concat(policy, BIN, "!"));

		// concat(values) — append each list element in order
		put(client, params, key, "hello");
		modifyAndShow(client, params, key, "concat([\" \", \"big\", \" world\"])",
			StringOperation.concat(policy, BIN, Arrays.asList(" ", "big", " world")));

		// append — Unicode-aware end-append (alongside legacy Operation.append)
		put(client, params, key, "hello");
		modifyAndShow(client, params, key, "append(\"!\")",
			StringOperation.append(policy, BIN, "!"));

		// prepend — Unicode-aware front-insert (alongside legacy Operation.prepend)
		put(client, params, key, "world");
		modifyAndShow(client, params, key, "prepend(\"hello \")",
			StringOperation.prepend(policy, BIN, "hello "));

		// snip — remove half-open codepoint range
		put(client, params, key, "hello beautiful world");
		modifyAndShow(client, params, key, "snip(5, 15)",
			StringOperation.snip(policy, BIN, 5, 15));

		// replace — first occurrence only
		put(client, params, key, "hello world world");
		modifyAndShow(client, params, key, "replace(\"world\", \"earth\")",
			StringOperation.replace(policy, BIN, "world", "earth"));

		// replaceAll — every occurrence
		put(client, params, key, "aabaa");
		modifyAndShow(client, params, key, "replaceAll(\"a\", \"x\")",
			StringOperation.replaceAll(policy, BIN, "a", "x"));

		// upper
		put(client, params, key, "hello world");
		modifyAndShow(client, params, key, "upper()",
			StringOperation.upper(policy, BIN));

		// lower
		put(client, params, key, "HELLO WORLD");
		modifyAndShow(client, params, key, "lower()",
			StringOperation.lower(policy, BIN));

		// caseFold — locale-independent fold for comparison keys
		put(client, params, key, "HELLO World");
		modifyAndShow(client, params, key, "caseFold()",
			StringOperation.caseFold(policy, BIN));

		// normalizeNFC — Unicode NFC normalization
		put(client, params, key, "café");
		modifyAndShow(client, params, key, "normalizeNFC()",
			StringOperation.normalizeNFC(policy, BIN));

		// trimStart — drop leading whitespace
		put(client, params, key, "  hello  ");
		modifyAndShow(client, params, key, "trimStart()",
			StringOperation.trimStart(policy, BIN));

		// trimEnd — drop trailing whitespace
		put(client, params, key, "  hello  ");
		modifyAndShow(client, params, key, "trimEnd()",
			StringOperation.trimEnd(policy, BIN));

		// trim — drop both ends
		put(client, params, key, "  hello world  ");
		modifyAndShow(client, params, key, "trim()",
			StringOperation.trim(policy, BIN));

		// padStart — left-pad up to target codepoint length
		put(client, params, key, "hello");
		modifyAndShow(client, params, key, "padStart(10, \"*\")",
			StringOperation.padStart(policy, BIN, 10, "*"));

		// padEnd — right-pad up to target codepoint length
		put(client, params, key, "hello");
		modifyAndShow(client, params, key, "padEnd(10, \".\")",
			StringOperation.padEnd(policy, BIN, 10, "."));

		// repeat — repeat string n times
		put(client, params, key, "ab");
		modifyAndShow(client, params, key, "repeat(3)",
			StringOperation.repeat(policy, BIN, 3));

		// regexReplace — pass GLOBAL to replace every match (default replaces first only)
		put(client, params, key, "abc123def456");
		modifyAndShow(client, params, key, "regexReplace(\"[0-9]+\", \"NUM\", GLOBAL)",
			StringOperation.regexReplace(policy, BIN, "[0-9]+", "NUM",
				StringRegexFlags.GLOBAL));
	}

	/**
	 * toString — convert any int / float / string / blob bin to its string
	 * representation. Unlike the other ops, this does not accept a CTX.
	 */
	private void runToString(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "opstr_tostring");
		String numBin = "n";

		client.delete(params.writePolicy, key);
		client.put(params.writePolicy, key, new Bin(numBin, 42));

		Record r = client.operate(params.writePolicy, key, StringOperation.toString(numBin));
		console.info("toString(int 42) = \"" + r.getString(numBin) + "\"");
	}

	private void put(IAerospikeClient client, Parameters params, Key key, String value) {
		client.delete(params.writePolicy, key);
		client.put(params.writePolicy, key, new Bin(BIN, value));
	}

	private void modifyAndShow(IAerospikeClient client, Parameters params, Key key,
			String label, Operation modifyOp) {
		client.operate(params.writePolicy, key, modifyOp);
		String result = client.get(params.policy, key).getString(BIN);
		console.info(label + " -> \"" + result + "\"");
	}
}
