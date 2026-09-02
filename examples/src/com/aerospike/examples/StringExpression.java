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
import com.aerospike.client.Record;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.StringExp;
import com.aerospike.client.operation.StringNumericType;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;

public class StringExpression extends Example {

	private static final String BIN = "text";
	private static final String VAR = "result";

	/**
	 * Demonstrate every {@link StringExp} expression builder. Each demo evaluates
	 * the expression with {@code ExpOperation.read} and prints the result.
	 * Modify-style expressions (upper, replace, …) return the modified string
	 * value; the bin is <strong>not</strong> mutated.
	 * Requires server version 8.1.3 or later.
	 */
	@Override
	public void runExample() throws Exception {
		IAerospikeClient client = client();
		Parameters params = params();
		runReadExps(client, params);
		runModifyExps(client, params);
		runToString(client, params);
	}

	/**
	 * Read-only string expressions.
	 */
	private void runReadExps(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "stringexp_read");
		Record r;

		// strlen — codepoint count
		put(client, params, key, "hello world");
		r = evalExp(client, params, key, StringExp.strlen(Exp.stringBin(BIN)));
		console.info("strlen(\"hello world\") = " + r.getLong(VAR));

		// substr(start) — codepoint slice to end of string
		r = evalExp(client, params, key, StringExp.substr(Exp.val(6), Exp.stringBin(BIN)));
		console.info("substr(6) = \"" + r.getString(VAR) + "\"");

		// substr(start, end) — half-open codepoint range
		r = evalExp(client, params, key,
			StringExp.substr(Exp.val(0), Exp.val(5), Exp.stringBin(BIN)));
		console.info("substr(0, 5) = \"" + r.getString(VAR) + "\"");

		// charAt — single-codepoint slice
		r = evalExp(client, params, key, StringExp.charAt(Exp.val(6), Exp.stringBin(BIN)));
		console.info("charAt(6) = \"" + r.getString(VAR) + "\"");

		// find(needle) — index of first match, -1 if absent
		r = evalExp(client, params, key, StringExp.find(Exp.val("world"), Exp.stringBin(BIN)));
		console.info("find(\"world\") = " + r.getLong(VAR));

		// find(needle, occurrence) — index of nth match
		put(client, params, key, "ababab");
		r = evalExp(client, params, key,
			StringExp.find(Exp.val("ab"), Exp.val(2), Exp.stringBin(BIN)));
		console.info("find(\"ab\", occurrence=2) on \"ababab\" = " + r.getLong(VAR));

		// contains
		put(client, params, key, "hello world");
		r = evalExp(client, params, key,
			StringExp.contains(Exp.val("hello"), Exp.stringBin(BIN)));
		console.info("contains(\"hello\") = " + r.getBoolean(VAR));

		// startsWith
		r = evalExp(client, params, key,
			StringExp.startsWith(Exp.val("hello"), Exp.stringBin(BIN)));
		console.info("startsWith(\"hello\") = " + r.getBoolean(VAR));

		// endsWith
		r = evalExp(client, params, key,
			StringExp.endsWith(Exp.val("world"), Exp.stringBin(BIN)));
		console.info("endsWith(\"world\") = " + r.getBoolean(VAR));

		// toInteger — parse string as int64
		put(client, params, key, "12345");
		r = evalExp(client, params, key, StringExp.toInteger(Exp.stringBin(BIN)));
		console.info("toInteger(\"12345\") = " + r.getLong(VAR));

		// toDouble — parse string as float64
		put(client, params, key, "3.14");
		r = evalExp(client, params, key, StringExp.toDouble(Exp.stringBin(BIN)));
		console.info("toDouble(\"3.14\") = " + r.getDouble(VAR));

		// byteLength — UTF-8 byte count (differs from strlen for non-ASCII)
		put(client, params, key, "héllo");
		r = evalExp(client, params, key, StringExp.byteLength(Exp.stringBin(BIN)));
		console.info("byteLength(\"héllo\") = " + r.getLong(VAR) + " (5 codepoints, 6 UTF-8 bytes)");

		// isNumeric — accepts integer or float
		put(client, params, key, "12345");
		r = evalExp(client, params, key, StringExp.isNumeric(Exp.stringBin(BIN)));
		console.info("isNumeric(\"12345\") = " + r.getBoolean(VAR));

		// isNumeric(numericType) — restrict by StringNumericType
		put(client, params, key, "3.14");
		r = evalExp(client, params, key,
			StringExp.isNumeric(StringNumericType.INT, Exp.stringBin(BIN)));
		console.info("isNumeric(\"3.14\", INT) = " + r.getBoolean(VAR));

		// FLOAT needs a '.' followed by a digit, so a pure-digit string fails it
		put(client, params, key, "12345");
		r = evalExp(client, params, key,
			StringExp.isNumeric(StringNumericType.FLOAT, Exp.stringBin(BIN)));
		console.info("isNumeric(\"12345\", FLOAT) = " + r.getBoolean(VAR));

		// isUpper
		put(client, params, key, "HELLO");
		r = evalExp(client, params, key, StringExp.isUpper(Exp.stringBin(BIN)));
		console.info("isUpper(\"HELLO\") = " + r.getBoolean(VAR));

		// isLower
		put(client, params, key, "hello");
		r = evalExp(client, params, key, StringExp.isLower(Exp.stringBin(BIN)));
		console.info("isLower(\"hello\") = " + r.getBoolean(VAR));

		// toBlob — UTF-8 bytes as byte[]
		r = evalExp(client, params, key, StringExp.toBlob(Exp.stringBin(BIN)));
		console.info("toBlob(\"hello\") = " + Arrays.toString((byte[]) r.getValue(VAR)));

		// split — one element per codepoint
		put(client, params, key, "abc");
		r = evalExp(client, params, key, StringExp.split(Exp.stringBin(BIN)));
		console.info("split() = " + r.getList(VAR));

		// split(separator)
		put(client, params, key, "one,two,three");
		r = evalExp(client, params, key,
			StringExp.split(Exp.val(","), Exp.stringBin(BIN)));
		console.info("split(\",\") = " + r.getList(VAR));

		// b64Decode — decode base64 text to byte[]
		put(client, params, key, "aGVsbG8=");
		r = evalExp(client, params, key, StringExp.b64Decode(Exp.stringBin(BIN)));
		console.info("b64Decode(\"aGVsbG8=\") = \"" + new String((byte[]) r.getValue(VAR)) + "\"");

		// regexCompare — ICU regex pattern match
		put(client, params, key, "Hello123World");
		r = evalExp(client, params, key,
			StringExp.regexCompare(Exp.val("[0-9]+"), Exp.stringBin(BIN)));
		console.info("regexCompare(\"[0-9]+\") = " + r.getBoolean(VAR));

		// regexCompare(flags) — case-insensitive match
		put(client, params, key, "HELLO");
		r = evalExp(client, params, key,
			StringExp.regexCompare(Exp.val("hello"),
				StringRegexFlags.CASE_INSENSITIVE, Exp.stringBin(BIN)));
		console.info("regexCompare(\"hello\", CASE_INSENSITIVE) = " + r.getBoolean(VAR));
	}

	/**
	 * Modify-style expressions: each returns the transformed string value.
	 * The underlying bin is not mutated.
	 */
	private void runModifyExps(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "stringexp_modify");
		StringPolicy policy = StringPolicy.Default;
		Record r;

		// insert — splice value at codepoint index
		put(client, params, key, "hello world");
		r = evalExp(client, params, key,
			StringExp.insert(policy, Exp.val(5), Exp.val(" beautiful"), Exp.stringBin(BIN)));
		console.info("insert(5, \" beautiful\") -> \"" + r.getString(VAR) + "\"");

		// overwrite — replace codepoints starting at index
		r = evalExp(client, params, key,
			StringExp.overwrite(policy, Exp.val(6), Exp.val("earth"), Exp.stringBin(BIN)));
		console.info("overwrite(6, \"earth\") -> \"" + r.getString(VAR) + "\"");

		// concat — append a list of strings in order
		put(client, params, key, "hello");
		r = evalExp(client, params, key,
			StringExp.concat(policy,
				Exp.val(Arrays.asList(" ", "big", " world")),
				Exp.stringBin(BIN)));
		console.info("concat([\" \", \"big\", \" world\"]) -> \"" + r.getString(VAR) + "\"");

		// append — Unicode-aware end-append
		r = evalExp(client, params, key,
			StringExp.append(policy, Exp.val("!"), Exp.stringBin(BIN)));
		console.info("append(\"!\") -> \"" + r.getString(VAR) + "\"");

		// prepend — Unicode-aware front-insert
		put(client, params, key, "world");
		r = evalExp(client, params, key,
			StringExp.prepend(policy, Exp.val("hello "), Exp.stringBin(BIN)));
		console.info("prepend(\"hello \") -> \"" + r.getString(VAR) + "\"");

		// snip — remove half-open codepoint range
		put(client, params, key, "hello beautiful world");
		r = evalExp(client, params, key,
			StringExp.snip(policy, Exp.val(5), Exp.val(15), Exp.stringBin(BIN)));
		console.info("snip(5, 15) -> \"" + r.getString(VAR) + "\"");

		// replace — first occurrence only
		put(client, params, key, "hello world world");
		r = evalExp(client, params, key,
			StringExp.replace(policy, Exp.val("world"), Exp.val("earth"), Exp.stringBin(BIN)));
		console.info("replace(\"world\", \"earth\") -> \"" + r.getString(VAR) + "\"");

		// replaceAll — every occurrence
		put(client, params, key, "aabaa");
		r = evalExp(client, params, key,
			StringExp.replaceAll(policy, Exp.val("a"), Exp.val("x"), Exp.stringBin(BIN)));
		console.info("replaceAll(\"a\", \"x\") -> \"" + r.getString(VAR) + "\"");

		// upper
		put(client, params, key, "hello world");
		r = evalExp(client, params, key, StringExp.upper(policy, Exp.stringBin(BIN)));
		console.info("upper() -> \"" + r.getString(VAR) + "\"");

		// lower
		put(client, params, key, "HELLO WORLD");
		r = evalExp(client, params, key, StringExp.lower(policy, Exp.stringBin(BIN)));
		console.info("lower() -> \"" + r.getString(VAR) + "\"");

		// caseFold — locale-independent fold for comparison keys
		put(client, params, key, "HELLO World");
		r = evalExp(client, params, key, StringExp.caseFold(policy, Exp.stringBin(BIN)));
		console.info("caseFold() -> \"" + r.getString(VAR) + "\"");

		// normalizeNFC — Unicode NFC normalization
		put(client, params, key, "café");
		r = evalExp(client, params, key, StringExp.normalizeNFC(policy, Exp.stringBin(BIN)));
		console.info("normalizeNFC() -> \"" + r.getString(VAR) + "\"");

		// trimStart — drop leading whitespace
		put(client, params, key, "  hello  ");
		r = evalExp(client, params, key, StringExp.trimStart(policy, Exp.stringBin(BIN)));
		console.info("trimStart() -> \"" + r.getString(VAR) + "\"");

		// trimEnd — drop trailing whitespace
		r = evalExp(client, params, key, StringExp.trimEnd(policy, Exp.stringBin(BIN)));
		console.info("trimEnd() -> \"" + r.getString(VAR) + "\"");

		// trim — drop both ends
		put(client, params, key, "  hello world  ");
		r = evalExp(client, params, key, StringExp.trim(policy, Exp.stringBin(BIN)));
		console.info("trim() -> \"" + r.getString(VAR) + "\"");

		// padStart — left-pad up to target codepoint length
		put(client, params, key, "hello");
		r = evalExp(client, params, key,
			StringExp.padStart(policy, Exp.val(10), Exp.val("*"), Exp.stringBin(BIN)));
		console.info("padStart(10, \"*\") -> \"" + r.getString(VAR) + "\"");

		// padEnd — right-pad up to target codepoint length
		r = evalExp(client, params, key,
			StringExp.padEnd(policy, Exp.val(10), Exp.val("."), Exp.stringBin(BIN)));
		console.info("padEnd(10, \".\") -> \"" + r.getString(VAR) + "\"");

		// repeat — repeat string n times
		put(client, params, key, "ab");
		r = evalExp(client, params, key,
			StringExp.repeat(policy, Exp.val(3), Exp.stringBin(BIN)));
		console.info("repeat(3) -> \"" + r.getString(VAR) + "\"");

		// regexReplace — pass GLOBAL to replace every match (default replaces first only)
		put(client, params, key, "abc123def456");
		r = evalExp(client, params, key,
			StringExp.regexReplace(policy,
				Exp.val("[0-9]+"), Exp.val("NUM"),
				StringRegexFlags.GLOBAL, Exp.stringBin(BIN)));
		console.info("regexReplace(\"[0-9]+\", \"NUM\", GLOBAL) -> \"" + r.getString(VAR) + "\"");
	}

	/**
	 * toString — stringify any int / float / string / blob source. Demonstrated
	 * on an integer bin.
	 */
	private void runToString(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "stringexp_tostring");
		String numBin = "n";

		client.delete(params.writePolicy, key);
		client.put(params.writePolicy, key, new Bin(numBin, 42));

		Expression exp = Exp.build(StringExp.toString(Exp.intBin(numBin)));
		Record r = client.operate(params.writePolicy, key,
			ExpOperation.read(VAR, exp, ExpReadFlags.DEFAULT));
		console.info("toString(intBin(\"n\") = 42) -> \"" + r.getString(VAR) + "\"");
	}

	private void put(IAerospikeClient client, Parameters params, Key key, String value) {
		client.delete(params.writePolicy, key);
		client.put(params.writePolicy, key, new Bin(BIN, value));
	}

	private Record evalExp(IAerospikeClient client, Parameters params, Key key, Exp e) {
		Expression exp = Exp.build(e);
		return client.operate(params.writePolicy, key,
			ExpOperation.read(VAR, exp, ExpReadFlags.DEFAULT));
	}
}
