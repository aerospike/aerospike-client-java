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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.operation.StringOperation;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.test.sync.TestSync;

/**
 * Negative tests for the server's bin-UTF-8 entry gate 8.1.3.
 *
 * <p>Every read and modify op in {@link StringOperation} must reject a string
 * bin whose stored bytes are not well-formed UTF-8. The server's
 * {@code as_bin_string_read} / {@code as_bin_string_modify} entry helpers run
 * {@code utf8_string_length} on the bin before dispatching to the op-specific
 * code, returning {@code AS_ERR_INVALID_ENCODING} ({@link ResultCode#INVALID_ENCODING}).
 *
 * <p>Java {@link String} cannot directly hold an ill-formed UTF-8 sequence —
 * the standard {@code UTF_8} decoder substitutes {@code U+FFFD}. To plant
 * raw invalid bytes in a string-typed bin we use
 * {@link Value.BytesValue#BytesValue(byte[], int) Value.BytesValue(bytes, ParticleType.STRING)},
 * which writes the bytes verbatim with the {@code STRING} particle type byte.
 *
 * <p>The fixture {@code BAD = {0xED, 0xA0, 0x80}} is U+D800 (ill-formed surrogate),
 * the same fixture used by the server's {@code EntryParityUtf8} unit tests.
 */
public class TestStringInvalidUtf8 extends TestSync {
	private static final String BIN = "sbin";
	private static final Key KEY = new Key(args.namespace, args.set, "string-invalid-utf8-key");
	private static final StringPolicy POLICY = StringPolicy.Default;

	/** Ill-formed UTF-8: 3-byte encoding of U+D800 (surrogate). */
	private static final byte[] BAD = new byte[] { (byte)0xED, (byte)0xA0, (byte)0x80 };

	@BeforeClass
	public static void serverVersionCheck() {
		Assume.assumeTrue(
			"Skipping: string operations require server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));
	}

	@Before
	public void plantInvalidBin() {
		client.delete(null, KEY);
		// BytesValue(..., ParticleType.STRING) writes the bytes verbatim under the
		// STRING particle type, bypassing Java-side UTF-8 sanitization.
		client.put(null, KEY,
			new Bin(BIN, new Value.BytesValue(BAD, ParticleType.STRING)));
	}

	private static void assertInvalidEncoding(Operation op) {
		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> client.operate(null, KEY, op));
		assertEquals(ResultCode.INVALID_ENCODING, ae.getResultCode());
	}

	//=================================================================
	// Read ops — bin gate fires before op-specific logic
	//=================================================================

	@Test public void strlenRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.strlen(BIN));
	}

	@Test public void substrRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.substr(BIN, 0));
	}

	@Test public void charAtRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.charAt(BIN, 0));
	}

	@Test public void findRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.find(BIN, "x"));
	}

	@Test public void containsRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.contains(BIN, "x"));
	}

	@Test public void startsWithRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.startsWith(BIN, "x"));
	}

	@Test public void endsWithRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.endsWith(BIN, "x"));
	}

	@Test public void toIntegerRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.toInteger(BIN));
	}

	@Test public void toDoubleRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.toDouble(BIN));
	}

	// byte_length, to_blob, b64_decode, trim*, repeat, concat are listed in the
	// 8.1.3 client report as "unaffected" by UTF-8, but per the doc's §3 and
	// §11 they hit the same bin gate as strlen and must also reject.
	@Test public void byteLengthRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.byteLength(BIN));
	}

	@Test public void isNumericRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.isNumeric(BIN));
	}

	@Test public void isUpperRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.isUpper(BIN));
	}

	@Test public void isLowerRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.isLower(BIN));
	}

	@Test public void toBlobRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.toBlob(BIN));
	}

	@Test public void splitRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.split(BIN, ","));
	}

	@Test public void b64DecodeRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.b64Decode(BIN));
	}

	@Test public void regexCompareRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.regexCompare(BIN, "x"));
	}

	//=================================================================
	// Modify ops — bin gate also fires here; bin must remain unchanged.
	//
	// We can't easily verify "bin bytes unchanged" via client.get because the
	// Java client decodes STRING particles through java.nio UTF-8, which
	// replaces ill-formed sequences with U+FFFD; the raw bytes are not
	// recoverable through the public client surface. The fact that a
	// subsequent strlen on the same bin still hits INVALID_ENCODING (see
	// failedModifyDoesNotOverwriteBin below) proves the failed modify did
	// not replace the bin with a well-formed value.
	//=================================================================

	@Test public void insertRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.insert(POLICY, BIN, 0, "x"));
	}

	@Test public void overwriteRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.overwrite(POLICY, BIN, 0, "x"));
	}

	@Test public void concatRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.concat(POLICY, BIN, "x"));
	}

	@Test public void snipRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.snip(POLICY, BIN, 0, 1));
	}

	@Test public void replaceRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.replace(POLICY, BIN, "x", "y"));
	}

	@Test public void replaceAllRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.replaceAll(POLICY, BIN, "x", "y"));
	}

	@Test public void upperRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.upper(POLICY, BIN));
	}

	@Test public void lowerRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.lower(POLICY, BIN));
	}

	@Test public void caseFoldRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.caseFold(POLICY, BIN));
	}

	@Test public void normalizeNFCRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.normalizeNFC(POLICY, BIN));
	}

	@Test public void trimStartRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.trimStart(POLICY, BIN));
	}

	@Test public void trimEndRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.trimEnd(POLICY, BIN));
	}

	@Test public void trimRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.trim(POLICY, BIN));
	}

	@Test public void padStartRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.padStart(POLICY, BIN, 10, "*"));
	}

	@Test public void padEndRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.padEnd(POLICY, BIN, 10, "*"));
	}

	@Test public void repeatRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.repeat(POLICY, BIN, 2));
	}

	@Test public void regexReplaceRejectsInvalidBin() {
		assertInvalidEncoding(StringOperation.regexReplace(
			POLICY, BIN, "x", "y", StringRegexFlags.DEFAULT));
	}

	//=================================================================
	// Post-failure invariant
	//=================================================================

	@Test
	public void failedModifyDoesNotOverwriteBin() {
		// First modify attempt must fail with INVALID_ENCODING.
		assertInvalidEncoding(StringOperation.upper(POLICY, BIN));
		// A subsequent read on the same bin must also fail at the gate, proving
		// the bin still holds the original invalid bytes (the failed modify
		// did not replace it with a well-formed value).
		assertInvalidEncoding(StringOperation.strlen(BIN));
	}

	//=================================================================
	// Client-side defense — invalid UTF-8 in an op argument is rejected
	// before the wire by Utf8.encodedLength (client/src/.../util/Utf8.java:87).
	// This complements the server's invalid-arg gate (which Java callers can't
	// normally reach because String → UTF-8 conversion either throws here or
	// substitutes well-formed bytes).
	//=================================================================

	@Test
	public void unpairedSurrogateInArgIsRejectedClientSide() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, "hello"));
		// "\uD800" is an unpaired high surrogate. The client's UTF-8 encoder
		// throws AerospikeException before sending the op.
		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> client.operate(null, KEY,
				StringOperation.find(BIN, "\uD800")));
		// Sanity-check the message — encodedLength's throw includes the word.
		assertTrue("expected 'surrogate' in message: " + ae.getMessage(),
			ae.getMessage() != null && ae.getMessage().contains("surrogate"));
	}
}
