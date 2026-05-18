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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.RecordParser;
import com.aerospike.test.util.TestBase;

/**
 * Unit-style tests that exercise RecordParser's msgpack error-detail decoding
 * by feeding it synthetic wire-format buffers. No server connection required.
 *
 * <p>Also verifies the info4 verbosity bit math (Command.INFO4_ERROR_VERBOSITY_*).
 */
public class TestErrorDetailParser extends TestBase {

	// ---------- Verbosity bit math (fix #3) ----------

	@Test
	public void verbosityShiftAndMaskAreConsistent() {
		assertEquals(5, Command.INFO4_ERROR_VERBOSITY_SHIFT);
		assertEquals(0x60, Command.INFO4_ERROR_VERBOSITY_MASK);
		// Mask must cover exactly two bits at shift position.
		assertEquals(0x60, (0x03 << Command.INFO4_ERROR_VERBOSITY_SHIFT));
	}

	@Test
	public void verbosityValueInRangeIsPreservedAfterMasking() {
		for (int v = 0; v <= 3; v++) {
			int actual = (v << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK;
			assertEquals("v=" + v, v << Command.INFO4_ERROR_VERBOSITY_SHIFT, actual);
		}
	}

	@Test
	public void verbosityOutOfRangeCannotCorruptOtherInfo4Bits() {
		// Key invariant: regardless of input, masking guarantees only bits 5-6 can ever be set.
		// No other info4 bit (TXN_VERIFY_READ, TXN_ROLL_FORWARD, TXN_ROLL_BACK, TXN_ON_LOCKING_ONLY, etc.)
		// may flip from a stray verbosity value.
		int otherBits = ~Command.INFO4_ERROR_VERBOSITY_MASK & 0xFF;
		for (int v : new int[] {0, 1, 2, 3, 4, 8, 16, 255, Integer.MAX_VALUE, -1}) {
			int written = (v << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK;
			assertEquals("other-bit pollution for v=" + v, 0, written & otherBits);
			assertTrue("result must fit in mask for v=" + v, written == (written & Command.INFO4_ERROR_VERBOSITY_MASK));
		}

		// Specific spot checks: values that, pre-mask, would set bits OUTSIDE 5-6.
		// 4 << 5 = 0x80 (bit 7),  8 << 5 = 0x100 (bit 8),  16 << 5 = 0x200 (bit 9).
		// All three masked → 0 because none have bits 5 or 6 lit.
		assertEquals(0, (4  << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
		assertEquals(0, (8  << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
		assertEquals(0, (16 << Command.INFO4_ERROR_VERBOSITY_SHIFT) & Command.INFO4_ERROR_VERBOSITY_MASK);
	}

	// ---------- Parser: fixmap (baseline) ----------

	@Test
	public void parsesFixmapWithSubcodeAndMessage() {
		byte[] detail = fixmap2(
			pair(intKey(1), fixint(99)),
			pair(intKey(2), fixstr("cannot append"))
		);
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals("cannot append (subcode=99)", rp.serverMessage);
	}

	@Test
	public void parsesFixmapWithSubcodeOnly() {
		byte[] detail = fixmap1(pair(intKey(1), fixint(42)));
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals("error subcode=42", rp.serverMessage);
	}

	@Test
	public void parsesFixmapWithMessageOnly() {
		byte[] detail = fixmap1(pair(intKey(2), fixstr("oops")));
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals("oops", rp.serverMessage);
	}

	@Test
	public void parsesKeysInReverseOrder() {
		// Server is allowed to emit the map keys in any order; result must be identical.
		byte[] detail = fixmap2(
			pair(intKey(2), fixstr("swap")),
			pair(intKey(1), fixint(7))
		);
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals("swap (subcode=7)", rp.serverMessage);
	}

	@Test
	public void parsesMultiByteUtf8Message() {
		// Mix BMP and supplementary-plane code points so we exercise both
		// 2/3-byte and 4-byte UTF-8 sequences.
		String multibyte = "αβγ · 测试 · 🚀";
		byte[] detail = fixmap2(
			pair(intKey(1), fixint(1)),
			pair(intKey(2), fixstr(multibyte))
		);
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals(multibyte + " (subcode=1)", rp.serverMessage);
	}

	// ---------- Parser: msgpack types that the original hand-rolled decoder didn't handle (fix #2) ----------

	@Test
	public void parsesMap16Header() {
		// Build with 16 entries to force map16. Real keys 1 and 2; pad rest with unknown keys 100..113 → uint8.
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		// map16 header: 0xDE NN NN
		payload.write(0xDE);
		payload.write(0x00);
		payload.write(16);
		writeBytes(payload, pair(intKey(1), fixint(7)));
		writeBytes(payload, pair(intKey(2), fixstr("boom")));
		for (int i = 0; i < 14; i++) {
			// unknown key, uint8 (0xCC NN), value nil (0xC0)
			payload.write(0xCC);
			payload.write(100 + i);
			payload.write(0xC0);
		}
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("boom (subcode=7)", rp.serverMessage);
	}

	@Test
	public void parsesMap32Header() {
		// 0xDF + 4-byte big-endian count. Only 2 entries here — exercising the
		// header path, not the count.
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0xDF);
		writeInt(payload, 2);
		writeBytes(payload, pair(intKey(1), fixint(9)));
		writeBytes(payload, pair(intKey(2), fixstr("m32")));
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("m32 (subcode=9)", rp.serverMessage);
	}

	@Test
	public void parsesStr32Message() {
		// 100-char message that we choose to encode with str32 to verify that path works.
		String big = repeat('x', 100);
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82); // fixmap, 2 entries
		writeBytes(payload, pair(intKey(1), fixint(5)));
		// key=2
		writeBytes(payload, intKey(2));
		// str32 prefix
		payload.write(0xDB);
		payload.write(0x00);
		payload.write(0x00);
		payload.write(0x00);
		payload.write(big.length());
		writeBytes(payload, big.getBytes(StandardCharsets.UTF_8));

		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals(big + " (subcode=5)", rp.serverMessage);
	}

	@Test
	public void parsesSubcodeAsFixint() {
		// 0..127 — encoded as a single positive-fixint byte.
		byte[] detail = fixmap2(
			pair(intKey(1), fixint(127)),
			pair(intKey(2), fixstr("fx"))
		);
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertEquals("fx (subcode=127)", rp.serverMessage);
	}

	@Test
	public void parsesSubcodeAsUint8() {
		// 200 doesn't fit in fixint (max 127) so server would emit uint8 (0xCC NN).
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		writeBytes(payload, intKey(1));
		payload.write(0xCC);
		payload.write(200);
		writeBytes(payload, pair(intKey(2), fixstr("u8")));
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("u8 (subcode=200)", rp.serverMessage);
	}

	@Test
	public void parsesSubcodeAsUint16() {
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		// key=1, subcode uint16 = 1100 (0x044C)
		writeBytes(payload, intKey(1));
		payload.write(0xCD);
		payload.write(0x04);
		payload.write(0x4C);
		writeBytes(payload, pair(intKey(2), fixstr("hi")));

		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("hi (subcode=1100)", rp.serverMessage);
	}

	@Test
	public void parsesSubcodeAsUint32() {
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		// key=1, subcode uint32 = 70000 (0x00011170)
		writeBytes(payload, intKey(1));
		payload.write(0xCE);
		payload.write(0x00);
		payload.write(0x01);
		payload.write(0x11);
		payload.write(0x70);
		writeBytes(payload, pair(intKey(2), fixstr("x")));

		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("x (subcode=70000)", rp.serverMessage);
	}

	@Test
	public void parsesSubcodeAsUint64() {
		// 5,000,000,000 doesn't fit in uint32; forces the 0xCF / 8-byte path.
		long value = 5_000_000_000L;
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		writeBytes(payload, intKey(1));
		payload.write(0xCF);
		writeLong(payload, value);
		writeBytes(payload, pair(intKey(2), fixstr("u64")));
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("u64 (subcode=" + value + ")", rp.serverMessage);
	}

	@Test
	public void parsesMessageAsStr8() {
		// 0xD9 + 1-byte length. Server may pick str8 even for short strings;
		// the parser must accept whichever encoding it gets.
		String msg = "string8";
		byte[] data = msg.getBytes(StandardCharsets.UTF_8);
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		writeBytes(payload, pair(intKey(1), fixint(3)));
		writeBytes(payload, intKey(2));
		payload.write(0xD9);
		payload.write(data.length);
		writeBytes(payload, data);
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals(msg + " (subcode=3)", rp.serverMessage);
	}

	@Test
	public void parsesMessageAsStr16() {
		// 0xDA + 2-byte length.
		String msg = "string16";
		byte[] data = msg.getBytes(StandardCharsets.UTF_8);
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x82);
		writeBytes(payload, pair(intKey(1), fixint(4)));
		writeBytes(payload, intKey(2));
		payload.write(0xDA);
		writeShort(payload, data.length);
		writeBytes(payload, data);
		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals(msg + " (subcode=4)", rp.serverMessage);
	}

	// ---------- Parser: defensive/edge cases ----------

	@Test
	public void emptyMapProducesNoMessage() {
		// 0x80 = fixmap, 0 entries → parseErrorDetails returns null.
		RecordParser rp = parserFor(new byte[]{(byte)0x80});
		rp.parseFields(null, null, false);
		assertNull(rp.serverMessage);
	}

	@Test
	public void truncatedValueReturnsNullNotThrow() {
		// fixmap-1, key=1, uint16 prefix — but value bytes are missing.
		// Parser must return null and MUST NOT throw or read past the buffer.
		byte[] detail = new byte[]{(byte)0x81, 0x01, (byte)0xCD};
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertNull(rp.serverMessage);
	}

	@Test
	public void truncatedMapHeaderReturnsNull() {
		// 0xDE (map16 prefix) with NO count bytes following.
		byte[] detail = new byte[]{(byte)0xDE};
		RecordParser rp = parserFor(detail);
		rp.parseFields(null, null, false);
		assertNull(rp.serverMessage);
	}

	@Test
	public void unknownKeysAreSkippedNotFatal() {
		// 4-entry fixmap: unknown int, subcode, unknown nil, message
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		payload.write(0x84); // fixmap, 4 entries
		writeBytes(payload, pair(intKey(50), fixint(0))); // unknown key 50 → fixint value
		writeBytes(payload, pair(intKey(1), fixint(7)));
		writeBytes(payload, intKey(51));
		payload.write(0xC0); // nil
		writeBytes(payload, pair(intKey(2), fixstr("z")));

		RecordParser rp = parserFor(payload.toByteArray());
		rp.parseFields(null, null, false);
		assertEquals("z (subcode=7)", rp.serverMessage);
	}

	@Test
	public void parseSucceedsWhenAdditionalNonErrorFieldsPresent() {
		// Build buffer with fieldCount=2: one bogus field type, then ERROR_MESSAGE.
		byte[] detail = fixmap2(
			pair(intKey(1), fixint(1)),
			pair(intKey(2), fixstr("ok"))
		);
		byte[] buffer = bufferWithFields(
			new int[]{0xCD, FieldType.ERROR_MESSAGE},   // first field type is unknown to parseFieldsError → skipped
			new byte[][]{new byte[]{0x01, 0x02, 0x03}, detail}
		);
		RecordParser rp = new RecordParser(buffer, 0, buffer.length);
		rp.parseFields(null, null, false);
		assertEquals("ok (subcode=1)", rp.serverMessage);
	}

	@Test
	public void missingErrorFieldYieldsNullMessage() {
		// fieldCount = 0
		byte[] buffer = bufferWithFields(new int[0], new byte[0][]);
		RecordParser rp = new RecordParser(buffer, 0, buffer.length);
		rp.parseFields(null, null, false);
		assertNull(rp.serverMessage);
	}

	// ---------- helpers: wire format & msgpack composition ----------

	/**
	 * Build a buffer containing a single ERROR_MESSAGE field with the given msgpack
	 * payload, then construct a parser sitting at the right offset.
	 */
	private static RecordParser parserFor(byte[] msgpackDetail) {
		byte[] buf = bufferWithFields(
			new int[]{FieldType.ERROR_MESSAGE},
			new byte[][]{msgpackDetail}
		);
		// Async constructor expects receiveSize >= MSG_REMAINING_HEADER_SIZE (22).
		return new RecordParser(buf, 0, buf.length);
	}

	/**
	 * Lay out: [5-byte proto stub][22-byte msg header with fieldCount=N][fields...]
	 */
	private static byte[] bufferWithFields(int[] fieldTypes, byte[][] fieldDatas) {
		assertEquals(fieldTypes.length, fieldDatas.length);

		// Msg-header layout per RecordParser async ctor (22 bytes from buf[0]):
		//   bytes 0..4   : header-size marker + 4 attr bytes (parser skips via offset += 5)
		//   byte  5      : result code
		//   bytes 6..9   : generation
		//   bytes 10..13 : expiration
		//   bytes 14..17 : skipped (parser does offset += 8 after expiration)
		//   bytes 18..19 : fieldCount
		//   bytes 20..21 : opCount
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (int i = 0; i < 5; i++) out.write(0);  // size marker + 4 attrs
		out.write(0);                              // result code
		for (int i = 0; i < 4; i++) out.write(0);  // generation
		for (int i = 0; i < 4; i++) out.write(0);  // expiration
		for (int i = 0; i < 4; i++) out.write(0);  // skip
		writeShort(out, fieldTypes.length);        // fieldCount
		writeShort(out, 0);                        // opCount

		for (int i = 0; i < fieldTypes.length; i++) {
			byte[] data = fieldDatas[i];
			int size = data.length + 1; // includes type byte
			writeInt(out, size);
			out.write(fieldTypes[i] & 0xFF);
			writeBytes(out, data);
		}
		return out.toByteArray();
	}

	private static byte[] fixmap1(byte[] kv) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(0x81);
		writeBytes(out, kv);
		return out.toByteArray();
	}

	private static byte[] fixmap2(byte[] kv1, byte[] kv2) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(0x82);
		writeBytes(out, kv1);
		writeBytes(out, kv2);
		return out.toByteArray();
	}

	private static byte[] pair(byte[] k, byte[] v) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		writeBytes(out, k);
		writeBytes(out, v);
		return out.toByteArray();
	}

	private static byte[] intKey(int v) {
		// positive fixint (0–127)
		assertTrue(v >= 0 && v <= 0x7F);
		return new byte[]{(byte)v};
	}

	private static byte[] fixint(int v) {
		assertTrue(v >= 0 && v <= 0x7F);
		return new byte[]{(byte)v};
	}

	private static byte[] fixstr(String s) {
		byte[] data = s.getBytes(StandardCharsets.UTF_8);
		assertTrue("fixstr supports up to 31 bytes", data.length <= 31);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(0xA0 | data.length);
		writeBytes(out, data);
		return out.toByteArray();
	}

	private static void writeBytes(ByteArrayOutputStream out, byte[] b) {
		try {
			out.write(b);
		}
		catch (java.io.IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void writeShort(ByteArrayOutputStream out, int v) {
		out.write((v >> 8) & 0xFF);
		out.write(v & 0xFF);
	}

	private static void writeInt(ByteArrayOutputStream out, int v) {
		out.write((v >> 24) & 0xFF);
		out.write((v >> 16) & 0xFF);
		out.write((v >> 8) & 0xFF);
		out.write(v & 0xFF);
	}

	private static void writeLong(ByteArrayOutputStream out, long v) {
		out.write((int)((v >> 56) & 0xFF));
		out.write((int)((v >> 48) & 0xFF));
		out.write((int)((v >> 40) & 0xFF));
		out.write((int)((v >> 32) & 0xFF));
		out.write((int)((v >> 24) & 0xFF));
		out.write((int)((v >> 16) & 0xFF));
		out.write((int)((v >> 8) & 0xFF));
		out.write((int)(v & 0xFF));
	}

	private static String repeat(char c, int n) {
		char[] arr = new char[n];
		java.util.Arrays.fill(arr, c);
		return new String(arr);
	}

	// Ensure assertNotNull is referenced (used for nullity checks in helpers).
	@SuppressWarnings("unused")
	private static void unused() { assertNotNull(""); }
}
