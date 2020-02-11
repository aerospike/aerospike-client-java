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

import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;

import gnu.crypto.hash.RipeMD160;
import gnu.crypto.util.Base64;

public final class Crypto {
	/**
	 * Generate unique server hash value from set name, key type and user defined key.
	 * The hash function is RIPEMD-160 (a 160 bit hash).
	 */
	public static byte[] computeDigest(String setName, Value key) {
		// This method runs 14% faster using thread local byte array
		// versus creating the buffer each time.
		byte[] buffer = ThreadLocalData.getBuffer();
		int setLength = Buffer.stringToUtf8(setName, buffer, 0);

		buffer[setLength] = (byte)key.getType();
		int keyLength = key.write(buffer, setLength + 1);

		RipeMD160 hash = new RipeMD160();
		hash.update(buffer, 0, setLength);
		hash.update(buffer, setLength, keyLength + 1);
		return hash.digest();
	}

	/**
	 * Decode base64 bytes into a byte array.
	 */
	public static byte[] decodeBase64(byte[] src, int off, int len) {
		return Base64.decode(src, off, len);
	}

	/**
	 * Encode bytes into a base64 encoded string.
	 */
	public static String encodeBase64(byte[] src) {
		return Base64.encode(src, 0, src.length, false);
	}
}
