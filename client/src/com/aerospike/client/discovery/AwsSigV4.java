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
package com.aerospike.client.discovery;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.SortedMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.aerospike.client.AerospikeException;

/**
 * AWS Signature Version 4 signing of one request.
 */
final class AwsSigV4 {
	static final String ALGORITHM = "AWS4-HMAC-SHA256";

	private static final DateTimeFormatter AMZ_DATE =
		DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
	private static final char[] HEX = "0123456789abcdef".toCharArray();

	private AwsSigV4() {
	}

	static String amzDate(long millis) {
		return AMZ_DATE.format(Instant.ofEpochMilli(millis));
	}

	/**
	 * Return the Authorization header value. Header names must be lower case.
	 */
	static String authorization(
		String accessKeyId,
		String secretAccessKey,
		String method,
		String path,
		String query,
		SortedMap<String,String> headers,
		byte[] payload,
		String region,
		String service,
		String amzDate
	) {
		String scope = scope(amzDate.substring(0, 8), region, service);
		String request = canonicalRequest(method, path, query, headers, hex(sha256(payload)));
		String signature = signature(signingKey(secretAccessKey, amzDate.substring(0, 8), region, service),
			stringToSign(amzDate, scope, request));
		return ALGORITHM + " Credential=" + accessKeyId + '/' + scope + ", SignedHeaders=" + signedHeaders(headers) +
			", Signature=" + signature;
	}

	static String canonicalRequest(String method, String path, String query, SortedMap<String,String> headers,
		String payloadHash) {
		StringBuilder sb = new StringBuilder(256);
		sb.append(method).append('\n').append(path).append('\n').append(query).append('\n');

		for (Map.Entry<String,String> entry : headers.entrySet()) {
			sb.append(entry.getKey()).append(':').append(entry.getValue().trim()).append('\n');
		}
		sb.append('\n').append(signedHeaders(headers)).append('\n').append(payloadHash);
		return sb.toString();
	}

	static String signedHeaders(SortedMap<String,String> headers) {
		return String.join(";", headers.keySet());
	}

	static String scope(String date, String region, String service) {
		return date + '/' + region + '/' + service + "/aws4_request";
	}

	static String stringToSign(String amzDate, String scope, String canonicalRequest) {
		return ALGORITHM + '\n' + amzDate + '\n' + scope + '\n' +
			hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
	}

	static byte[] signingKey(String secretAccessKey, String date, String region, String service) {
		byte[] key = hmac(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), date);
		key = hmac(key, region);
		key = hmac(key, service);
		return hmac(key, "aws4_request");
	}

	static String signature(byte[] signingKey, String stringToSign) {
		return hex(hmac(signingKey, stringToSign));
	}

	/**
	 * Percent-encode every byte except the RFC 3986 unreserved characters.
	 */
	static String encode(String value) {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		StringBuilder sb = new StringBuilder(bytes.length + 16);

		for (byte b : bytes) {
			char c = (char)(b & 0xff);

			if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
				c == '-' || c == '_' || c == '.' || c == '~') {
				sb.append(c);
			}
			else {
				sb.append('%').append(Character.toUpperCase(HEX[c >> 4])).append(Character.toUpperCase(HEX[c & 0xf]));
			}
		}
		return sb.toString();
	}

	static String hex(byte[] bytes) {
		char[] chars = new char[bytes.length * 2];

		for (int i = 0; i < bytes.length; i++) {
			chars[i * 2] = HEX[(bytes[i] >> 4) & 0xf];
			chars[i * 2 + 1] = HEX[bytes[i] & 0xf];
		}
		return new String(chars);
	}

	static byte[] sha256(byte[] bytes) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(bytes);
		}
		catch (GeneralSecurityException e) {
			throw new AerospikeException("SHA-256 is unavailable: " + e, e);
		}
	}

	private static byte[] hmac(byte[] key, String data) {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(key, "HmacSHA256"));
			return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
		}
		catch (GeneralSecurityException e) {
			throw new AerospikeException("HmacSHA256 is unavailable: " + e, e);
		}
	}
}
