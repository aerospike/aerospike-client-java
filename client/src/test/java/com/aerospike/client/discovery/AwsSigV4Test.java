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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import org.junit.Test;

/**
 * SigV4 against AWS's published examples. Sources:
 * <ul>
 * <li>AWS General Reference, "Examples of how to derive a signing key for Signature Version 4"
 * (secret wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY, 20120215, us-east-1, iam): every
 * intermediate key.</li>
 * <li>AWS General Reference, "Create a canonical request for Signature Version 4", "Create a string
 * to sign for Signature Version 4" and "Calculate the signature for AWS Signature Version 4": the
 * IAM {@code ListUsers} request of 20150830T123600Z, canonical request, its hash, string to sign,
 * signing key and signature.</li>
 * <li>AWS Signature Version 4 test suite ({@code aws-sig-v4-test-suite}), case {@code get-vanilla}:
 * canonical request, string to sign and Authorization header.</li>
 * </ul>
 */
public class AwsSigV4Test {
	private static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	private static final String EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

	@Test
	public void signingKeyDerivationExample() {
		assertEquals("f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d",
			AwsSigV4.hex(AwsSigV4.signingKey(SECRET, "20120215", "us-east-1", "iam")));
	}

	@Test
	public void iamListUsersExample() {
		TreeMap<String,String> headers = new TreeMap<>();
		headers.put("content-type", "application/x-www-form-urlencoded; charset=utf-8");
		headers.put("host", "iam.amazonaws.com");
		headers.put("x-amz-date", "20150830T123600Z");

		String canonical = AwsSigV4.canonicalRequest("GET", "/", "Action=ListUsers&Version=2010-05-08", headers,
			AwsSigV4.hex(AwsSigV4.sha256(new byte[0])));
		String expectedCanonical =
			"GET\n" +
			"/\n" +
			"Action=ListUsers&Version=2010-05-08\n" +
			"content-type:application/x-www-form-urlencoded; charset=utf-8\n" +
			"host:iam.amazonaws.com\n" +
			"x-amz-date:20150830T123600Z\n" +
			"\n" +
			"content-type;host;x-amz-date\n" +
			EMPTY_HASH;
		assertEquals(expectedCanonical, canonical);
		assertEquals("f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
			AwsSigV4.hex(AwsSigV4.sha256(canonical.getBytes(StandardCharsets.UTF_8))));

		String scope = AwsSigV4.scope("20150830", "us-east-1", "iam");
		String stringToSign = AwsSigV4.stringToSign("20150830T123600Z", scope, canonical);
		assertEquals(
			"AWS4-HMAC-SHA256\n" +
			"20150830T123600Z\n" +
			"20150830/us-east-1/iam/aws4_request\n" +
			"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59", stringToSign);

		byte[] key = AwsSigV4.signingKey(SECRET, "20150830", "us-east-1", "iam");
		assertEquals("c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9", AwsSigV4.hex(key));

		String signature = AwsSigV4.signature(key, stringToSign);
		System.out.println("SIGV4 iam ListUsers canonical hash=" +
			AwsSigV4.hex(AwsSigV4.sha256(canonical.getBytes(StandardCharsets.UTF_8))) + " signature=" + signature);
		assertEquals("5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7", signature);
	}

	@Test
	public void testSuiteGetVanilla() {
		TreeMap<String,String> headers = new TreeMap<>();
		headers.put("host", "example.amazonaws.com");
		headers.put("x-amz-date", "20150830T123600Z");

		String canonical = AwsSigV4.canonicalRequest("GET", "/", "", headers, EMPTY_HASH);
		assertEquals(
			"GET\n" +
			"/\n" +
			"\n" +
			"host:example.amazonaws.com\n" +
			"x-amz-date:20150830T123600Z\n" +
			"\n" +
			"host;x-amz-date\n" +
			EMPTY_HASH, canonical);

		String stringToSign = AwsSigV4.stringToSign("20150830T123600Z",
			AwsSigV4.scope("20150830", "us-east-1", "service"), canonical);
		assertEquals(
			"AWS4-HMAC-SHA256\n" +
			"20150830T123600Z\n" +
			"20150830/us-east-1/service/aws4_request\n" +
			"bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63", stringToSign);

		String authorization = AwsSigV4.authorization("AKIDEXAMPLE", SECRET, "GET", "/", "", headers, new byte[0],
			"us-east-1", "service", "20150830T123600Z");
		System.out.println("SIGV4 get-vanilla authorization=" + authorization);
		assertEquals("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
			"SignedHeaders=host;x-amz-date, " +
			"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31", authorization);
	}

	@Test
	public void encodeUnreservedOnly() {
		assertEquals("tag%3Aaerospike-cluster", AwsSigV4.encode("tag:aerospike-cluster"));
		assertEquals("a%20b~c_d.e-f%2F%2B%3D", AwsSigV4.encode("a b~c_d.e-f/+="));
		assertEquals("%C3%A9", AwsSigV4.encode("é"));
	}

	@Test
	public void amzDate() {
		assertEquals("20150830T123600Z", AwsSigV4.amzDate(1440938160000L));
	}
}
