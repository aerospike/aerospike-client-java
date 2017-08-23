/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.policy;

import java.math.BigInteger;

/**
 * TLS connection policy.
 * Secure TLS connections are only supported for AerospikeClient synchronous commands.
 */
public final class TlsPolicy {
	/**
	 * Allowable TLS protocols that the client can use for secure connections.
	 * Available cipher names can be obtained by {@link javax.net.ssl.SSLSocket#getSupportedProtocols()}
	 * Multiple protocols can be specified. Example:
	 * <pre>
	 * {@code
	 * TlsPolicy policy = new TlsPolicy();
	 * policy.protocols = new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"};
	 * }
	 * </pre>
	 * Default: TLSv1.2 (Only allow TLSv1.2 protocol)
	 */
	public String[] protocols = new String[] {"TLSv1.2"};
	
	/**
	 * Allowable TLS ciphers that the client can use for secure connections.
	 * Available cipher names can be obtained by {@link javax.net.ssl.SSLSocket#getSupportedCipherSuites()}
	 * Multiple ciphers can be specified. 
	 * Default: null (Allow default ciphers defined by JVM)
	 */
	public String[] ciphers;

	/**
	 * Reject certificates whose serial numbers match a serial number in this array.
	 * Default: null (Do not exclude by certificate serial number)
	 */
	public BigInteger[] revokeCertificates;

	/**
	 * Encrypt data on TLS socket only.  Do not authenticate server certificate.
	 * If true, an anonymous cipher (like TLS_DH_anon_WITH_AES_128_CBC_SHA256) should be enabled on
	 * the client.  This anonymous cipher will only work with applications running Java 8+.
	 * <p>
	 * The server should also be configured so anonymous ciphers are allowed.
	 * <pre>
	 * tls-mode encrypt-only
	 * tls-cipher-suite aNULL
	 * </pre>
	 * Default: false
	 */
	public boolean encryptOnly;
	
	/**
	 * Copy TLS policy from another TLS policy.
	 */
	public TlsPolicy(TlsPolicy other) {
		this.protocols = other.protocols;
		this.ciphers = other.ciphers;
		this.revokeCertificates = other.revokeCertificates;
		this.encryptOnly = other.encryptOnly;
	}

	/**
	 * Default constructor.
	 */
	public TlsPolicy() {
	}
}
