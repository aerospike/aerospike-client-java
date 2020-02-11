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
package com.aerospike.client.policy;

import java.math.BigInteger;

import javax.net.ssl.SSLContext;

/**
 * TLS connection policy.  Secure TLS connections are supported for
 * synchronous commands and netty backed asynchronous commands.
 */
public final class TlsPolicy {
	/**
	 * Optional SSLContext configuration instead using default SSLContext.
	 * <p>
	 * Default: null (use default SSLContext).
	 */
	public SSLContext context;

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
	 * <p>
	 * Default: null (Allow default ciphers defined by JVM)
	 */
	public String[] ciphers;

	/**
	 * Reject certificates whose serial numbers match a serial number in this array.
	 * <p>
	 * Default: null (Do not exclude by certificate serial number)
	 */
	public BigInteger[] revokeCertificates;

	/**
	 * Use TLS connections only for login authentication.  All other communication with
	 * the server will be done with non-TLS connections.
	 * <p>
	 * Default: false (Use TLS connections for all communication with server.)
	 */
	public boolean forLoginOnly;

	/**
	 * Copy TLS policy from another TLS policy.
	 */
	public TlsPolicy(TlsPolicy other) {
		this.context = other.context;
		this.protocols = other.protocols;
		this.ciphers = other.ciphers;
		this.revokeCertificates = other.revokeCertificates;
		this.forLoginOnly = other.forLoginOnly;
	}

	/**
	 * Default constructor.
	 */
	public TlsPolicy() {
	}
}
