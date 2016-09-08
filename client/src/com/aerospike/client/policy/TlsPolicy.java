/*
 * Copyright 2012-2016 Aerospike, Inc.
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
	 * Default: null (Allow default protocols defined by JVM)
	 */
	public String[] protocols;
	
	/**
	 * Allowable TLS ciphers that the client can use for secure connections.
	 * Available cipher names can be obtained by {@link javax.net.ssl.SSLSocket#getSupportedCipherSuites()}
	 * Multiple ciphers can be specified. 
	 * Default: null (Allow default ciphers defined by JVM)
	 */
	public String[] ciphers;

	/**
	 * Encrypt data on TLS socket only.  Do not authenticate server certificate.
	 * An anonymous cipher must be enabled on both client and server when encryptOnly is enabled.
	 * Default: false
	 */
	public boolean encryptOnly;
}
