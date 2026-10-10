/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import com.aerospike.client.async.NettyTlsContext;

/**
 * TLS connection policy.  Secure TLS connections are supported for
 * synchronous commands and netty backed asynchronous commands.
 * <p>
 * The server certificate is matched against the node's tlsName (the reference identifier)
 * per RFC 9525:
 * <ul>
 * <li>An IPv4 or IPv6 literal tlsName (IP-ID) matches only an iPAddress subject alternative
 * name holding the same address. It never matches a dNSName, a wildcard or the subject CN.</li>
 * <li>Any other tlsName (DNS-ID) matches only a dNSName subject alternative name. Matching is
 * ASCII case-insensitive.</li>
 * <li>The only wildcard is a left-most label of exactly "*" matching one non-empty label:
 * "*.example.com" matches "a.example.com", but not "example.com" or "a.b.example.com".</li>
 * <li>The subject CN is not an identifier.</li>
 * </ul>
 * Deprecated legacy fallback: when those rules find no match, the certificate is still accepted
 * if tlsName exactly equals (case-sensitive) a subject CN or a dNSName, as in earlier releases,
 * and a warning is logged once per tlsName. This fallback is not RFC 9525 conformant and will be
 * removed in the next major release, when such certificates (for example, CN-only certificates
 * or an IP address present only as a dNSName) will be rejected.
 */
public final class TlsPolicy {
	/**
	 * Optional SSLContext configuration instead using default SSLContext.
	 * <p>
	 * Default: null (use default SSLContext).
	 */
	public SSLContext context;

	/**
	 * Optional NettyTlsContext configuration. This field is used when the same NettyTlsContext
	 * instance needs to be shared between multiple AerospikeClient instances. If this field
	 * is null, the AerospikeClient constructor will create a new NettyTlsContext when netty
	 * eventloops are used with TLS.
	 *
	 * <pre>{@code
	 * // Share NettyTlsContext across AerospikeClient instances.
	 * TlsPolicy tp = new TlsPolicy();
	 * tp.protocols = new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"};
	 * tp.nettyContext = new NettyTlsContext(tp);
	 *
	 * ClientPolicy cp = new ClientPolicy();
	 * cp.tlsPolicy = tp;
	 *
	 * AerospikeClient cluster1 = new AerospikeClient(cp, "host1", 3000);
	 * AerospikeClient cluster2 = new AerospikeClient(cp, "host2", 3000);
	 * }</pre>
	 *
	 * Default: null (create NettyTlsContext for each AerospikeClient instance when netty is used).
	 */
	public NettyTlsContext nettyContext;

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

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setContext(SSLContext context) {
		this.context = context;
	}

	public void setNettyContext(NettyTlsContext nettyContext) {
		this.nettyContext = nettyContext;
	}

	public void setProtocols(String[] protocols) {
		this.protocols = protocols;
	}

	public void setCiphers(String[] ciphers) {
		this.ciphers = ciphers;
	}

	public void setRevokeCertificates(BigInteger[] revokeCertificates) {
		this.revokeCertificates = revokeCertificates;
	}

	public void setForLoginOnly(boolean forLoginOnly) {
		this.forLoginOnly = forLoginOnly;
	}
}
