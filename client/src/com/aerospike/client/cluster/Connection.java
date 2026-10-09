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
package com.aerospike.client.cluster;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.naming.directory.Attribute;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.x500.X500Principal;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

/**
 * Socket connection wrapper.
 */
public final class Connection implements Closeable {
	private static final Set<String> legacyWarned = ConcurrentHashMap.newKeySet();
	private static final Pattern DOTTED_QUAD = Pattern.compile("[0-9]+(\\.[0-9]+){3}");

	// GeneralName tags returned by X509Certificate.getSubjectAlternativeNames() (RFC 5280).
	private static final int SAN_DNS_NAME = 2;
	private static final int SAN_IP_ADDRESS = 7;

	private static final int IPV4_LENGTH = 4;
	private static final int IPV6_LENGTH = 16;
	private static final int IPV4_MAX_OCTET = 255;
	private static final int IPV4_MAX_DIGITS = 3;
	private static final int IPV6_MAX_DIGITS = 4;

	private final Socket socket;
	private final InputStream in;
	private final OutputStream out;
	protected final Pool pool;
	private volatile long lastUsed;

	public Connection(InetSocketAddress address, int timeoutMillis) throws AerospikeException.Connection {
		this(address, timeoutMillis, null, null);
	}

	public Connection(InetSocketAddress address, int timeoutMillis, Node node, Pool pool) throws AerospikeException.Connection {
		this.pool = pool;

		try {
			socket = new Socket();

			try {
				socket.setTcpNoDelay(true);

				if (timeoutMillis > 0) {
					socket.setSoTimeout(timeoutMillis);
				}
				else {
					// Do not wait indefinitely on connection if no timeout is specified.
					// Retry functionality will attempt to reconnect later.
					timeoutMillis = 2000;
				}
				socket.connect(address, timeoutMillis);
				in = socket.getInputStream();
				out = socket.getOutputStream();
				lastUsed = System.nanoTime();
			}
			catch (Throwable e) {
				// socket.close() will close input/output streams according to doc.
				socket.close();

				if (node != null) {
					node.incrErrorRate();
				}
				throw e;
			}
		}
		catch (AerospikeException ae) {
			throw ae;
		}
		catch (Throwable e) {
			throw new AerospikeException.Connection(e);
		}
	}

	public Connection(TlsPolicy policy, String tlsName, InetSocketAddress address, int timeoutMillis) throws AerospikeException.Connection {
		this(policy, tlsName, address, timeoutMillis, null, null);
	}

	public Connection(TlsPolicy policy, String tlsName, InetSocketAddress address, int timeoutMillis, Node node, Pool pool) throws AerospikeException.Connection {
		this.pool = pool;

		try {
			SSLSocketFactory sslsocketfactory = (policy.context != null) ?
					policy.context.getSocketFactory() :
					(SSLSocketFactory)SSLSocketFactory.getDefault();
			SSLSocket sslSocket = (SSLSocket)sslsocketfactory.createSocket();
			socket = sslSocket;

			try {
				socket.setTcpNoDelay(true);

				if (timeoutMillis > 0) {
					socket.setSoTimeout(timeoutMillis);
				}
				else {
					// Do not wait indefinitely on connection if no timeout is specified.
					// Retry functionality will attempt to reconnect later.
					timeoutMillis = 2000;
				}

				/*
				String[] protocols = sslSocket.getSupportedProtocols();
				for (String protocol : protocols) {
					Log.info("Protocol: " + protocol);
				}
				String[] ciphers = sslSocket.getSupportedCipherSuites();
				for (String cipher : ciphers) {
					Log.info("Cipher: " + cipher);
				}
				*/

				if (policy.protocols != null) {
					sslSocket.setEnabledProtocols(policy.protocols);
				}

				if (policy.ciphers != null) {
					sslSocket.setEnabledCipherSuites(policy.ciphers);
				}

				sslSocket.setUseClientMode(true);
				sslSocket.connect(address, timeoutMillis);
				sslSocket.startHandshake();
				X509Certificate cert = (X509Certificate)sslSocket.getSession().getPeerCertificates()[0];
				validateServerCertificate(policy, tlsName, cert);

				in = socket.getInputStream();
				out = socket.getOutputStream();
				lastUsed = System.nanoTime();
			}
			catch (Throwable e) {
				// socket.close() will close input/output streams according to doc.
				socket.close();

				if (node != null) {
					node.incrErrorRate();
				}
				throw e;
			}
		}
		catch (AerospikeException ae) {
			throw ae;
		}
		catch (Throwable e) {
			throw new AerospikeException.Connection(e);
		}
	}

	/**
	 * Validate server certificate against the node's tlsName (the reference identifier).
	 * <p>
	 * If tlsName is an IPv4 or IPv6 literal, it is an IP-ID and matches only an iPAddress
	 * subject alternative name holding the same address (compared as address bytes).
	 * Otherwise it is a DNS-ID and matches only a dNSName subject alternative name, compared
	 * ASCII case-insensitive. The only wildcard is a left-most label of exactly "*", which
	 * matches one non-empty label: "*.example.com" matches "a.example.com", but not
	 * "example.com" or "a.b.example.com". The subject CN is not an identifier (RFC 9525).
	 * <p>
	 * Deprecated legacy fallback: if those rules find no match, the certificate is still
	 * accepted when tlsName exactly equals (case-sensitive) a subject CN or a dNSName, and a
	 * warning is logged once per tlsName. This fallback is not RFC 9525 conformant and will be
	 * removed in the next major release, when such certificates will be rejected.
	 * <p>
	 * Certificates whose serial number is in {@link TlsPolicy#revokeCertificates} are rejected.
	 *
	 * @throws AerospikeException	if the certificate is rejected. This is deliberately not
	 *								{@link AerospikeException.Connection}, so it is not retried.
	 */
	public static void validateServerCertificate(TlsPolicy policy, String tlsName, X509Certificate cert) throws Exception {
		if (tlsName == null) {
			// Do not throw AerospikeException.Connection because that exception will be retried.
			// We don't want to retry on TLS errors. Throw standard AerospikeException instead.
			throw new AerospikeException("Invalid TLS name: null");
		}

		// Exclude certificate serial numbers.
		if (policy.revokeCertificates != null) {
			BigInteger serialNumber = cert.getSerialNumber();

			for (BigInteger sn : policy.revokeCertificates) {
				if (sn.equals(serialNumber)) {
					throw new AerospikeException("Invalid certificate serial number: " + sn);
				}
			}
		}

		// Search for subject alternative names.
		Collection<List<?>> allNames;

		try {
			allNames = cert.getSubjectAlternativeNames();
		}
		catch (CertificateParsingException cpe) {
			allNames = null;
		}

		if (allNames != null) {
			byte[] ip = parseIpLiteral(tlsName);
			boolean ipId = ip != null || isIpId(tlsName);

			for (List<?> list : allNames) {
				int type = (Integer)list.get(0);

				if (ipId) {
					if (ip != null && type == SAN_IP_ADDRESS && Arrays.equals(ip, parseIpLiteral((String)list.get(1)))) {
						return;
					}
				}
				else if (type == SAN_DNS_NAME && matchDnsName((String)list.get(1), tlsName)) {
					return;
				}
			}
		}

		// Deprecated legacy fallback. Remove in the next major release.
		if (matchLegacyName(tlsName, cert, allNames)) {
			if (Log.warnEnabled() && legacyWarned.add(tlsName)) {
				Log.warn("TLS name '" + tlsName + "' matched the server certificate only by the legacy rule " +
					"(subject CN or IP address as dNSName). This is not RFC 9525 conformant and will be " +
					"rejected in the next major release.");
			}
			return;
		}

		throw new AerospikeException("Invalid TLS name: " + tlsName);
	}

	private static boolean matchLegacyName(String tlsName, X509Certificate cert, Collection<List<?>> allNames) throws Exception {
		// Search for subject certificate name.
		String subject = cert.getSubjectX500Principal().getName(X500Principal.RFC2253);
		LdapName ldapName = new LdapName(subject);

		for (Rdn rdn : ldapName.getRdns()) {
			Attribute cn = rdn.toAttributes().get("CN");

			if (cn != null) {
				String certName = (String)cn.get();

				/*
				if (Log.debugEnabled()) {
					Log.debug("Cert name: " + certName);
				}
				*/

				if (certName.equals(tlsName)) {
					return true;
				}
			}
		}

		// Search for exact dNSName.
		if (allNames != null) {
			for (List<?> list : allNames) {
				int type = (Integer)list.get(0);

				if (type == SAN_DNS_NAME && list.get(1).equals(tlsName)) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean matchDnsName(String pattern, String name) {
		if (pattern.startsWith("*.")) {
			if (pattern.indexOf('*', 1) >= 0) {
				return false;
			}

			int dot = name.indexOf('.');
			return dot > 0 && equalsIgnoreCaseAscii(pattern.substring(1), name.substring(dot));
		}
		return pattern.indexOf('*') < 0 && equalsIgnoreCaseAscii(pattern, name);
	}

	private static boolean equalsIgnoreCaseAscii(String a, String b) {
		if (a.length() != b.length()) {
			return false;
		}

		for (int i = 0; i < a.length(); i++) {
			char x = a.charAt(i);
			char y = b.charAt(i);

			if (x != y && toLowerAscii(x) != toLowerAscii(y)) {
				return false;
			}
		}
		return true;
	}

	private static char toLowerAscii(char c) {
		return (c >= 'A' && c <= 'Z') ? (char)(c + ('a' - 'A')) : c;
	}

	/**
	 * Parse IPv4 or IPv6 literal without name resolution. Return null if not a literal.
	 */
	private static byte[] parseIpLiteral(String s) {
		if (s.length() > 2 && s.charAt(0) == '[' && s.charAt(s.length() - 1) == ']') {
			return parseIPv6(s.substring(1, s.length() - 1));
		}
		return (s.indexOf(':') >= 0) ? parseIPv6(s) : parseIPv4(s);
	}

	/**
	 * Names that are IP literals in form but have no unambiguous address (scoped IPv6,
	 * IPv4 with leading zeros) are IP-IDs that match no certificate identifier.
	 */
	private static boolean isIpId(String s) {
		return s.indexOf(':') >= 0 || DOTTED_QUAD.matcher(s).matches();
	}

	private static byte[] parseIPv4(String s) {
		String[] parts = s.split("\\.", -1);

		if (parts.length != IPV4_LENGTH) {
			return null;
		}

		byte[] addr = new byte[IPV4_LENGTH];

		for (int i = 0; i < IPV4_LENGTH; i++) {
			if (parts[i].length() > 1 && parts[i].charAt(0) == '0') {
				return null;
			}

			int v = parseDigits(parts[i], IPV4_MAX_DIGITS, 10);

			if (v < 0 || v > IPV4_MAX_OCTET) {
				return null;
			}
			addr[i] = (byte)v;
		}
		return addr;
	}

	private static byte[] parseIPv6(String s) {
		int dc = s.indexOf("::");
		byte[] head;
		byte[] tail;

		if (dc >= 0) {
			if (s.indexOf("::", dc + 1) >= 0) {
				return null;
			}

			String h = s.substring(0, dc);

			if (h.indexOf('.') >= 0) {
				return null;
			}
			head = parseIPv6Groups(h);
			tail = parseIPv6Groups(s.substring(dc + 2));
		}
		else {
			head = parseIPv6Groups(s);
			tail = new byte[0];
		}

		if (head == null || tail == null) {
			return null;
		}

		int len = head.length + tail.length;

		// "::" must stand for at least one 16-bit group.
		if ((dc >= 0) ? len > IPV6_LENGTH - 2 : len != IPV6_LENGTH) {
			return null;
		}

		byte[] addr = new byte[IPV6_LENGTH];
		System.arraycopy(head, 0, addr, 0, head.length);
		System.arraycopy(tail, 0, addr, IPV6_LENGTH - tail.length, tail.length);
		return addr;
	}

	private static byte[] parseIPv6Groups(String s) {
		if (s.isEmpty()) {
			return new byte[0];
		}

		String[] groups = s.split(":", -1);
		byte[] out = new byte[groups.length * 2 + 2];
		int n = 0;

		for (int i = 0; i < groups.length; i++) {
			String g = groups[i];

			if (i == groups.length - 1 && g.indexOf('.') >= 0) {
				byte[] v4 = parseIPv4(g);

				if (v4 == null) {
					return null;
				}
				System.arraycopy(v4, 0, out, n, IPV4_LENGTH);
				n += IPV4_LENGTH;
				break;
			}

			int v = parseDigits(g, IPV6_MAX_DIGITS, 16);

			if (v < 0) {
				return null;
			}
			out[n++] = (byte)(v >> 8);
			out[n++] = (byte)v;
		}
		return Arrays.copyOf(out, n);
	}

	private static int parseDigits(String s, int maxLength, int radix) {
		if (s.isEmpty() || s.length() > maxLength) {
			return -1;
		}

		int v = 0;

		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			int d;

			if (c >= '0' && c <= '9') {
				d = c - '0';
			}
			else if (radix == 16 && c >= 'a' && c <= 'f') {
				d = c - 'a' + 10;
			}
			else if (radix == 16 && c >= 'A' && c <= 'F') {
				d = c - 'A' + 10;
			}
			else {
				return -1;
			}
			v = v * radix + d;
		}
		return v;
	}

	public void write(byte[] buffer, int length) throws IOException {
		// Never write more than 8 KB at a time.  Apparently, the jni socket write does an extra
		// malloc and free if buffer size > 8 KB.
		final int max = length;
		int pos = 0;
		int len;

		while (pos < max) {
			len = max - pos;

			if (len > 8192)
				len = 8192;

			out.write(buffer, pos, len);
			pos += len;
		}
	}

	public void readFully(byte[] buffer, int length) throws IOException {
		int pos = 0;

		while (pos < length) {
			int	count = in.read(buffer, pos, length - pos);

			if (count < 0)
				throw new EOFException();

			pos += count;
		}
	}

	public void readFully(byte[] buffer, int length, byte state) throws IOException {
		int offset = 0;
		int count = 0;

		while (offset < length) {
			try {
				count = in.read(buffer, offset, length - offset);
			}
			catch (SocketTimeoutException ste) {
				throw new ReadTimeout(buffer, offset, length, state);
			}

			if (count < 0) {
				throw new EOFException();
			}
			offset += count;
		}
	}

	public int read(byte[] buffer, int pos, int length) throws IOException {
		return in.read(buffer, pos, length);
	}

	/**
	 * Is socket closed from client perspective only.
	 */
	public boolean isClosed() {
		return lastUsed == 0;
	}

	public void setTimeout(int timeout) throws SocketException {
		socket.setSoTimeout(timeout);
	}

	public InputStream getInputStream() {
		return in;
	}

	public long getLastUsed() {
		return lastUsed;
	}

	public void updateLastUsed() {
		lastUsed = System.nanoTime();
	}

	/**
	 * Close socket and associated streams.
	 */
	public void close() {
		lastUsed = 0;

		try {
			in.close();
			out.close();
			socket.close();
		}
		catch (Throwable e) {
			if (Log.errorEnabled()) {
				Log.error("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}

	public static final class ReadTimeout extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public final byte[] buffer;
		public final int offset;
		public final int length;
		public final byte state;

		public ReadTimeout(byte[] buffer, int offset, int length, byte state)  {
			super("timeout");
			this.buffer = buffer;
			this.offset = offset;
			this.length = length;
			this.state = state;
		}
	}
}
