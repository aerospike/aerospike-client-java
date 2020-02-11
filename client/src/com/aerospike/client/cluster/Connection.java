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
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
	private final Socket socket;
	private final InputStream in;
	private final OutputStream out;
	protected final Pool pool;
	private final long maxSocketIdle;
	private volatile long lastUsed;

	public Connection(InetSocketAddress address, int timeoutMillis) throws AerospikeException.Connection {
		this(address, timeoutMillis, TimeUnit.SECONDS.toNanos(55), null);
	}

	public Connection(InetSocketAddress address, int timeoutMillis, long maxSocketIdle, Pool pool, Node node) throws AerospikeException.Connection {
		this(address, timeoutMillis, maxSocketIdle, pool);
		node.connsOpened.getAndIncrement();
	}

	public Connection(InetSocketAddress address, int timeoutMillis, long maxSocketIdle, Pool pool) throws AerospikeException.Connection {
		this.maxSocketIdle = maxSocketIdle;
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
			catch (Exception e) {
				// socket.close() will close input/output streams according to doc.
				socket.close();
				throw e;
			}
		}
		catch (AerospikeException ae) {
			throw ae;
		}
		catch (Exception e) {
			throw new AerospikeException.Connection(e);
		}
	}

	public Connection(TlsPolicy policy, String tlsName, InetSocketAddress address, int timeoutMillis, long maxSocketIdle, Pool pool, Node node) throws AerospikeException.Connection {
		this(policy, tlsName, address, timeoutMillis, maxSocketIdle, pool);
		node.connsOpened.getAndIncrement();
	}

	public Connection(TlsPolicy policy, String tlsName, InetSocketAddress address, int timeoutMillis, long maxSocketIdle, Pool pool) throws AerospikeException.Connection {
		this.maxSocketIdle = maxSocketIdle;
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
			catch (Exception e) {
				// socket.close() will close input/output streams according to doc.
				socket.close();
				throw e;
			}
		}
		catch (AerospikeException ae) {
			throw ae;
		}
		catch (Exception e) {
			throw new AerospikeException.Connection(e);
		}
	}

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
					return;
				}
			}
		}

		// Search for subject alternative names.
		Collection<List<?>> allNames = cert.getSubjectAlternativeNames();

		if (allNames != null) {
			for (List<?> list : allNames) {
				int type = (Integer)list.get(0);

				/*
				if (Log.debugEnabled()) {
					Log.debug("SAN " + type + ": " + list.get(1));
				}
				*/

				if (type == 2 && list.get(1).equals(tlsName)) {
					return;
				}
			}
		}

		throw new AerospikeException("Invalid TLS name: " + tlsName);
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
	 * Is socket connected and used within specified limits.
	 */
	public boolean isValid() {
		return (System.nanoTime() - lastUsed) <= maxSocketIdle;
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
	 * Close socket and associated streams after updating node statistics.
	 */
	public void close(Node node) {
		node.connsClosed.getAndIncrement();
		close();
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
		catch (Exception e) {
			if (Log.debugEnabled()) {
				Log.debug("Error closing socket: " + Util.getErrorMessage(e));
			}
		}
	}

	public static final class ReadTimeout extends RuntimeException {
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
