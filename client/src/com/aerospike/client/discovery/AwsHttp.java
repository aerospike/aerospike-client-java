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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Minimal HTTP/1.1 client with every connect, handshake and read bounded by one deadline. One
 * connection per request, closed when the response is read, so no connection is pooled and no
 * thread is started. {@code HttpURLConnection} is not used because it returns connections to a
 * keep-alive cache whose timer thread it starts on its own.
 */
final class AwsHttp {
	static final int MAX_BODY = 16 * 1024 * 1024;

	private AwsHttp() {
	}

	static final class Response {
		final int status;
		final Map<String,String> headers;
		final byte[] body;

		Response(int status, Map<String,String> headers, byte[] body) {
			this.status = status;
			this.headers = headers;
			this.body = body;
		}

		String text() {
			return new String(body, StandardCharsets.UTF_8);
		}
	}

	/**
	 * Return the Host header value for the URI.
	 */
	static String host(URI uri) {
		return (uri.getPort() < 0)? uri.getHost() : uri.getHost() + ':' + uri.getPort();
	}

	/**
	 * Send one request and read the response. Header names must be lower case; Host is added when
	 * absent. {@code deadline} is a {@link System#nanoTime()} value.
	 *
	 * @throws SocketTimeoutException	if the deadline passes
	 * @throws IOException				on any other I/O or protocol failure
	 */
	static Response request(String method, URI uri, Map<String,String> headers, byte[] body, long deadline)
		throws IOException {
		boolean tls = "https".equalsIgnoreCase(uri.getScheme());
		String host = uri.getHost();
		int port = (uri.getPort() >= 0)? uri.getPort() : (tls? 443 : 80);
		InetAddress address = InetAddress.getByName(host);
		Socket socket = new Socket();

		try {
			socket.connect(new InetSocketAddress(address, port), remaining(deadline));

			if (tls) {
				SSLSocket ssl = (SSLSocket)((SSLSocketFactory)SSLSocketFactory.getDefault()).createSocket(socket,
					host, port, true);
				SSLParameters params = ssl.getSSLParameters();
				params.setEndpointIdentificationAlgorithm("HTTPS");
				ssl.setSSLParameters(params);
				socket = ssl;
				socket.setSoTimeout(remaining(deadline));
				ssl.startHandshake();
			}

			String path = (uri.getRawPath() == null || uri.getRawPath().isEmpty())? "/" : uri.getRawPath();

			if (uri.getRawQuery() != null) {
				path += '?' + uri.getRawQuery();
			}

			StringBuilder sb = new StringBuilder(512);
			sb.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");

			if (! headers.containsKey("host")) {
				sb.append("host: ").append(host(uri)).append("\r\n");
			}

			for (Map.Entry<String,String> entry : headers.entrySet()) {
				sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\r\n");
			}

			if (body != null) {
				sb.append("content-length: ").append(body.length).append("\r\n");
			}
			sb.append("connection: close\r\n\r\n");

			socket.setSoTimeout(remaining(deadline));
			OutputStream out = socket.getOutputStream();
			out.write(sb.toString().getBytes(StandardCharsets.UTF_8));

			if (body != null) {
				out.write(body);
			}
			out.flush();
			return read(new Reader(socket, deadline));
		}
		finally {
			try {
				socket.close();
			}
			catch (IOException ignore) {
			}
		}
	}

	private static Response read(Reader in) throws IOException {
		String line = in.line();
		String[] fields = line.split(" ", 3);

		if (fields.length < 2 || ! fields[0].startsWith("HTTP/")) {
			throw new ProtocolException("invalid HTTP status line");
		}

		int status;

		try {
			status = Integer.parseInt(fields[1]);
		}
		catch (NumberFormatException e) {
			throw new ProtocolException("invalid HTTP status line");
		}

		Map<String,String> headers = new HashMap<>();

		while (! (line = in.line()).isEmpty()) {
			int i = line.indexOf(':');

			if (i > 0) {
				headers.put(line.substring(0, i).trim().toLowerCase(Locale.ROOT), line.substring(i + 1).trim());
			}
		}

		ByteArrayOutputStream body = new ByteArrayOutputStream();
		String length = headers.get("content-length");

		if ("chunked".equalsIgnoreCase(headers.get("transfer-encoding"))) {
			while (true) {
				String size = in.line();
				int semi = size.indexOf(';');
				int n;

				try {
					n = Integer.parseInt(((semi >= 0)? size.substring(0, semi) : size).trim(), 16);
				}
				catch (NumberFormatException e) {
					throw new ProtocolException("invalid HTTP chunk size");
				}

				if (n == 0) {
					while (! in.line().isEmpty()) {
					}
					break;
				}
				in.copy(body, n);
				in.line();
			}
		}
		else if (length != null) {
			int n;

			try {
				n = Integer.parseInt(length);
			}
			catch (NumberFormatException e) {
				throw new ProtocolException("invalid HTTP content-length");
			}
			in.copy(body, n);
		}
		else if (status >= 200 && status != 204 && status != 304) {
			in.copy(body, -1);
		}
		return new Response(status, headers, body.toByteArray());
	}

	/**
	 * Return the milliseconds left before the deadline, rounded up, for a socket timeout.
	 *
	 * @throws SocketTimeoutException	if the deadline has passed
	 */
	static int remaining(long deadline) throws SocketTimeoutException {
		long nanos = deadline - System.nanoTime();

		if (nanos <= 0) {
			throw new SocketTimeoutException("timed out");
		}
		return (int)Math.min(Integer.MAX_VALUE, (nanos + 999_999) / 1_000_000);
	}

	private static final class Reader {
		private final Socket socket;
		private final InputStream in;
		private final long deadline;
		private final byte[] buffer = new byte[8192];
		private int offset;
		private int limit;
		private int total;

		Reader(Socket socket, long deadline) throws IOException {
			this.socket = socket;
			this.in = socket.getInputStream();
			this.deadline = deadline;
		}

		private boolean fill() throws IOException {
			socket.setSoTimeout(remaining(deadline));
			int n = in.read(buffer);

			if (n < 0) {
				return false;
			}
			total += n;

			if (total > MAX_BODY) {
				throw new ProtocolException("HTTP response exceeds " + MAX_BODY + " bytes");
			}
			offset = 0;
			limit = n;
			return true;
		}

		String line() throws IOException {
			ByteArrayOutputStream sb = new ByteArrayOutputStream(128);

			while (true) {
				if (offset == limit && ! fill()) {
					throw new EOFException("HTTP connection closed");
				}

				byte b = buffer[offset++];

				if (b == '\n') {
					break;
				}
				if (b != '\r') {
					sb.write(b);
				}
			}
			return new String(sb.toByteArray(), StandardCharsets.ISO_8859_1);
		}

		void copy(ByteArrayOutputStream out, int length) throws IOException {
			while (length != 0) {
				if (offset == limit && ! fill()) {
					if (length < 0) {
						return;
					}
					throw new EOFException("HTTP connection closed");
				}

				int n = limit - offset;

				if (length > 0 && n > length) {
					n = length;
				}
				out.write(buffer, offset, n);
				offset += n;

				if (length > 0) {
					length -= n;
				}
			}
		}
	}
}
