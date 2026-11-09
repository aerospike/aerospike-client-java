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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.naming.CommunicationException;
import javax.naming.ConfigurationException;
import javax.naming.InvalidNameException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;
import javax.naming.ServiceUnavailableException;

/**
 * DNS queries over UDP, with a TCP retry of a truncated answer, on the calling thread. Servers are
 * tried in order, once each, and values and exceptions are those of the JDK DNS provider
 * ({@code com.sun.jndi.dns.DnsClient}) with {@code com.sun.jndi.dns.timeout.retries} set to 1.
 */
final class DnsResolver {
	static final int A = 1;
	static final int AAAA = 28;
	static final int SRV = 33;

	private static final int DNS_PORT = 53;
	private static final int CLASS_IN = 1;
	private static final int HEADER_SIZE = 12;
	private static final int UDP_SIZE = 8000;
	private static final int MAX_POINTERS = 16;
	private static final String[] RCODES = new String[] {"No error", "DNS format error", "DNS server failure",
		"DNS name not found", "DNS operation not supported", "DNS service refused"};
	private static final SecureRandom RANDOM = new SecureRandom();

	private final InetAddress[] servers;
	private final int[] ports;

	/**
	 * @param providerUrl	space separated {@code dns://host[:port][/domain]} URLs
	 */
	DnsResolver(String providerUrl) throws NamingException {
		String[] urls = providerUrl.trim().split(" +");
		this.servers = new InetAddress[urls.length];
		this.ports = new int[urls.length];

		for (int i = 0; i < urls.length; i++) {
			String server = urls[i];

			if (server.startsWith("dns://")) {
				server = server.substring(6);
			}

			int slash = server.indexOf('/');

			if (slash >= 0) {
				server = server.substring(0, slash);
			}

			if (server.isEmpty()) {
				server = "localhost";
			}

			int colon = server.indexOf(':', server.indexOf(']') + 1);
			String host = (colon < 0)? server : server.substring(0, colon);

			try {
				ports[i] = (colon < 0)? DNS_PORT : Integer.parseInt(server.substring(colon + 1));
				servers[i] = InetAddress.getByName(host);
			}
			catch (NumberFormatException | UnknownHostException e) {
				NamingException ne = new ConfigurationException("Invalid DNS server: " + server);
				ne.setRootCause(e);
				throw ne;
			}
		}
	}

	int serverCount() {
		return servers.length;
	}

	/**
	 * Query an absolute name and return the values of the answer records of the given type, in
	 * answer order without duplicates.
	 *
	 * @param timeout	milliseconds for each UDP and each TCP attempt
	 * @param deadline	{@link System#nanoTime()} past which no attempt runs
	 */
	List<String> query(String name, int type, long timeout, long deadline) throws NamingException {
		int id = RANDOM.nextInt(0x10000);
		byte[] query = encodeQuery(name, type, id);
		boolean[] doNotRetry = new boolean[servers.length];
		Exception error = null;

		for (int i = 0; i < servers.length; i++) {
			if (System.nanoTime() - deadline >= 0) {
				if (error == null) {
					error = new SocketTimeoutException("deadline");
				}
				break;
			}

			try {
				byte[] msg = udp(servers[i], ports[i], query, end(timeout, deadline));

				if ((msg[2] & 0x02) != 0) {
					for (int j = 0; j < servers.length; j++) {
						int k = (i + j) % servers.length;

						if (doNotRetry[k]) {
							continue;
						}

						try {
							msg = tcp(servers[k], ports[k], query, end(timeout, deadline));
							break;
						}
						catch (Exception e) {
						}
					}
				}
				return answers(msg, type);
			}
			catch (IOException e) {
				if (error == null) {
					error = e;
				}
				if (e instanceof PortUnreachableException) {
					doNotRetry[i] = true;
				}
			}
			catch (NameNotFoundException e) {
				throw e;
			}
			catch (CommunicationException e) {
				if (error == null) {
					error = e;
				}
			}
			catch (NamingException e) {
				if (error == null) {
					error = e;
				}
				doNotRetry[i] = true;
			}
		}

		if (error instanceof NamingException) {
			throw (NamingException)error;
		}
		NamingException ne = new CommunicationException("DNS error");
		ne.setRootCause(error);
		throw ne;
	}

	private static long end(long timeout, long deadline) {
		long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
		return (end - deadline < 0)? end : deadline;
	}

	private static int remaining(long end, String message) throws SocketTimeoutException {
		long remaining = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime());

		if (remaining <= 0) {
			throw new SocketTimeoutException(message);
		}
		return (int)Math.min(remaining, Integer.MAX_VALUE);
	}

	private static byte[] udp(InetAddress server, int port, byte[] query, long end) throws IOException, NamingException {
		try (DatagramSocket socket = new DatagramSocket()) {
			socket.connect(server, port);
			socket.send(new DatagramPacket(query, query.length));

			byte[] buf = new byte[UDP_SIZE];

			while (true) {
				socket.setSoTimeout(remaining(end, "Receive timed out"));
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				byte[] msg = Arrays.copyOf(buf, packet.getLength());

				if (isResponse(msg, query)) {
					return msg;
				}
			}
		}
	}

	private static byte[] tcp(InetAddress server, int port, byte[] query, long end) throws IOException, NamingException {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(server, port), remaining(end, "connect timed out"));

			byte[] frame = new byte[query.length + 2];
			frame[0] = (byte)(query.length >> 8);
			frame[1] = (byte)query.length;
			System.arraycopy(query, 0, frame, 2, query.length);

			socket.setSoTimeout(remaining(end, "Read timed out"));
			OutputStream out = socket.getOutputStream();
			out.write(frame);
			out.flush();

			InputStream in = socket.getInputStream();
			byte[] length = read(socket, in, new byte[2], end);
			byte[] msg = read(socket, in, new byte[((length[0] & 0xFF) << 8) | (length[1] & 0xFF)], end);

			if (! isResponse(msg, query)) {
				throw new CommunicationException("DNS error: response does not match the query");
			}
			return msg;
		}
	}

	private static byte[] read(Socket socket, InputStream in, byte[] buf, long end) throws IOException {
		int pos = 0;

		while (pos < buf.length) {
			socket.setSoTimeout(remaining(end, "Read timed out"));
			int count = in.read(buf, pos, buf.length - pos);

			if (count < 0) {
				throw new EOFException("DNS error: no response");
			}
			pos += count;
		}
		return buf;
	}

	/**
	 * Return true when msg answers query: same ID and question, with QR set. Throw on a header
	 * that is too short, QR clear, or an error response code with the query's ID.
	 */
	private static boolean isResponse(byte[] msg, byte[] query) throws NamingException {
		if (msg.length < HEADER_SIZE) {
			throw new CommunicationException("DNS error: corrupted message header");
		}

		if ((msg[2] & 0x80) == 0) {
			throw new CommunicationException("DNS error: expecting response");
		}

		if (msg[0] != query[0] || msg[1] != query[1]) {
			return false;
		}

		checkResponseCode(msg[3] & 0x0F);

		if (u16(msg, 4) != 1) {
			return false;
		}

		StringBuilder qname = new StringBuilder();
		int pos = decodeName(msg, HEADER_SIZE, qname);

		if (pos + 4 > msg.length) {
			throw corrupted();
		}

		StringBuilder name = new StringBuilder();
		int qpos = decodeName(query, HEADER_SIZE, name);

		return u16(msg, pos) == u16(query, qpos) && u16(msg, pos + 2) == u16(query, qpos + 2) &&
			equalsIgnoreAsciiCase(qname, name);
	}

	private static void checkResponseCode(int rcode) throws NamingException {
		if (rcode == 0) {
			return;
		}

		String msg = ((rcode < RCODES.length)? RCODES[rcode] : "DNS error") + " [response code " + rcode + "]";

		switch (rcode) {
		case 2:
			throw new ServiceUnavailableException(msg);
		case 3:
			throw new NameNotFoundException(msg);
		case 4:
		case 5:
			throw new OperationNotSupportedException(msg);
		default:
			throw new NamingException(msg);
		}
	}

	/**
	 * Return the value of each answer record of the given type and class IN, as the JDK DNS
	 * provider formats it, without duplicates. The question, answer and authority sections are
	 * checked; the additional section is not read.
	 */
	private static List<String> answers(byte[] msg, int type) throws NamingException {
		int questions = u16(msg, 4);
		int answers = u16(msg, 6);
		int records = answers + u16(msg, 8);
		int pos = HEADER_SIZE;

		for (int i = 0; i < questions; i++) {
			pos = decodeName(msg, pos, new StringBuilder()) + 4;

			if (pos > msg.length) {
				throw corrupted();
			}
		}

		List<String> values = new ArrayList<>();

		for (int i = 0; i < records; i++) {
			pos = decodeName(msg, pos, new StringBuilder());

			if (pos + 10 > msg.length) {
				throw corrupted();
			}

			int rrtype = u16(msg, pos);
			int rrclass = u16(msg, pos + 2);
			int rdlength = u16(msg, pos + 8);
			pos += 10;

			if (pos + rdlength > msg.length) {
				throw corrupted();
			}

			if (i < answers && rrtype == type && rrclass == CLASS_IN) {
				String value = rdata(msg, type, pos, rdlength);

				if (! values.contains(value)) {
					values.add(value);
				}
			}
			pos += rdlength;
		}
		return values;
	}

	private static String rdata(byte[] msg, int type, int pos, int rdlength) throws NamingException {
		switch (type) {
		case A:
			if (rdlength != 4) {
				throw corrupted();
			}
			return decodeA(msg, pos);

		case AAAA:
			if (rdlength != 16) {
				throw corrupted();
			}
			return decodeAAAA(msg, pos);

		case SRV: {
			if (rdlength < 7) {
				throw corrupted();
			}

			StringBuilder target = new StringBuilder();

			if (decodeName(msg, pos + 6, target) > pos + rdlength) {
				throw corrupted();
			}
			return u16(msg, pos) + " " + u16(msg, pos + 2) + " " + u16(msg, pos + 4) + " " + target;
		}

		default:
			throw new NamingException("Unsupported record type " + type);
		}
	}

	private static String decodeA(byte[] msg, int pos) {
		return (msg[pos] & 0xFF) + "." + (msg[pos + 1] & 0xFF) + "." + (msg[pos + 2] & 0xFF) + "." + (msg[pos + 3] & 0xFF);
	}

	private static String decodeAAAA(byte[] msg, int pos) {
		int[] groups = new int[8];

		for (int i = 0; i < 8; i++) {
			groups[i] = u16(msg, pos + 2 * i);
		}

		// The longest run of two or more zero groups, the first when tied.
		int curBase = -1;
		int curLen = 0;
		int bestBase = -1;
		int bestLen = 0;

		for (int i = 0; i < 8; i++) {
			if (groups[i] == 0) {
				if (curBase == -1) {
					curBase = i;
					curLen = 1;
				}
				else {
					curLen++;

					if (curLen >= 2 && curLen > bestLen) {
						bestBase = curBase;
						bestLen = curLen;
					}
				}
			}
			else {
				curBase = -1;
			}
		}

		if (bestBase == 0) {
			if (bestLen == 6 || (bestLen == 7 && groups[7] > 1)) {
				return "::" + decodeA(msg, pos + 12);
			}
			if (bestLen == 5 && groups[5] == 0xFFFF) {
				return "::ffff:" + decodeA(msg, pos + 12);
			}
		}

		StringBuilder sb = new StringBuilder(40);

		if (bestBase == 0) {
			sb.append(':');
		}

		for (int i = 0; i < 8; i++) {
			if (bestBase == -1 || i < bestBase || i >= bestBase + bestLen) {
				sb.append(Integer.toHexString(groups[i]));

				if (i < 7) {
					sb.append(':');
				}
			}
			else if (i == bestBase) {
				sb.append(':');
			}
		}
		return sb.toString();
	}

	/**
	 * Append the name at pos in the JDK DNS provider's text form (labels joined by dots, with a
	 * trailing dot, and dots and backslashes in labels escaped) and return the position after it.
	 */
	private static int decodeName(byte[] msg, int pos, StringBuilder sb) throws NamingException {
		int end = -1;
		int pointers = 0;

		while (true) {
			if (pos >= msg.length) {
				throw corrupted();
			}

			int len = msg[pos] & 0xFF;

			if (len == 0) {
				pos++;
				break;
			}

			if (len <= 63) {
				if (pos + 1 + len > msg.length) {
					throw corrupted();
				}

				for (int i = pos + 1; i <= pos + len; i++) {
					char c = (char)(msg[i] & 0xFF);

					if (c == '.' || c == '\\') {
						sb.append('\\');
					}
					sb.append(c);
				}
				sb.append('.');
				pos += 1 + len;
			}
			else if ((len & 0xC0) == 0xC0) {
				if (pos + 2 > msg.length || ++pointers > MAX_POINTERS) {
					throw corrupted();
				}

				if (end < 0) {
					end = pos + 2;
				}
				pos = u16(msg, pos) & 0x3FFF;
			}
			else {
				throw corrupted();
			}
		}

		if (sb.length() == 0) {
			sb.append('.');
		}
		return (end < 0)? pos : end;
	}

	/**
	 * Encode a standard query with recursion desired, parsing name as the JDK DNS provider does:
	 * labels separated by dots, {@code \DDD} and {@code \c} escapes.
	 */
	private static byte[] encodeQuery(String name, int type, int id) throws InvalidNameException {
		List<byte[]> labels = new ArrayList<>();
		StringBuilder label = new StringBuilder();
		int octets = 1;

		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);

			if (c == '\\') {
				if (i + 3 < name.length() && isDigits(name, i + 1)) {
					c = (char)Integer.parseInt(name.substring(i + 1, i + 4));
					i += 3;
				}
				else if (i + 1 < name.length()) {
					c = name.charAt(++i);
				}
				else {
					throw new InvalidNameException("Invalid escape sequence in " + name);
				}
				label.append(c);
			}
			else if (c == '.') {
				if (label.length() == 0) {
					throw new InvalidNameException("Bad DNS name: " + name);
				}
				labels.add(toOctets(label, name));
				octets += label.length() + 1;
				label.setLength(0);
			}
			else {
				label.append(c);
			}
		}

		if (label.length() > 0) {
			labels.add(toOctets(label, name));
			octets += label.length() + 1;
		}

		if (octets > 255) {
			throw new InvalidNameException("Name exceeds 255 octets: " + name);
		}

		byte[] query = new byte[HEADER_SIZE + octets + 4];
		query[0] = (byte)(id >> 8);
		query[1] = (byte)id;
		query[2] = 0x01;
		query[5] = 1;

		int pos = HEADER_SIZE;

		for (byte[] bytes : labels) {
			query[pos++] = (byte)bytes.length;
			System.arraycopy(bytes, 0, query, pos, bytes.length);
			pos += bytes.length;
		}
		query[pos++] = 0;
		query[pos++] = (byte)(type >> 8);
		query[pos++] = (byte)type;
		query[pos++] = 0;
		query[pos] = CLASS_IN;
		return query;
	}

	private static boolean isDigits(String s, int pos) {
		for (int i = pos; i < pos + 3; i++) {
			if (! Character.isDigit(s.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	private static byte[] toOctets(StringBuilder label, String name) throws InvalidNameException {
		if (label.length() > 63) {
			throw new InvalidNameException("Label exceeds 63 octets: " + label);
		}

		byte[] bytes = new byte[label.length()];

		for (int i = 0; i < bytes.length; i++) {
			char c = label.charAt(i);

			if (c > 0xFF) {
				throw new InvalidNameException("Label has two-byte char: " + label);
			}
			bytes[i] = (byte)c;
		}
		return bytes;
	}

	private static boolean equalsIgnoreAsciiCase(CharSequence a, CharSequence b) {
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
		return (c >= 'A' && c <= 'Z')? (char)(c + 32) : c;
	}

	private static int u16(byte[] msg, int pos) {
		return ((msg[pos] & 0xFF) << 8) | (msg[pos + 1] & 0xFF);
	}

	private static CommunicationException corrupted() {
		return new CommunicationException("DNS error: corrupted message");
	}
}
