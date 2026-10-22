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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-process DNS server on a loopback UDP port, for tests. Answers A, AAAA and SRV queries from
 * in-memory records, with an optional additional section; unknown names get NXDOMAIN.
 */
public final class StubDnsServer implements AutoCloseable {
	public static final int A = 1;
	public static final int AAAA = 28;
	public static final int SRV = 33;
	public static final int NOERROR = 0;
	public static final int SERVFAIL = 2;
	public static final int NXDOMAIN = 3;
	public static final int NO_ANSWER = -1;

	private final DatagramSocket socket;
	private final Thread thread;
	private final Map<String,List<byte[]>> answers = new ConcurrentHashMap<>();
	private final Map<String,List<byte[]>> additional = new ConcurrentHashMap<>();
	private final Map<String,Integer> rcodes = new ConcurrentHashMap<>();
	private final List<String> queries = new CopyOnWriteArrayList<>();
	private volatile boolean silent;
	private volatile boolean truncated;
	private ServerSocket tcp;

	public StubDnsServer() throws SocketException {
		this(false);
	}

	/**
	 * @param silent	receive queries and never answer
	 */
	public StubDnsServer(boolean silent) throws SocketException {
		this.silent = silent;
		this.socket = new DatagramSocket(0, InetAddress.getLoopbackAddress());
		this.thread = new Thread(this::run, "stub-dns");
		this.thread.setDaemon(true);
		this.thread.start();
	}

	public String address() {
		return "127.0.0.1:" + socket.getLocalPort();
	}

	public void setSilent(boolean silent) {
		this.silent = silent;
	}

	/**
	 * Answer every query with the truncated flag and no records, so the client retries over TCP.
	 */
	public StubDnsServer truncate() {
		this.truncated = true;
		return this;
	}

	/**
	 * Listen on the same TCP port and never answer. Connections complete in the backlog.
	 */
	public StubDnsServer hangTcp() throws Exception {
		tcp = new ServerSocket(socket.getLocalPort(), 50, InetAddress.getLoopbackAddress());
		return this;
	}

	public StubDnsServer a(String name, String... addresses) throws Exception {
		for (String address : addresses) {
			answer(name, A, InetAddress.getByName(address).getAddress());
		}
		return this;
	}

	public StubDnsServer aaaa(String name, String... addresses) throws Exception {
		for (String address : addresses) {
			answer(name, AAAA, InetAddress.getByName(address).getAddress());
		}
		return this;
	}

	public StubDnsServer srv(String name, int priority, int weight, int port, String target) {
		ByteBuffer bb = ByteBuffer.allocate(6);
		bb.putShort((short)priority).putShort((short)weight).putShort((short)port);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.writeBytes(bb.array());
		out.writeBytes(encodeName(target));
		answer(name, SRV, out.toByteArray());
		return this;
	}

	/**
	 * Add an A record for target to the additional section of every answer for name.
	 */
	public StubDnsServer additionalA(String name, String target, String address) throws Exception {
		additional.computeIfAbsent(key(name, SRV), k -> new CopyOnWriteArrayList<>())
			.add(record(target, A, InetAddress.getByName(address).getAddress()));
		return this;
	}

	/**
	 * Set the response code for one name and type, or {@link #NO_ANSWER} to never answer it.
	 */
	public StubDnsServer rcode(String name, int type, int rcode) {
		rcodes.put(key(name, type), rcode);
		return this;
	}

	public void clear() {
		answers.clear();
		additional.clear();
		rcodes.clear();
	}

	/**
	 * Return the queries received, each as {@code name/type}.
	 */
	public List<String> queries() {
		return queries;
	}

	private void answer(String name, int type, byte[] rdata) {
		answers.computeIfAbsent(key(name, type), k -> new CopyOnWriteArrayList<>()).add(record(name, type, rdata));
	}

	private static String key(String name, int type) {
		String n = name.toLowerCase(Locale.ROOT);
		return (n.endsWith(".")? n : n + '.') + '/' + type;
	}

	private void run() {
		byte[] buf = new byte[512];

		while (! socket.isClosed()) {
			try {
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				ByteBuffer in = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
				short id = in.getShort();
				short flags = in.getShort();
				in.getShort();
				in.getShort();
				in.getShort();
				in.getShort();
				int qstart = in.position();
				String qname = decodeName(in);
				int qtype = in.getShort() & 0xFFFF;
				in.getShort();
				int qend = in.position();
				String key = key(qname, qtype);
				queries.add(key);

				if (silent) {
					continue;
				}

				List<byte[]> an = answers.getOrDefault(key, List.of());
				List<byte[]> ar = additional.getOrDefault(key, List.of());
				Integer rcode = rcodes.get(key);

				if (rcode == null) {
					rcode = (an.isEmpty() && ! hasName(qname))? NXDOMAIN : NOERROR;
				}

				if (rcode == NO_ANSWER) {
					continue;
				}

				if (rcode != NOERROR || truncated) {
					an = List.of();
					ar = List.of();
				}

				ByteArrayOutputStream out = new ByteArrayOutputStream();
				ByteBuffer hdr = ByteBuffer.allocate(12);
				hdr.putShort(id);
				hdr.putShort((short)(0x8000 | 0x0400 | (truncated? 0x0200 : 0) | (flags & 0x0100) | 0x0080 | rcode));
				hdr.putShort((short)1).putShort((short)an.size()).putShort((short)0).putShort((short)ar.size());
				out.writeBytes(hdr.array());
				out.write(packet.getData(), qstart, qend - qstart);

				for (byte[] rr : an) {
					out.writeBytes(rr);
				}
				for (byte[] rr : ar) {
					out.writeBytes(rr);
				}

				byte[] response = out.toByteArray();
				socket.send(new DatagramPacket(response, response.length, packet.getSocketAddress()));
			}
			catch (Exception e) {
				if (socket.isClosed()) {
					return;
				}
			}
		}
	}

	private boolean hasName(String name) {
		String prefix = key(name, 0);
		prefix = prefix.substring(0, prefix.length() - 1);

		for (String key : answers.keySet()) {
			if (key.startsWith(prefix)) {
				return true;
			}
		}
		return false;
	}

	private static byte[] record(String name, int type, byte[] rdata) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.writeBytes(encodeName(name));
		ByteBuffer bb = ByteBuffer.allocate(10);
		bb.putShort((short)type).putShort((short)1).putInt(60).putShort((short)rdata.length);
		out.writeBytes(bb.array());
		out.writeBytes(rdata);
		return out.toByteArray();
	}

	private static byte[] encodeName(String name) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		for (String label : name.split("\\.")) {
			if (label.isEmpty()) {
				continue;
			}
			byte[] bytes = label.getBytes(StandardCharsets.US_ASCII);
			out.write(bytes.length);
			out.writeBytes(bytes);
		}
		out.write(0);
		return out.toByteArray();
	}

	private static String decodeName(ByteBuffer in) {
		List<String> labels = new ArrayList<>();
		int len;

		while ((len = in.get() & 0xFF) != 0) {
			byte[] bytes = new byte[len];
			in.get(bytes);
			labels.add(new String(bytes, StandardCharsets.US_ASCII));
		}
		return String.join(".", labels) + '.';
	}

	@Override
	public void close() throws Exception {
		if (tcp != null) {
			tcp.close();
		}
		socket.close();
		thread.join(1000);
	}
}
