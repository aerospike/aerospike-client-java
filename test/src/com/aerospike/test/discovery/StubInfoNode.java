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
package com.aerospike.test.discovery;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.net.ssl.SSLContext;

import com.aerospike.client.command.Buffer;

/**
 * Stub Aerospike node for the L5 driver tests: answers info requests on 127.0.0.1, in the clear or
 * over TLS, from {@link #values}, and records every info command it receives. A command it has no
 * value for gets {@code ok}; any other message closes the connection. The partition generation is
 * 1 and the partition map is empty.
 */
public final class StubInfoNode implements AutoCloseable {
	final Map<String,String> values = new ConcurrentHashMap<>();
	private final List<String> commands = new CopyOnWriteArrayList<>();
	private final List<Socket> sockets = new CopyOnWriteArrayList<>();
	private final ServerSocket server;

	StubInfoNode(String name, SSLContext tls) throws IOException {
		InetAddress loopback = InetAddress.getByName("127.0.0.1");
		server = tls == null ? new ServerSocket(0, 16, loopback) :
			tls.getServerSocketFactory().createServerSocket(0, 16, loopback);
		values.put("node", name);
		values.put("partition-generation", "1");
		values.put("replicas", "");
		values.put("peers-generation", "1");
		values.put("build", "8.1.0.0");

		Thread thread = new Thread(this::accept, "stub-" + name);
		thread.setDaemon(true);
		thread.start();
	}

	int port() {
		return server.getLocalPort();
	}

	List<String> commands() {
		return new ArrayList<>(commands);
	}

	private void accept() {
		while (true) {
			Socket socket;

			try {
				socket = server.accept();
			}
			catch (IOException e) {
				return;
			}
			sockets.add(socket);
			Thread thread = new Thread(() -> serve(socket));
			thread.setDaemon(true);
			thread.start();
		}
	}

	private void serve(Socket socket) {
		try (Socket s = socket) {
			DataInputStream in = new DataInputStream(s.getInputStream());
			OutputStream out = s.getOutputStream();
			byte[] header = new byte[8];

			while (true) {
				in.readFully(header);

				if (header[1] != 1) {
					return;
				}
				byte[] request = new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)];
				in.readFully(request);
				StringBuilder sb = new StringBuilder();

				for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
					if (! command.isEmpty()) {
						commands.add(command);
						sb.append(command).append('\t').append(values.getOrDefault(command, "ok")).append('\n');
					}
				}
				byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
				byte[] response = new byte[8 + body.length];
				Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), response, 0);
				System.arraycopy(body, 0, response, 8, body.length);
				out.write(response);
				out.flush();
			}
		}
		catch (IOException e) {
		}
	}

	@Override
	public void close() throws IOException {
		server.close();

		for (Socket socket : sockets) {
			socket.close();
		}
	}
}
