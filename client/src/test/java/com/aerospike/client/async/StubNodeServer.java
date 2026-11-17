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
package com.aerospike.client.async;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.command.Buffer;

/**
 * Single Aerospike node on a loopback port, for tests. Answers the info commands the cluster tend
 * needs, owning every partition of one namespace, and answers every data command with an empty
 * success. Data connections can be closed while idle, or on their next request, with FIN or RST.
 */
public final class StubNodeServer implements AutoCloseable {
	public static final String NAMESPACE = "test";

	public enum Close {
		FIN, RST
	}

	private static final int INFO = 1;
	private static final int MSG = 3;

	private final ServerSocket server;
	private final Thread acceptThread;
	private final Map<Socket,Thread> sockets = new ConcurrentHashMap<>();
	private final List<Socket> dataSockets = new CopyOnWriteArrayList<>();
	private final AtomicInteger dataConnections = new AtomicInteger();
	private final AtomicInteger dataRequests = new AtomicInteger();
	private final List<Long> accepts = new CopyOnWriteArrayList<>();
	private volatile String replicas;
	private volatile String nodeName;
	private volatile String peers = "1,3000,[]";
	private volatile int peersGeneration = 1;
	private volatile Close closeOnRequest;
	private volatile boolean resetOnHeader;

	public StubNodeServer() throws IOException {
		this(InetAddress.getLoopbackAddress(), 0, "BB9000000000001");
	}

	/**
	 * Node with the given name on the given local address and port. Port 0 picks a free port.
	 */
	public StubNodeServer(InetAddress address, int port, String nodeName) throws IOException {
		this.nodeName = nodeName;

		setOwnsPartitions(true);
		this.server = new ServerSocket(port, 64, address);

		acceptThread = new Thread(this::accept, "stub-node-accept");
		acceptThread.setDaemon(true);
		acceptThread.start();
	}

	public int port() {
		return server.getLocalPort();
	}

	/**
	 * Set the peers-clear-std reply.
	 */
	public void setPeers(String peers) {
		this.peers = peers;
	}

	/**
	 * Set the node name reply.
	 */
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * Own every partition of the namespace, or none.
	 */
	public void setOwnsPartitions(boolean owns) {
		byte[] bitmap = new byte[4096 / 8];

		if (owns) {
			Arrays.fill(bitmap, (byte)0xFF);
		}
		this.replicas = NAMESPACE + ":0,1," + Base64.getEncoder().encodeToString(bitmap) + ';';
	}

	/**
	 * Set the peers-generation reply.
	 */
	public void setPeersGeneration(int peersGeneration) {
		this.peersGeneration = peersGeneration;
	}

	/**
	 * Number of connections accepted.
	 */
	public int connections() {
		return accepts.size();
	}

	/**
	 * System.nanoTime() of the first connection accepted, or 0 if none was.
	 */
	public long firstAccept() {
		return accepts.isEmpty() ? 0 : accepts.get(0);
	}

	/**
	 * Number of connections that carried a data command.
	 */
	public int dataConnections() {
		return dataConnections.get();
	}

	/**
	 * Number of data commands received.
	 */
	public int dataRequests() {
		return dataRequests.get();
	}

	/**
	 * Close every open data connection now. Used while the client holds them idle in its pool.
	 */
	public void closeDataConnections(Close close) throws IOException {
		for (Socket socket : dataSockets) {
			close(socket, close);
		}
	}

	/**
	 * Close every open connection now, info and data, and keep accepting new ones. Returns once the
	 * threads serving them have ended, so the sockets are closed when it returns.
	 */
	public void closeConnections() throws IOException {
		for (Map.Entry<Socket,Thread> entry : sockets.entrySet()) {
			Socket socket = entry.getKey();
			sockets.remove(socket);
			dataSockets.remove(socket);
			socket.close();
			join(entry.getValue());
		}
	}

	/**
	 * Close the connection of the next data command after reading it, without a response.
	 */
	public void closeOnNextRequest(Close close) {
		closeOnRequest = close;
	}

	/**
	 * Reset the connection of the next data command after reading only its header, so the rest of
	 * a command larger than the socket buffers cannot be sent.
	 */
	public void resetOnNextHeader() {
		resetOnHeader = true;
	}

	private void accept() {
		while (true) {
			Socket socket;

			try {
				socket = server.accept();
			}
			catch (IOException ioe) {
				return;
			}

			accepts.add(System.nanoTime());
			Thread thread = new Thread(() -> serve(socket), "stub-node-conn");
			thread.setDaemon(true);
			sockets.put(socket, thread);
			thread.start();
		}
	}

	private void serve(Socket socket) {
		try {
			DataInputStream in = new DataInputStream(socket.getInputStream());
			OutputStream out = socket.getOutputStream();
			byte[] header = new byte[8];

			while (true) {
				in.readFully(header);
				long proto = Buffer.bytesToLong(header, 0);
				int type = (int)((proto >> 48) & 0xFF);

				if (type == MSG && ! dataSockets.contains(socket)) {
					dataSockets.add(socket);
					dataConnections.incrementAndGet();
				}

				if (type == MSG && resetOnHeader) {
					resetOnHeader = false;
					close(socket, Close.RST);
					return;
				}

				byte[] request = new byte[(int)(proto & 0xFFFFFFFFFFFFL)];
				in.readFully(request);

				byte[] body;

				if (type == MSG) {
					dataRequests.incrementAndGet();

					Close close = closeOnRequest;

					if (close != null) {
						closeOnRequest = null;
						close(socket, close);
						return;
					}
					body = new byte[22];
					body[0] = 22;
				}
				else {
					body = info(request);
				}

				byte[] response = new byte[8 + body.length];
				Buffer.longToBytes(body.length | (2L << 56) | ((long)type << 48), response, 0);
				System.arraycopy(body, 0, response, 8, body.length);
				out.write(response);
				out.flush();
			}
		}
		catch (IOException ioe) {
		}
	}

	private byte[] info(byte[] request) {
		StringBuilder sb = new StringBuilder();

		for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
			if (! command.isEmpty()) {
				sb.append(command).append('\t').append(value(command)).append('\n');
			}
		}
		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}

	private String value(String command) {
		switch (command) {
		case "node":
			return nodeName;
		case "partition-generation":
			return "1";
		case "peers-generation":
			return Integer.toString(peersGeneration);
		case "build":
			return "8.1.0.0";
		case "replicas":
			return replicas;
		case "peers-clear-std":
			return peers;
		default:
			return "ok";
		}
	}

	private void close(Socket socket, Close close) throws IOException {
		dataSockets.remove(socket);

		if (close == Close.RST) {
			socket.setSoLinger(true, 0);
		}
		socket.close();
	}

	private static void join(Thread thread) throws IOException {
		try {
			thread.join();
		}
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new InterruptedIOException();
		}
	}

	/**
	 * Stop listening and close every open connection. Returns once the accept thread and the threads
	 * serving connections have ended, so no connection is accepted or served after it returns.
	 */
	@Override
	public void close() throws IOException {
		server.close();
		join(acceptThread);
		closeConnections();
	}
}
