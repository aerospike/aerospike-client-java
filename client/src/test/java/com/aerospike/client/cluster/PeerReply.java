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
package com.aerospike.client.cluster;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.command.Buffer;

/**
 * Run {@link PeerParser} against one canned info reply served on a loopback socket.
 */
final class PeerReply {
	static List<Peer> parse(Cluster cluster, String reply) throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			Thread thread = new Thread(() -> reply(server, reply));
			thread.start();

			Connection conn = new Connection(new InetSocketAddress(server.getInetAddress(), server.getLocalPort()), 5000);
			List<Peer> peers = new ArrayList<>();

			try {
				new PeerParser(cluster, null, conn, peers);
			}
			finally {
				conn.close();
				thread.join();
			}
			return peers;
		}
	}

	private static void reply(ServerSocket server, String reply) {
		try (Socket socket = server.accept()) {
			DataInputStream in = new DataInputStream(socket.getInputStream());
			byte[] header = new byte[8];
			in.readFully(header);
			in.readFully(new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)]);

			byte[] body = reply.getBytes(StandardCharsets.UTF_8);
			byte[] response = new byte[8 + body.length];
			Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), response, 0);
			System.arraycopy(body, 0, response, 8, body.length);

			OutputStream out = socket.getOutputStream();
			out.write(response);
			out.flush();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
