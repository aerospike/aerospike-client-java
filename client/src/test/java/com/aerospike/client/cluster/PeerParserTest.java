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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

public class PeerParserTest {
	private static final String PEERS = "peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1:3001,10.0.0.9]],[BB9C,,[10.0.0.3]],[BB9B,,[10.0.0.2]]]\n";

	private static List<Peer> parse(TranslationMissMode mode) throws Exception {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.ipMap = Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2");
		policy.translationOnMiss = mode;

		try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1));
			ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			Thread thread = new Thread(() -> reply(server));
			thread.start();

			Connection conn = new Connection(new InetSocketAddress(server.getInetAddress(), server.getLocalPort()), 5000);
			List<Peer> peers = new ArrayList<>();

			try {
				PeerParser parser = new PeerParser(client.getCluster(), null, conn, peers);
				assertEquals(5, parser.generation);
			}
			finally {
				conn.close();
				thread.join();
			}
			return peers;
		}
	}

	private static void reply(ServerSocket server) {
		try (Socket socket = server.accept()) {
			DataInputStream in = new DataInputStream(socket.getInputStream());
			byte[] header = new byte[8];
			in.readFully(header);
			in.readFully(new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)]);

			byte[] body = PEERS.getBytes(StandardCharsets.UTF_8);
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

	@Test
	public void strictMissDropsOnlyMissedHost() throws Exception {
		List<Peer> peers = parse(TranslationMissMode.STRICT);

		assertEquals(3, peers.size());
		assertEquals("BB9A", peers.get(0).nodeName);
		assertEquals(List.of(new Host("192.168.1.1", 3001)), peers.get(0).hosts);
		assertEquals("BB9C", peers.get(1).nodeName);
		assertTrue(peers.get(1).hosts.isEmpty());
		assertEquals("BB9B", peers.get(2).nodeName);
		assertEquals(List.of(new Host("192.168.1.2", 3000)), peers.get(2).hosts);
	}

	@Test
	public void passThroughMissKeepsAdvertisedHost() throws Exception {
		List<Peer> peers = parse(TranslationMissMode.PASS_THROUGH);

		assertEquals(3, peers.size());
		assertEquals(List.of(new Host("192.168.1.1", 3001), new Host("10.0.0.9", 3000)), peers.get(0).hosts);
		assertEquals(List.of(new Host("10.0.0.3", 3000)), peers.get(1).hosts);
		assertEquals(List.of(new Host("192.168.1.2", 3000)), peers.get(2).hosts);
	}
}
