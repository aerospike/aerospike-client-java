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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations
 * under the License.
 */
package com.aerospike.client.cluster;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.aerospike.client.command.Command;

/**
 * Verifies that a command's total deadline bounds a complete socket read, even when the peer
 * periodically sends data before the per-read socket timeout expires.
 */
public class ConnectionReadDeadlineTest {
	@Test
	public void readFullyHonorsTotalDeadlineWhenPeerDripsData() throws Exception {
		final int socketTimeoutMillis = 5000;
		final long totalTimeoutMillis = 800;
		AtomicBoolean running = new AtomicBoolean(true);
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

		Thread dripper = new Thread(() -> {
			try (Socket accepted = server.accept()) {
				OutputStream out = accepted.getOutputStream();

				while (running.get()) {
					out.write(0);
					out.flush();
					Thread.sleep(100);
				}
			}
			catch (Exception e) {
				// The test closes the loopback socket when it finishes.
			}
		});
		dripper.setDaemon(true);
		dripper.start();

		Connection conn = new Connection(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()), socketTimeoutMillis);
		conn.setReadDeadline(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeoutMillis));
		long start = System.nanoTime();

		try {
			conn.readFully(new byte[10_000], 10_000, Command.STATE_READ_DETAIL);
			fail("readFully should time out at the command deadline");
		}
		catch (Connection.ReadTimeout e) {
			long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			assertTrue("dripper should have supplied bytes before the deadline", e.offset > 0);
			assertTrue("command deadline should bound the complete read", elapsedMillis < 2500);
		}
		finally {
			running.set(false);
			conn.close();
			server.close();
			dripper.join(1000);
		}
	}
}
