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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.Assume;
import org.junit.Test;

/**
 * The L5 driver's watch probe against two loopback listeners under one test name. Needs no server.
 */
public class TestL5DriverProbe {
	private static final String HOST = "probe.l5.test";

	@Test
	public void servingAfterThreeRoundsWithEveryAddress() throws Exception {
		InetAddress first = InetAddress.getByName("127.0.0.1");
		InetAddress second = InetAddress.getByName("127.0.0.2");

		if (! canBind(second)) {
			second = InetAddress.getByName("::1");
		}
		Assume.assumeTrue("Neither 127.0.0.2 nor ::1 can be bound locally", canBind(second));

		InetAddress[] addresses = {first, second};
		int port = freePort(first, second);
		L5Driver.Probe probe = new L5Driver.Probe(HOST, port, host -> {
			assertEquals(HOST, host);
			return addresses;
		});

		try (ServerSocket a = new ServerSocket(port, 64, first)) {
			probe.round(0);
			probe.round(10);
			assertNull(probe.servingAt);
			assertEquals(Long.valueOf(10), probe.lastClosedAt);

			try (ServerSocket b = new ServerSocket(port, 64, second)) {
				probe.round(20);
				probe.round(30);
				assertNull(probe.servingAt);
				probe.round(40);
				assertEquals(Long.valueOf(20), probe.servingAt);
				assertEquals(Long.valueOf(10), probe.lastClosedAt);
			}
		}
		assertEquals("[{\"address\":\"" + first.getHostAddress() + "\",\"firstConnectedAt\":0}," +
			"{\"address\":\"" + second.getHostAddress() + "\",\"firstConnectedAt\":20}]", probe.addresses());
	}

	@Test
	public void addressThatNeverConnectsIsNull() throws Exception {
		InetAddress first = InetAddress.getByName("127.0.0.1");
		int port;

		try (ServerSocket socket = new ServerSocket(0, 1, first)) {
			port = socket.getLocalPort();
		}

		L5Driver.Probe probe = new L5Driver.Probe(HOST, port, host -> new InetAddress[] {first});
		probe.round(0);
		probe.round(10);
		assertNull(probe.servingAt);
		assertEquals(Long.valueOf(10), probe.lastClosedAt);
		assertEquals("[{\"address\":\"" + first.getHostAddress() + "\",\"firstConnectedAt\":null}]", probe.addresses());
	}

	private static int freePort(InetAddress first, InetAddress second) throws IOException {
		for (int i = 0; i < 100; i++) {
			int port;

			try (ServerSocket socket = new ServerSocket(0, 1, second)) {
				port = socket.getLocalPort();
			}

			try (ServerSocket socket = new ServerSocket(port, 1, first)) {
				return port;
			}
			catch (BindException be) {
			}
		}
		throw new IOException("No port free on both " + first + " and " + second);
	}

	private static boolean canBind(InetAddress address) {
		try (ServerSocket socket = new ServerSocket(0, 1, address)) {
			return true;
		}
		catch (IOException ioe) {
			return false;
		}
	}
}
