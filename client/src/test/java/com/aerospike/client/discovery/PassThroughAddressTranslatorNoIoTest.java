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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;

import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.discovery.Endpoint.SourceList;

/**
 * U-2: pass-through {@code translate()} does no name lookup and no socket connect.
 */
public class PassThroughAddressTranslatorNoIoTest {
	@Test
	public void guardDetectsConnect() throws Exception {
		AddressTranslator connecting = advertised -> {
			try (Socket socket = new Socket()) {
				socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), advertised.port), 100);
			}
			catch (IOException ioe) {
				throw new UncheckedIOException(ioe);
			}
			return Optional.empty();
		};

		GuardedProxySelector.arm();

		try {
			connecting.translate(new Endpoint("BB9", null, "127.0.0.1", 1, SourceList.STANDARD));
			fail("Connect was not intercepted");
		}
		catch (AssertionError e) {
			assertEquals("Unexpected socket connect: socket://127.0.0.1:1", e.getMessage());
		}
		finally {
			GuardedProxySelector.disarm();
		}
	}

	@Test
	public void translateDoesNoNameLookupOrConnect() {
		PassThroughAddressTranslator translator = new PassThroughAddressTranslator();

		GuardedResolverProvider.arm();
		GuardedProxySelector.arm();

		try {
			Host name = translator.translate(new Endpoint("BB9", "tls1", "node1.internal.invalid", 3000, SourceList.STANDARD)).get();
			Host ipv6 = translator.translate(new Endpoint("BB9", null, "2001:db8::1", 3001, SourceList.ALTERNATE)).get();

			assertEquals("node1.internal.invalid", name.name);
			assertEquals("tls1", name.tlsName);
			assertEquals(3000, name.port);
			assertEquals("2001:db8::1", ipv6.name);
			assertEquals(3001, ipv6.port);
		}
		finally {
			GuardedProxySelector.disarm();
			GuardedResolverProvider.disarm();
		}
	}

	@Test
	public void translatorReferencesNoIoTypes() throws Exception {
		for (String name : new ClassConstants(PassThroughAddressTranslator.class).classReferences) {
			assertFalse(name, name.startsWith("java/net/"));
			assertFalse(name, name.startsWith("java/nio/"));
			assertFalse(name, name.startsWith("java/io/"));
		}
	}
}
