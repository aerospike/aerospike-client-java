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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assume;

import sun.net.spi.nameservice.NameService;
import sun.net.spi.nameservice.NameServiceDescriptor;

/**
 * Resolver that fails every name lookup made on a thread while it is armed, and answers registered
 * test hostnames with fixed addresses, counting their lookups. Registered through META-INF/services
 * and {@code sun.net.spi.nameservice.provider.1=dns,guarded} ahead of the default name service, so
 * it is the JVM-wide resolver for the client unit tests. Lookups are not cached, so every lookup of
 * a registered hostname is counted.
 * <p>
 * Java 9 and later have no such name service SPI, so {@link #arm()} and {@link #register} skip the
 * calling test there.
 */
public final class GuardedResolverProvider implements NameServiceDescriptor {
	private static final String UNAVAILABLE =
		"Guarded resolver needs the JDK 8 name service SPI, which this JVM does not have; " +
		"name lookups cannot be blocked or answered without real DNS";
	private static final ThreadLocal<Boolean> ARMED = ThreadLocal.withInitial(() -> false);
	private static final Map<String,InetAddress[]> HOSTS = new ConcurrentHashMap<>();
	private static final Map<String,AtomicInteger> LOOKUPS = new ConcurrentHashMap<>();
	private static final AtomicInteger TOTAL = new AtomicInteger();
	private static volatile boolean installed;

	/**
	 * Return true if the JVM resolves names through this provider.
	 */
	public static boolean installed() {
		InetAddress.getLoopbackAddress();
		return installed;
	}

	public static void arm() {
		Assume.assumeTrue(UNAVAILABLE, installed());
		ARMED.set(true);
	}

	public static void disarm() {
		ARMED.set(false);
	}

	/**
	 * Resolve host to the given addresses, in order, until unregistered.
	 */
	public static void register(String host, InetAddress... addresses) throws UnknownHostException {
		Assume.assumeTrue(UNAVAILABLE, installed());
		InetAddress[] named = new InetAddress[addresses.length];

		for (int i = 0; i < addresses.length; i++) {
			named[i] = InetAddress.getByAddress(host, addresses[i].getAddress());
		}
		LOOKUPS.put(host, new AtomicInteger());
		HOSTS.put(host, named);
	}

	public static void unregister(String host) {
		HOSTS.remove(host);
	}

	/**
	 * Number of lookups of a registered host since it was registered.
	 */
	public static int lookups(String host) {
		AtomicInteger count = LOOKUPS.get(host);
		return count == null ? 0 : count.get();
	}

	/**
	 * Number of name lookups of any host.
	 */
	public static int lookups() {
		return TOTAL.get();
	}

	@Override
	public NameService createNameService() {
		if (Security.getProperty("networkaddress.cache.ttl") == null) {
			Security.setProperty("networkaddress.cache.ttl", "0");
		}
		installed = true;

		// A lookup this service does not answer fails with UnknownHostException, so the JVM falls
		// through to the next configured name service, sun.net.spi.nameservice.provider.2=default.
		return new NameService() {
			@Override
			public InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException {
				check(host);
				TOTAL.incrementAndGet();

				InetAddress[] addresses = HOSTS.get(host);

				if (addresses != null) {
					LOOKUPS.get(host).incrementAndGet();
					return addresses.clone();
				}
				throw new UnknownHostException(host);
			}

			@Override
			public String getHostByAddr(byte[] addr) throws UnknownHostException {
				check("reverse lookup");
				throw new UnknownHostException();
			}
		};
	}

	private static void check(String host) {
		if (ARMED.get()) {
			throw new AssertionError("Unexpected name lookup: " + host);
		}
	}

	@Override
	public String getProviderName() {
		return "guarded";
	}

	@Override
	public String getType() {
		return "dns";
	}
}
