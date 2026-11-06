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
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.security.Security;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Resolver that fails every name lookup made on a thread while it is armed, and answers registered
 * test hostnames with fixed addresses, counting their lookups. Registered through META-INF/services,
 * so it is the JVM-wide resolver for the client unit tests. Lookups are not cached, so every
 * lookup of a registered hostname is counted.
 */
public final class GuardedResolverProvider extends InetAddressResolverProvider {
	private static final ThreadLocal<Boolean> ARMED = ThreadLocal.withInitial(() -> false);
	private static final Map<String,InetAddress[]> HOSTS = new ConcurrentHashMap<>();
	private static final Map<String,AtomicInteger> LOOKUPS = new ConcurrentHashMap<>();
	private static final AtomicInteger TOTAL = new AtomicInteger();

	public static void arm() {
		ARMED.set(true);
	}

	public static void disarm() {
		ARMED.set(false);
	}

	/**
	 * Resolve host to the given addresses, in order, until unregistered.
	 */
	public static void register(String host, InetAddress... addresses) throws UnknownHostException {
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
	public InetAddressResolver get(Configuration configuration) {
		InetAddressResolver builtin = configuration.builtinResolver();

		if (Security.getProperty("networkaddress.cache.ttl") == null) {
			Security.setProperty("networkaddress.cache.ttl", "0");
		}

		return new InetAddressResolver() {
			@Override
			public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
				check(host);
				TOTAL.incrementAndGet();

				InetAddress[] addresses = HOSTS.get(host);

				if (addresses != null) {
					LOOKUPS.get(host).incrementAndGet();
					return Arrays.stream(addresses);
				}
				return builtin.lookupByName(host, policy);
			}

			@Override
			public String lookupByAddress(byte[] addr) throws UnknownHostException {
				check("reverse lookup");
				return builtin.lookupByAddress(addr);
			}
		};
	}

	private static void check(String host) {
		if (ARMED.get()) {
			throw new AssertionError("Unexpected name lookup: " + host);
		}
	}

	@Override
	public String name() {
		return "guarded";
	}
}
