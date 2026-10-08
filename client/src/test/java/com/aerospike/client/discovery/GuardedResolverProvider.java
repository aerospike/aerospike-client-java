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
import java.util.stream.Stream;

/**
 * Resolver that fails every name lookup made on a thread while it is armed. Registered through
 * META-INF/services, so it is the JVM-wide resolver for the client unit tests.
 */
public final class GuardedResolverProvider extends InetAddressResolverProvider {
	private static final ThreadLocal<Boolean> ARMED = ThreadLocal.withInitial(() -> false);

	public static void arm() {
		ARMED.set(true);
	}

	public static void disarm() {
		ARMED.set(false);
	}

	@Override
	public InetAddressResolver get(Configuration configuration) {
		InetAddressResolver builtin = configuration.builtinResolver();

		return new InetAddressResolver() {
			@Override
			public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
				check(host);
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
