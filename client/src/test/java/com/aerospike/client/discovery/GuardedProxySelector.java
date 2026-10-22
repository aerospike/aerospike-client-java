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

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;

/**
 * Proxy selector that fails every {@link java.net.Socket} connect made on a thread while it is
 * armed. Installed as the JVM default on first use. NIO channel connects do not consult it.
 */
public final class GuardedProxySelector extends ProxySelector {
	private static final ThreadLocal<Boolean> ARMED = ThreadLocal.withInitial(() -> false);

	static {
		ProxySelector.setDefault(new GuardedProxySelector(ProxySelector.getDefault()));
	}

	private final ProxySelector builtin;

	private GuardedProxySelector(ProxySelector builtin) {
		this.builtin = builtin;
	}

	public static void arm() {
		ARMED.set(true);
	}

	public static void disarm() {
		ARMED.set(false);
	}

	@Override
	public List<Proxy> select(URI uri) {
		if (ARMED.get()) {
			throw new AssertionError("Unexpected socket connect: " + uri);
		}
		return (builtin != null)? builtin.select(uri) : List.of(Proxy.NO_PROXY);
	}

	@Override
	public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
		if (builtin != null) {
			builtin.connectFailed(uri, sa, ioe);
		}
	}
}
