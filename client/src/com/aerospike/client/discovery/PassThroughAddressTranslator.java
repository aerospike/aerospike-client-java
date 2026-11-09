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

import java.util.Optional;

import com.aerospike.client.Host;

/**
 * Address translator that returns every advertised endpoint unchanged: the identity translation.
 * Host name, port and TLS name are kept. It never misses, so
 * {@link com.aerospike.client.policy.ClientPolicy#translationOnMiss} never applies. Two nodes that
 * advertise the same host and port still fail the client's address collision check, which names
 * this translator as {@code pass-through}.
 *
 * <h2>Server-advertised setups</h2>
 * Use this translator when the server can be configured to advertise, for every node, an address
 * and port the client can reach. The client then connects to exactly what the server advertises.
 * <p>
 * Server settings, in the {@code network.service} context of each node:
 * <ul>
 * <li>{@code access-address} / {@code access-port}: the address and port in the standard list
 * (service-clear-std, peers-clear-std). Use them when every client reaches the nodes the same
 * way.</li>
 * <li>{@code alternate-access-address} / {@code alternate-access-port}: the address and port in
 * the alternate list (service-clear-alt, peers-clear-alt). Use them when clients outside the
 * cluster's network need a different address than clients inside it. The TLS lists use
 * {@code tls-alternate-access-address} / {@code tls-alternate-access-port}.</li>
 * </ul>
 * <p>
 * Client settings:
 * <ul>
 * <li>{@link com.aerospike.client.policy.ClientPolicy#useServicesAlternate}: true to read the
 * alternate list, false (default) to read the standard list.</li>
 * <li>{@link com.aerospike.client.policy.ClientPolicy#addressTranslator}: {@link #INSTANCE}.</li>
 * </ul>
 * <p>
 * Branch A, a distinct name per node. Each node advertises its own host name on the default port:
 * <pre>
 * node 1: alternate-access-address node1.priv.example.com
 * node 2: alternate-access-address node2.priv.example.com
 * node 3: alternate-access-address node3.priv.example.com
 *
 * ClientPolicy policy = new ClientPolicy();
 * policy.useServicesAlternate = true;
 * policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
 * AerospikeClient client = new AerospikeClient(policy, new Host("node1.priv.example.com", 3000));
 * </pre>
 * <p>
 * Branch B, one name with a port per node. Every node advertises the same host name and its own
 * port, for example a load balancer or port forwarder that routes each port to one node:
 * <pre>
 * node 1: alternate-access-address as.priv.example.com, alternate-access-port 31001
 * node 2: alternate-access-address as.priv.example.com, alternate-access-port 31002
 * node 3: alternate-access-address as.priv.example.com, alternate-access-port 31003
 *
 * ClientPolicy policy = new ClientPolicy();
 * policy.useServicesAlternate = true;
 * policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
 * AerospikeClient client = new AerospikeClient(policy, new Host("as.priv.example.com", 31001));
 * </pre>
 * <p>
 * Each node must advertise a distinct host and port. In Branch B, giving two nodes the same
 * {@code alternate-access-port} is a collision: the client logs it and adds no node for the
 * colliding peers.
 * <p>
 * When the server cannot be configured to advertise a reachable address, for example because the
 * address is assigned outside the server's control, use the {@code port-map} translator instead,
 * which maps each advertised endpoint to a reachable host and port on the client side.
 * {@link StaticMapAddressTranslator} covers the case where only the host name has to change.
 */
public final class PassThroughAddressTranslator implements AddressTranslator {
	/**
	 * Shared instance. The translator is stateless.
	 */
	public static final PassThroughAddressTranslator INSTANCE = new PassThroughAddressTranslator();

	/**
	 * Initialize translator.
	 */
	public PassThroughAddressTranslator() {
	}

	/**
	 * Return the advertised host name, port and TLS name unchanged. Never empty.
	 */
	@Override
	public Optional<Host> translate(Endpoint advertised) {
		return Optional.of(new Host(advertised.host, advertised.tlsName, advertised.port));
	}
}
