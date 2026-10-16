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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;

/**
 * Address translator that replaces both the advertised host and port with the address mapped to
 * the advertised endpoint or to the node that advertised it. Translator name {@code port-map},
 * YAML key {@code translation.port_map}.
 * <p>
 * Map grammar:
 * <ul>
 * <li>Key: an advertised {@code host:port}, or a node name. A key that contains {@code ':'} is a
 * {@code host:port}; any other key is a node name. A host:port key matches only that host on that
 * port. A node-name key matches every address the node advertises.</li>
 * <li>Value: the {@code host:port} to connect to. The port is 1 to 65535.</li>
 * <li>An IPv6 host is written in brackets, in keys and values: {@code [2001:db8::1]:3000}.</li>
 * </ul>
 * Keys are matched against the endpoint as advertised on the list the client uses, the alternate
 * list when {@link com.aerospike.client.policy.ClientPolicy#useServicesAlternate} is true and the
 * standard list otherwise. Author the map against that list: a map written for the other list
 * misses on every host:port key.
 * <p>
 * Precedence: the advertised {@code host:port} is looked up first and the node name second, so a
 * host:port key wins over a node-name key when both match. Any other endpoint is a miss and
 * {@link com.aerospike.client.policy.ClientPolicy#translationOnMiss} applies. The advertised TLS
 * name is kept.
 * <p>
 * The map is parsed and validated once, by the constructor, and is copied: later changes to the
 * map passed in have no effect.
 * <p>
 * Example: a load balancer with one port per node. Nodes 10.0.0.1 and 10.0.0.2 advertise port
 * 3000 and the load balancer forwards {@code lb.example.com:30001} and
 * {@code lb.example.com:30002} to them:
 * <pre>
 * Map&lt;String,String&gt; map = new HashMap&lt;&gt;();
 * map.put("10.0.0.1:3000", "lb.example.com:30001");
 * map.put("10.0.0.2:3000", "lb.example.com:30002");
 * policy.addressTranslator = new PortMapAddressTranslator(map);
 * </pre>
 * Example: a load balancer that derives each node's port from the node address, for example
 * {@code 30000} plus the last octet of a node in 10.0.0.0/24. The map is static, so pre-fill it
 * with every address a node can take, including nodes not started yet. A node that later appears
 * on a pre-filled address joins with no client change. Use
 * {@link TranslationMissMode#STRICT} to refuse a node outside the range:
 * <pre>
 * Map&lt;String,String&gt; map = new HashMap&lt;&gt;();
 * for (int i = 1; i &lt; 255; i++) {
 *     map.put("10.0.0." + i + ":3000", "lb.example.com:" + (30000 + i));
 * }
 * policy.addressTranslator = new PortMapAddressTranslator(map);
 * policy.translationOnMiss = TranslationMissMode.STRICT;
 * </pre>
 * Node-name keys map nodes whose addresses are not known in advance:
 * {@code map.put("BB9020011AC4202", "lb.example.com:30001")}.
 */
public final class PortMapAddressTranslator implements AddressTranslator {
	private final Map<String,Host> endpointMap;
	private final Map<String,Host> nodeMap;

	/**
	 * Initialize translator with a map of advertised {@code host:port} or node name to the
	 * {@code host:port} to connect to.
	 *
	 * @throws AerospikeException if the map is null or an entry is invalid. The message names the
	 * entry.
	 */
	public PortMapAddressTranslator(Map<String,String> portMap) {
		if (portMap == null) {
			throw new AerospikeException("Invalid port-map: map is null");
		}

		Map<String,Host> endpoints = new HashMap<>();
		Map<String,Host> nodes = new HashMap<>();

		for (Map.Entry<String,String> entry : portMap.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (key == null || key.isEmpty()) {
				throw invalid(key, value, "key is empty");
			}

			if (value == null) {
				throw invalid(key, value, "value is null");
			}

			Host target = parse(key, value, value);

			if (key.indexOf(':') >= 0) {
				Host advertised = parse(key, value, key);

				if (endpoints.put(key(advertised.name, advertised.port), target) != null) {
					throw invalid(key, value, "duplicates another key");
				}
			}
			else {
				nodes.put(key, target);
			}
		}
		this.endpointMap = Collections.unmodifiableMap(endpoints);
		this.nodeMap = Collections.unmodifiableMap(nodes);
	}

	/**
	 * Return the host and port mapped to the advertised {@code host:port}, else to the advertised
	 * node name, with the advertised TLS name. Return empty when neither is mapped. In-memory
	 * lookup only.
	 */
	@Override
	public Optional<Host> translate(Endpoint advertised) {
		Host target = endpointMap.get(key(advertised.host, advertised.port));

		if (target == null) {
			if (advertised.nodeName == null) {
				return Optional.empty();
			}

			target = nodeMap.get(advertised.nodeName);

			if (target == null) {
				return Optional.empty();
			}
		}
		return Optional.of(new Host(target.name, advertised.tlsName, target.port));
	}

	private static String key(String host, int port) {
		return (host.indexOf(':') >= 0)? "[" + host + "]:" + port : host + ':' + port;
	}

	private static Host parse(String key, String value, String address) {
		int index = address.lastIndexOf(':');

		if (index < 0) {
			throw invalid(key, value, address + " is not host:port");
		}

		String host = address.substring(0, index);

		if (host.startsWith("[")) {
			if (! host.endsWith("]") || host.length() < 3) {
				throw invalid(key, value, address + " has an invalid bracketed IPv6 host");
			}
			host = host.substring(1, host.length() - 1);
		}
		else if (host.indexOf(':') >= 0) {
			throw invalid(key, value, address + " has an IPv6 host that is not in brackets");
		}

		if (host.isEmpty()) {
			throw invalid(key, value, address + " has an empty host");
		}

		String portString = address.substring(index + 1);
		int port;

		try {
			port = Integer.parseInt(portString);
		}
		catch (NumberFormatException nfe) {
			throw invalid(key, value, address + " has an invalid port " + portString);
		}

		if (port < 1 || port > 65535) {
			throw invalid(key, value, address + " has port " + port + " out of range 1-65535");
		}
		return new Host(host, port);
	}

	private static AerospikeException invalid(String key, String value, String reason) {
		return new AerospikeException("Invalid port-map entry " + key + ": " + value + ": " + reason);
	}
}
