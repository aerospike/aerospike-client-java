/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;

/**
 * Parse node's peers.
 */
public final class PeerParser {
	private final Cluster cluster;
	private final Info parser;
	private String tlsName;
	private final int portDefault;
	public final int generation;

	public PeerParser(Cluster cluster, Node node, Connection conn, List<Peer> peers) {
		this.cluster = cluster;

		String command = (cluster.tlsPolicy != null)?
				cluster.useServicesAlternate ? "peers-tls-alt" : "peers-tls-std" :
				cluster.useServicesAlternate ? "peers-clear-alt" : "peers-clear-std";

		parser = new Info(node, conn, command);

		if (parser.length == 0) {
			throw new AerospikeException.Parse(command + " response is empty");
		}

		parser.skipToValue();
		generation = parser.parseInt();
		parser.expect(',');
		portDefault = parser.parseInt();
		parser.expect(',');
		parser.expect('[');

		// Reset peers
		peers.clear();

		if (parser.buffer[parser.offset] == ']') {
			return;
		}

		while (true) {
			Peer peer = parsePeer();
			peers.add(peer);

			if (parser.offset < parser.length && parser.buffer[parser.offset] == ',') {
				parser.offset++;
			}
			else {
				break;
			}
		}
	}

	private Peer parsePeer() {
		Peer peer = new Peer();
		parser.expect('[');
		peer.nodeName = parser.parseString(',');
		parser.offset++;
		peer.tlsName = tlsName = parser.parseString(',');
		parser.offset++;
		peer.hosts = parseHosts();
		parser.expect(']');
		return peer;
	}

	private List<Host> parseHosts() {
		ArrayList<Host> hosts = new ArrayList<Host>(4);
		parser.expect('[');

		if (parser.buffer[parser.offset] == ']') {
			return hosts;
		}

		while (true) {
			Host host = parseHost();
			hosts.add(host);

			if (parser.buffer[parser.offset] == ']') {
				parser.offset++;
				return hosts;
			}
			parser.offset++;
		}
	}

	private Host parseHost() {
		String host;

		if (parser.buffer[parser.offset] == '[') {
			// IPV6 addresses can start with bracket.
			parser.offset++;
			host = parser.parseString(']');
			parser.offset++;
		}
		else {
			host = parser.parseString(':', ',', ']');
		}

		if (cluster.ipMap != null) {
			String alternativeHost = cluster.ipMap.get(host);

			if (alternativeHost != null) {
				host = alternativeHost;
			}
		}

		if (parser.offset < parser.length) {
			byte b = parser.buffer[parser.offset];

			if (b == ':') {
				parser.offset++;
				int port = parser.parseInt();
				return new Host(host, tlsName, port);
			}

			if (b == ',' || b == ']') {
				return new Host(host, tlsName, portDefault);
			}
		}

		String response = parser.getTruncatedResponse();
		throw new AerospikeException.Parse("Unterminated host in response: " + response);
	}
}
