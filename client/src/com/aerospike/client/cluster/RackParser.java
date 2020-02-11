/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import java.util.HashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.command.Buffer;

/**
 * Parse rack-ids info command.
 */
public final class RackParser {
	static final String RebalanceGeneration = "rebalance-generation";
	static final String RackIds = "rack-ids";

	private final HashMap<String,Integer> racks;
	private final StringBuilder sb;
	private final byte[] buffer;
	private final int generation;
	private int length;
	private int offset;

	public RackParser(Connection conn, Node node) {
		// Send format:  rebalance-generation\nrack-ids\n
		this.racks = new HashMap<String,Integer>();

		Info info = new Info(conn, RebalanceGeneration, RackIds);
		this.length = info.length;

		if (length == 0) {
			throw new AerospikeException.Parse("rack-ids response is empty");
		}
		this.buffer = info.buffer;

		// Create reusable StringBuilder for performance.
		this.sb = new StringBuilder(32);  // Max namespace length

		generation = parseGeneration();

		parseRacks(node);
	}

	public int getGeneration() {
		return generation;
	}

	public HashMap<String,Integer> getRacks() {
		return racks;
	}

	private int parseGeneration() {
		expectName(RebalanceGeneration);

		int begin = offset;

		while (offset < length) {
			if (buffer[offset] == '\n') {
				String s = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();
				offset++;
				return Integer.parseInt(s);
			}
			offset++;
		}
		throw new AerospikeException.Parse("Failed to find " + RebalanceGeneration);
	}

	private void parseRacks(Node node) {
		// Use low-level info methods and parse byte array directly for maximum performance.
		// Receive format: rack-ids\t<ns1>:<rack1>;<ns2>:<rack2>...\n
		expectName(RackIds);

		int begin = offset;

		while (offset < length) {
			if (buffer[offset] == ':') {
				// Parse namespace.
				String namespace = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();

				if (namespace.length() <= 0 || namespace.length() >= 32) {
					String response = getTruncatedResponse();
					throw new AerospikeException.Parse("Invalid racks namespace " +
						namespace + ". Response=" + response);
				}
				begin = ++offset;

				// Parse rack.
				while (offset < length) {
					byte b = buffer[offset];

					if (b == ';' || b == '\n') {
						break;
					}
					offset++;
				}
				int rack = Integer.parseInt(new String(buffer, begin, offset - begin));

				racks.put(namespace, rack);
				begin = ++offset;
			}
			else {
				offset++;
			}
		}
	}

	private void expectName(String name) throws AerospikeException {
		int begin = offset;

		while (offset < length) {
			if (buffer[offset] == '\t') {
				String s = Buffer.utf8ToString(buffer, begin, offset - begin, sb).trim();

				if (name.equals(s)) {
					offset++;
					return;
				}
				break;
			}
			offset++;
		}
		throw new AerospikeException.Parse("Failed to find " + name);
	}

	private String getTruncatedResponse() {
		int max = (length > 200) ? 200 : length;
		return Buffer.utf8ToString(buffer, 0, max);
	}
}
