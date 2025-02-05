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
import java.util.HashMap;
import java.util.HashSet;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;

public final class Peers {
	public final ArrayList<Peer> peers;
	public final HashMap<String,Node> nodes;
	public final HashSet<Node> removeNodes;
	private final HashSet<Host> invalidHosts;
	public int refreshCount;
	public boolean genChanged;

	public Peers(int peerCapacity) {
		peers = new ArrayList<Peer>(peerCapacity);
		nodes = new HashMap<String,Node>(16);
		removeNodes = new HashSet<Node>(8);
		invalidHosts = new HashSet<Host>(8);
	}

	public boolean hasFailed(Host host) {
		return invalidHosts.contains(host);
	}

	public void fail(Host host) {
		invalidHosts.add(host);
	}

	public int getInvalidCount() {
		return invalidHosts.size();
	}

	public void clusterInitError() {
		StringBuilder sb = new StringBuilder();
		sb.append("Peers not reachable: ");

		boolean comma = false;

		for (Host host : invalidHosts) {
			if (comma) {
				sb.append(", ");
			}
			else {
				comma = true;
			}
			sb.append(host);
		}
		throw new AerospikeException(sb.toString());
	}
}
