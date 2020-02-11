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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.aerospike.client.Host;

public final class Peers {
	public final ArrayList<Peer> peers;
	public final HashSet<Host> hosts;
	public final HashMap<String,Node> nodes;
	public int refreshCount;
	public boolean usePeers;
	public boolean genChanged;

	public Peers(int peerCapacity, int addCapacity) {
		peers = new ArrayList<Peer>(peerCapacity);
		hosts = new HashSet<Host>(addCapacity);
		nodes = new HashMap<String,Node>(addCapacity);
		usePeers = true;
	}
}
