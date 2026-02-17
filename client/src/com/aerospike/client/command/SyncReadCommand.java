/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.command;

import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.Policy;

public abstract class SyncReadCommand extends SyncCommand {
	final Key key;
	final Partition partition;

	public SyncReadCommand(Cluster cluster, Policy policy, Key key) {
		super(cluster, policy, key.namespace);
		this.key = key;
		this.partition = Partition.read(cluster, policy, key);
		cluster.addCommandCount();
	}

	@Override
	protected Node getNode() {
		return partition.getNodeRead(cluster);
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.READ;
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryRead(timeout);
		return true;
	}
}
