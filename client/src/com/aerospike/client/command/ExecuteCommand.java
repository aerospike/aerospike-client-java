/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.WritePolicy;

public final class ExecuteCommand extends ReadCommand {
	private final WritePolicy writePolicy;
	private final String packageName;
	private final String functionName;
	private final Value[] args;

	public ExecuteCommand(
		Cluster cluster,
		WritePolicy writePolicy,
		Key key,
		String packageName,
		String functionName,
		Value[] args
	) {
		super(cluster, writePolicy, key, Partition.write(cluster, writePolicy, key), false);
		this.writePolicy = writePolicy;
		this.packageName = packageName;
		this.functionName = functionName;
		this.args = args;
	}

	@Override
	protected boolean isWrite() {
		return true;
	}

	@Override
	protected Node getNode() {
		return partition.getNodeWrite(cluster);
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setUdf(writePolicy, key, packageName, functionName, args);
	}

	@Override
	protected void handleNotFound(int resultCode) {
		throw new AerospikeException(resultCode);
	}

	@Override
	protected boolean prepareRetry(boolean timeout) {
		partition.prepareRetryWrite(timeout);
		return true;
	}
}
