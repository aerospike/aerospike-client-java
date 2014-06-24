/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import com.aerospike.client.policy.Policy;

public final class ExecuteCommand extends ReadCommand {
	private final String packageName;
	private final String functionName;
	private final Value[] args;

	public ExecuteCommand(
		Cluster cluster, 
		Policy policy,
		Key key,
		String packageName,
		String functionName,
		Value[] args
	) {
		super(cluster, policy, key, null);
		this.packageName = packageName;
		this.functionName = functionName;
		this.args = args;
	}
	
	@Override
	protected void writeBuffer() throws AerospikeException {
		setUdf(key, packageName, functionName, args);
	}
}
