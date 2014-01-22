/*
 * Aerospike Client - Java Library
 *
 * Copyright 2014 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
