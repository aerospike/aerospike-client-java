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
import com.aerospike.client.Operation;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.WritePolicy;

public final class OperateCommand extends ReadCommand {
	private final WritePolicy policy;
	private final Operation[] operations;

	public OperateCommand(Cluster cluster, WritePolicy policy, Key key, Operation[] operations) {
		super(cluster, policy, key, null);
		this.policy = (policy == null) ? new WritePolicy() : policy;
		this.operations = operations;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setOperate(policy, key, operations);
	}
}
