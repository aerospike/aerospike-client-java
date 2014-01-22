/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

import java.nio.ByteBuffer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public final class AsyncWrite extends AsyncSingleCommand {
	private final WritePolicy policy;
	private final WriteListener listener;
	private final Bin[] bins;
	private final Operation.Type operation;
		
	public AsyncWrite(AsyncCluster cluster, WritePolicy policy, WriteListener listener, Key key, Bin[] bins, Operation.Type operation) {
		super(cluster, key);
		this.policy = (policy == null)? new WritePolicy() : policy;
		this.listener = listener;
		this.bins = bins;
		this.operation = operation;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setWrite(policy, operation, key, bins);
	}

	protected void parseResult(ByteBuffer byteBuffer) throws AerospikeException {
		int resultCode = byteBuffer.get(5) & 0xFF;
		
		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}
	}

	protected void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key);
		}
	}	

	protected void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
