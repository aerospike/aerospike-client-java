/*
 * Aerospike Client - Java Library
 *
 * Copyright 2014 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

import java.nio.ByteBuffer;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.Policy;

public class AsyncReadHeader extends AsyncSingleCommand {
	private final Policy policy;
	private final RecordListener listener;
	private Record record;
	
	public AsyncReadHeader(AsyncCluster cluster, Policy policy, RecordListener listener, Key key) {
		super(cluster, key);
		this.policy = (policy == null) ? new Policy() : policy;
		this.listener = listener;
	}

	@Override
	protected Policy getPolicy() {
		return policy;
	}

	@Override
	protected void writeBuffer() throws AerospikeException {
		setReadHeader(key);
	}

	protected final void parseResult(ByteBuffer byteBuffer) throws AerospikeException {
		int resultCode = byteBuffer.get(5) & 0xFF;

        if (resultCode == 0) {
        	int generation = byteBuffer.getInt(6);
    		int expiration = byteBuffer.getInt(10);
    		record = new Record(null, null, generation, expiration);
        }
        else {
        	if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
        		record = null;
        	}
        	else {
        		throw new AerospikeException(resultCode);
        	}
        }		        
	}

	protected final void onSuccess() {
		if (listener != null) {
			listener.onSuccess(key, record);
		}
	}

	protected final void onFailure(AerospikeException e) {
		if (listener != null) {
			listener.onFailure(e);
		}
	}
}
