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
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

public final class AsyncWrite extends AsyncSingleCommand {
	private final WriteListener listener;
		
	public AsyncWrite(AsyncCluster cluster, Key key, WriteListener listener) {
		super(cluster, key);
		this.listener = listener;
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
