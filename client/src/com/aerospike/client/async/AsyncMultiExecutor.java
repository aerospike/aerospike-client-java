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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;

public abstract class AsyncMultiExecutor {

	private final AtomicInteger completedCount = new AtomicInteger();
	protected int completedSize;
	private boolean failed;
	
	protected final void childSuccess() {
		int count = completedCount.incrementAndGet();
		
		if (!failed && count >= completedSize) {
			onSuccess();
		}
	}
	
	protected final void childFailure(AerospikeException ae) {
		failed = true;
		completedCount.incrementAndGet();
		onFailure(ae);
	}
	
	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
