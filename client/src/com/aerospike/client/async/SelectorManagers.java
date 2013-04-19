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

import java.io.IOException;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.util.Util;

public final class SelectorManagers {
	
	private final SelectorManager[] managers;
    private final AtomicInteger current = new AtomicInteger();
	
	public SelectorManagers(AsyncClientPolicy policy) throws AerospikeException {
		managers = new SelectorManager[policy.asyncSelectorThreads];
		
		SelectorProvider provider = SelectorProvider.provider();
		
		for (int i = 0; i < policy.asyncSelectorThreads; i++) {
			try {
				managers[i] = new SelectorManager(policy, provider);
			}
			catch (IOException ioe) {
                for (int j = 0; j < i; j++) {
                	managers[j].close();
                }
				throw new AerospikeException("Failed to construct event manager: " + Util.getErrorMessage(ioe));
			}
		}
		
		int count = 0;
		
		for (SelectorManager manager : managers) {
			manager.setName("selector" + count);
			manager.setDaemon(true);
			manager.start();
			count++;
		}
	}
		
	public SelectorManager next() {
        return managers[Math.abs(current.getAndIncrement() % managers.length)];
	}
	
	public void close() {		
		for (SelectorManager manager : managers) {
			manager.close();
		}
	}
}
