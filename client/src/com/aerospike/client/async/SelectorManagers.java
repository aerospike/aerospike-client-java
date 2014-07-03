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
package com.aerospike.client.async;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.util.Util;

public final class SelectorManagers implements Closeable {
	
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
