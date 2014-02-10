/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
