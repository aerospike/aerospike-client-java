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
package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Synchronous read/write task.
 */
public class RWTaskSync extends RWTask {

	public RWTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(client, args, counters, keyStart, keyCount);	
	}
		
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.put(args.writePolicy, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(args.writePolicy, key, bins);
			counters.write.count.getAndIncrement();			
		}
	}
	
	protected void add(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.add(writePolicyGeneration, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.add(writePolicyGeneration, key, bins);
			counters.write.count.getAndIncrement();
		}
	}

	protected void get(int keyIdx, Key key, String binName) throws AerospikeException {
		Record record;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			record = client.get(args.readPolicy, key, binName);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(args.readPolicy, key, binName);
		}
		
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}

		if (args.validate) {
			validateRead(keyIdx, record);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
		Record record;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			record = client.get(args.readPolicy, key);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(args.readPolicy, key);
		}
	
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}

		if (args.validate) {
			validateRead(keyIdx, record);
		}
	}
}
