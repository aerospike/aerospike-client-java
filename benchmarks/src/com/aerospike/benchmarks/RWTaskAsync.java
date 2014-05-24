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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;

/**
 * Asynchronous read/write task.
 */
public class RWTaskAsync extends RWTask {

	private final AsyncClient client;
	private final WriteHandler writeHandler;
	private final ReadHandler readHandler;
	
	public RWTaskAsync(AsyncClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(client, args, counters, keyStart, keyCount);
		this.client = client;
		writeHandler = new WriteHandler();
		readHandler = new ReadHandler();
	}
		
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}

		if (counters.write.latency != null) {
			client.put(args.writePolicy, new LatencyWriteHandler(), key, bins);	
		}
		else {
			client.put(args.writePolicy, writeHandler, key, bins);
		}
	}
		
	protected void add(Key key, Bin[] bins) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}
		
		if (counters.write.latency != null) {
			client.add(writePolicyGeneration, new LatencyWriteHandler(), key, bins);
		}
		else {
			client.add(writePolicyGeneration, writeHandler, key, bins);			
		}
	}
	
	protected void get(int keyIdx, Key key, String binName) throws AerospikeException {		
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.read.timeouts.get() > 0) {
			Thread.yield();
		}
		
		if (counters.read.latency != null) {		
			client.get(args.readPolicy, new LatencyReadHandler(), key, binName);
		}
		else {			
			client.get(args.readPolicy, readHandler, key, binName);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.read.timeouts.get() > 0) {
			Thread.yield();
		}

		if (counters.read.latency != null) {	
			client.get(args.readPolicy, new LatencyReadHandler(), key);
		}
		else {			
			client.get(args.readPolicy, readHandler, key);
		}
	}
	
	private final class WriteHandler implements WriteListener {		
		@Override
		public void onSuccess(Key key) {
			counters.write.count.getAndIncrement();
		}
	
		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
		}
	}

	private final class ReadHandler implements RecordListener {
		@Override
		public void onSuccess(Key key, Record record) {
			if (record == null && args.reportNotFound) {
				counters.readNotFound.getAndIncrement();	
			}
			else {
				counters.read.count.getAndIncrement();		
			}
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}
	}
	
	private final class LatencyWriteHandler implements WriteListener {
		private long begin;
		
		public LatencyWriteHandler() {
			this.begin = System.currentTimeMillis();
		}
		
		@Override
		public void onSuccess(Key key) {
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
		}		
	}
	
	private final class LatencyReadHandler implements RecordListener {
		private long begin;
		
		public LatencyReadHandler() {
			this.begin = System.currentTimeMillis();
		}
		
		@Override
		public void onSuccess(Key key, Record record) {
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
			
			if (record == null && args.reportNotFound) {
				counters.readNotFound.getAndIncrement();	
			}
			else {
				counters.read.count.getAndIncrement();		
			}
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}		
	}
	
	/*
	private final class ValidateHandler implements RecordListener {
		
		private final int keyIdx;
		
		public ValidateHandler(int keyIdx) {
			this.keyIdx = keyIdx;
		}
		
		@Override
		public void onSuccess(Key key, Record record) {
			counters.read.count.getAndIncrement();
			validateRead(keyIdx, record);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}
	}*/
}
