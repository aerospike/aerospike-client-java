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
package com.aerospike.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

/**
 * Random Read/Write workload task.
 */
public abstract class RWTask implements Runnable {

	final AerospikeClient client;
	final Arguments args;
	final CounterStore counters;
	final Random random;
	final WritePolicy writePolicyGeneration;
	ExpectedValue[] expectedValues;
	final double readPct;
	final double readMultiBinPct;
	final double writeMultiBinPct;
	final int keyStart;
	final int keyCount;
	
	public RWTask(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		this.client = client;
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
		
		// Use default constructor which uses a different seed for each invocation.
		// Do not use System.currentTimeMillis() for a seed because it is often
		// the same across concurrent threads, thus causing hot keys.
		random = new Random();
				
		writePolicyGeneration = new WritePolicy();
		writePolicyGeneration.timeout = args.writePolicy.timeout;
		writePolicyGeneration.maxRetries = args.writePolicy.maxRetries;
		writePolicyGeneration.sleepBetweenRetries = args.writePolicy.sleepBetweenRetries;
		writePolicyGeneration.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;
		
		readPct = (double)args.readPct / 100.0;
		readMultiBinPct = (double)args.readMultiBinPct / 100.0;
		writeMultiBinPct = (double)args.writeMultiBinPct / 100.0;
	}	
	
	public void run() {
		// Load data if we're going to be validating.
		if (args.validate) {
			setupValidation();
		}

		while (true) {
			try {
				switch (args.workload) {
				case READ_UPDATE:
					readUpdate();
					break;
					
				case READ_MODIFY_UPDATE:
					readModifyUpdate();		
					break;
					
				case READ_MODIFY_INCREMENT:
					readModifyIncrement();		
					break;
					
				case READ_MODIFY_DECREMENT:
					readModifyDecrement();		
					break;
					
				case READ_FROM_FILE:
					readFromFile();	
					break;
				}
			} 
			catch (Exception e) {
				if (args.debug) {
					e.printStackTrace();
				}
				else {
					System.out.println("Exception - " + e.toString());
				}
			}		 

			// Throttle throughput
			if (args.throughput > 0) {
				int transactions = counters.write.count.get() + counters.read.count.get();
				
				if (transactions > args.throughput) {
					long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();
					
					if (millis > 0) {
						Util.sleep(millis);
					}
				}
			}
		}
	}
	
	private void readUpdate() {
		if (random.nextDouble() < this.readPct) {
			boolean isMultiBin = random.nextDouble() < readMultiBinPct;
			
			if (args.batchSize <= 1) {
				int key = random.nextInt(keyCount);
				doRead(key, isMultiBin);
			}
			else {
				doReadBatch(isMultiBin);
			}
		}
		else {
			boolean isMultiBin = random.nextDouble() < writeMultiBinPct;
			
			if (args.batchSize <= 1) {
				// Single record write.
				int key = random.nextInt(keyCount);
				doWrite(key, isMultiBin);
			}
			else {
				// Batch write is not supported, so write batch size one record at a time.
				for (int i = 0; i < args.batchSize; i++) {
					int key = random.nextInt(keyCount);
					doWrite(key, isMultiBin);
				}
			}
		}		
	}
	
	private void readModifyUpdate() {
		int key = random.nextInt(keyCount);
				
		// Read all bins.
		doRead(key, true);
		// Write one bin.
		doWrite(key, false);
	}
	
	private void readModifyIncrement() {
		int key = random.nextInt(keyCount);

		// Read all bins.
		doRead(key, true);
		// Increment one bin.
		doIncrement(key, 1);
	}

	private void readModifyDecrement() {
		int key = random.nextInt(keyCount);

		// Read all bins.
		doRead(key, true);
		// Decrement one bin.
		doIncrement(key, -1);
	}
	
	private void readFromFile() {
		int key = random.nextInt(keyCount);
		
		if (args.keyType == KeyType.STRING) {
		    doReadString(key, true);
		}    
		else if (args.keyType == KeyType.INTEGER) {
			doReadLong(key, true);
		}
	}

	/**
	 * Read existing values from the database, save them away in our validation arrays.
	 */
	private void setupValidation() {
		expectedValues = new ExpectedValue[keyCount];
		
		// Load starting values
		for (int i = 0; i < keyCount; i++) {
			Bin[] bins = null;
			int generation = 0;
			
			try {
				Key key = new Key(args.namespace, args.setName, keyStart + i);
				Record record = client.get(args.readPolicy, key);
				
				if (record != null && record.bins != null) {
					Map<String,Object> map = record.bins;
					int max = map.size();
					bins = new Bin[max];
					
					for (int j = 0; j < max; j++) {
						String name = Integer.toString(j);
						bins[j] = new Bin(name, map.get(name));
					}
					generation = record.generation;
				}
				counters.read.count.getAndIncrement();
			}
			catch (Exception e) {				
				readFailure(e);
			}
			expectedValues[i] = new ExpectedValue(bins, generation);
		}

		// Tell the global counter that this task is finished loading
		this.counters.loadValuesFinishedTasks.incrementAndGet();

		// Wait for all tasks to be finished loading
		while (! this.counters.loadValuesFinished.get()) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				System.out.println("Can't sleep while waiting for all values to load");
			}
		}
	}
	
	/**
	 * Write the key at the given index
	 */
	protected void doWrite(int keyIdx, boolean multiBin) {
		Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);
		Bin[] bins = args.getBins(random, multiBin);
		
		try {
			switch (args.storeType) {
			case KVS:
				put(key, bins);
				break;
	
			case LLIST:
				largeListAdd(key, bins[0].value);
				break;

			case LSTACK:
				largeStackPush(key, bins[0].value);
				break;
			}
			
			if (args.validate) {
				this.expectedValues[keyIdx].write(bins);
			}
		}
		catch (AerospikeException ae) {
			writeFailure(ae);
		}	
		catch (Exception e) {
			writeFailure(e);
		}
	}

	/**
	 * Increment (or decrement, if incrValue is negative) the key at the given index.
	 */
	protected void doIncrement(int keyIdx, int incrValue) {
		// set up bin for increment
		Bin[] bins = new Bin[] {new Bin("", incrValue)};
		
		try {
			add(new Key(args.namespace, args.setName, keyStart + keyIdx), bins);
			
			if (args.validate) {
				this.expectedValues[keyIdx].add(bins, incrValue);
			}
		}
		catch (AerospikeException ae) {
			writeFailure(ae);
		}
		catch (Exception e) {
			writeFailure(e);
		}
	}
		
	/**
	 * Read the key at the given index.
	 */
	protected void doRead(int keyIdx, boolean multiBin) {
		try {
			Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);
			
			if (multiBin) {
				switch (args.storeType) {
				case KVS:
					// Read all bins, maybe validate
					get(key);			
					break;
					
				case LLIST:
					largeListGet(key);
					break;

				case LSTACK:
					largeStackPeek(key);
					break;
				}
			} 
			else {
				switch (args.storeType) {
				case KVS:
					// Read one bin, maybe validate
					get(key, "0");
					break;
					
				case LLIST:
					largeListGet(key);
					break;

				case LSTACK:
					largeStackPeek(key);
					break;
				}
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}
		catch (Exception e) {
			readFailure(e);
		}
	}

	/**
	 * Read batch of keys in one call.
	 */
	protected void doReadBatch(boolean multiBin) {
		Key[] keys = new Key[args.batchSize];
		
		for (int i = 0; i < keys.length; i++) {
			int keyIdx = random.nextInt(keyCount);
			keys[i] = new Key(args.namespace, args.setName, keyStart + keyIdx);
		}
		
		try {
			if (multiBin) {
				// Read all bins, maybe validate
				get(keys);			
			} 
			else {
				// Read one bin, maybe validate
				get(keys, "0");			
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}	
		catch (Exception e) {
			readFailure(e);
		}	
	}

	/**
	 * Read the keys of type Integer from the file supplied.
	 */
	protected void doReadLong(int keyIdx, boolean multiBin) {
		long numKey = Long.parseLong(Main.keyList.get(keyStart + keyIdx));
		
		try {
			if (multiBin) {
				// Read all bins, maybe validate
				get(new Key(args.namespace, args.setName, numKey));			
			} 
			else {
				// Read one bin, maybe validate
				get(new Key(args.namespace, args.setName, numKey), "0");			
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}	
		catch (Exception e) {
			readFailure(e);
		}
	}
	
	/**
	 * Read the keys of type String from the file supplied.
	 */
	protected void doReadString(int keyIdx,boolean multiBin) {
		String strKey = Main.keyList.get(keyStart+keyIdx);

		try {
			if (multiBin) {
				// Read all bins, maybe validate
				get(new Key(args.namespace, args.setName, strKey));			
			} 
			else {
				// Read one bin, maybe validate
				get(new Key(args.namespace, args.setName, strKey), "0");			
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}	
		catch (Exception e) {
			readFailure(e);
		}
		
	}
	
	protected void processRead(Key key, Record record) {
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}

		if (args.validate) {
			int keyIdx = key.userKey.toInteger() - keyStart;
			
			if (! this.expectedValues[keyIdx].validate(record)) {
				this.counters.valueMismatchCnt.incrementAndGet();
			}
		}	
	}

	protected void processLargeRead(Key key, List<?> list) {
		if ((list == null || list.size() == 0) && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}
	}

	protected void writeFailure(AerospikeException ae) {
		if (ae.getResultCode() == ResultCode.TIMEOUT) {		
			counters.write.timeouts.getAndIncrement();
		}
		else {			
			counters.write.errors.getAndIncrement();
			
			if (args.debug) {
				ae.printStackTrace();
			}
		}
	}

	protected void writeFailure(Exception e) {
		counters.write.errors.getAndIncrement();
		
		if (args.debug) {
			e.printStackTrace();
		}
	}
	
	protected void readFailure(AerospikeException ae) {
		if (ae.getResultCode() == ResultCode.TIMEOUT) {		
			counters.read.timeouts.getAndIncrement();
		}
		else {			
			counters.read.errors.getAndIncrement();
			
			if (args.debug) {
				ae.printStackTrace();
			}
		}
	}

	protected void readFailure(Exception e) {
		counters.read.errors.getAndIncrement();
		
		if (args.debug) {
			e.printStackTrace();
		}
	}

	protected abstract void put(Key key, Bin[] bins) throws AerospikeException;
	protected abstract void add(Key key, Bin[] bins) throws AerospikeException;
	protected abstract void get(Key key, String binName) throws AerospikeException;
	protected abstract void get(Key key) throws AerospikeException;
	protected abstract void get(Key[] keys) throws AerospikeException;
	protected abstract void get(Key[] keys, String binName) throws AerospikeException;

	protected abstract void largeListAdd(Key key, Value value) throws AerospikeException;
	protected abstract void largeListGet(Key key) throws AerospikeException;

	protected abstract void largeStackPush(Key key, Value value) throws AerospikeException;
	protected abstract void largeStackPeek(Key key) throws AerospikeException;
}
