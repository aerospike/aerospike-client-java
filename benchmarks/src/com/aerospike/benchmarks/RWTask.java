/*
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

/**
 * Random Read/Write workload task.
 */
public abstract class RWTask implements Runnable {

	final AerospikeClient client;
	final Arguments args;
	final CounterStore counters;
	final RandomShift random;
	final WritePolicy writePolicyGeneration;
	ExpectedValue[] expectedValues;
	final long keyStart;
	final long keyCount;
	volatile boolean valid;
	
	public RWTask(AerospikeClient client, Arguments args, CounterStore counters, long keyStart, long keyCount) {
		this.client = client;
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
		this.valid = true;
		
		random = new RandomShift();
				
		writePolicyGeneration = new WritePolicy();
		writePolicyGeneration.timeout = args.writePolicy.timeout;
		writePolicyGeneration.maxRetries = args.writePolicy.maxRetries;
		writePolicyGeneration.sleepBetweenRetries = args.writePolicy.sleepBetweenRetries;
		writePolicyGeneration.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;		
	}	
	
	public void run() {
		// Load data if we're going to be validating.              
		if (args.validate) {
			setupValidation();
		}

		while (valid) {
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
					
				case TRANSACTION:
					runTransaction();
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
				int transactions;
				if (counters.transaction.latency != null) {
					// Measure the transactions as per one "business" transaction
					transactions = counters.transaction.count.get();
				}
				else {
					transactions = counters.write.count.get() + counters.read.count.get();
				}
				if (transactions > args.throughput) {
					long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();                                        

					if (millis > 0) {
						Util.sleep(millis);
					}
				}
			}
		}
	}
	
	public void stop() {
		valid = false;
	}
	
	private long[] getKeys(int count) {
		long[] keys = new long[count];
		for (int i = 0; i < count; i++) {
			keys[i] = random.nextLong(keyCount);
		}
		return keys;
	}
	
	private void runTransaction() {
		long key;
		Iterator<TransactionalItem> iterator = args.transactionalWorkload.iterator(random);
		long begin = System.nanoTime();
		while (iterator.hasNext()) {
			TransactionalItem thisItem = iterator.next();
			switch (thisItem.getType()) {
				case MULTI_BIN_READ:
					key = random.nextLong(keyCount);
					doRead(key, true);
					break;
				case MULTI_BIN_BATCH_READ:
					doRead(getKeys(thisItem.getRepetitions()), true);
					break;
				case MULTI_BIN_REPLACE:
					key = random.nextLong(keyCount);
					doWrite(key, true, args.replacePolicy);
					break;
				case MULTI_BIN_UPDATE:
					key = random.nextLong(keyCount);
					doWrite(key, true, args.updatePolicy);
					break;
				case SINGLE_BIN_INCREMENT:
					key = random.nextLong(keyCount);
					// Increment one bin.
					doIncrement(key, 1);
					break;

				case SINGLE_BIN_READ:
					key = random.nextLong(keyCount);
					doRead(key, false);
					break;
				case SINGLE_BIN_BATCH_READ:
					doRead(getKeys(thisItem.getRepetitions()), false);
					break;

				case SINGLE_BIN_REPLACE:
					key = random.nextLong(keyCount);
					doWrite(key, false, args.replacePolicy);
					break;
				case SINGLE_BIN_UPDATE:
					key = random.nextLong(keyCount);
					doWrite(key, false, args.updatePolicy);
					break;
				default:
					break;
			}
		}
		
		if (counters.transaction.latency != null) {
			long elapsed = System.nanoTime() - begin;
			counters.transaction.count.getAndIncrement();			
			counters.transaction.latency.add(elapsed);
		}
	}
	
	private void readUpdate() {
		if (random.nextInt(100) < args.readPct) {
			boolean isMultiBin = random.nextInt(100) < args.readMultiBinPct;
			
			if (args.batchSize <= 1) {
				long key = random.nextLong(keyCount);
				doRead(key, isMultiBin);
			}
			else {
				doReadBatch(isMultiBin);
			}
		}
		else {
			boolean isMultiBin = random.nextInt(100) < args.writeMultiBinPct;
			
			if (args.batchSize <= 1) {
				// Single record write.
				long key = random.nextLong(keyCount);
				doWrite(key, isMultiBin);
			}
			else {
				// Batch write is not supported, so write batch size one record at a time.
				for (int i = 0; i < args.batchSize; i++) {
					long key = random.nextLong(keyCount);
					doWrite(key, isMultiBin);
				}
			}
		}		
	}
	
	private void readModifyUpdate() {
		long key = random.nextLong(keyCount);
				
		// Read all bins.
		doRead(key, true);
		// Write one bin.
		doWrite(key, false);
	}
	
	private void readModifyIncrement() {
		long key = random.nextLong(keyCount);

		// Read all bins.
		doRead(key, true);
		// Increment one bin.
		doIncrement(key, 1);
	}

	private void readModifyDecrement() {
		long key = random.nextLong(keyCount);

		// Read all bins.
		doRead(key, true);
		// Decrement one bin.
		doIncrement(key, -1);
	}
	
	private void readFromFile() {
		long key = random.nextLong(keyCount);
		
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
		// Warning: validate only works when keyCount < Integer.MAX_VALUE.
		int keyCapacity = (int)keyCount;
		expectedValues = new ExpectedValue[keyCapacity];
		
		// Load starting values
		for (int i = 0; i < keyCapacity; i++) {
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
	
	protected void doWrite(long keyIdx, boolean multiBin) {
		doWrite(keyIdx, multiBin, null);
	}
	
	/**
	 * Write the key at the given index
	 */
	protected void doWrite(long keyIdx, boolean multiBin, WritePolicy writePolicy) {
		Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);
		// Use predictable value for 0th bin same as key value
		Bin[] bins = args.getBins(random, multiBin, keyStart + keyIdx);
		
		try {
			switch (args.storeType) {
			case KVS:
				put(key, bins, writePolicy);
				break;
	
			case LLIST:
				largeListAdd(key, bins[0].value);
				break;

			case LSTACK:
				largeStackPush(key, bins[0].value);
				break;
			}
			
			if (args.validate) {
				// Warning: validate only works when keyIdx < Integer.MAX_VALUE.
				this.expectedValues[(int)keyIdx].write(bins);
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
	protected void doIncrement(long keyIdx, int incrValue) {
		// set up bin for increment
		Bin[] bins = new Bin[] {new Bin("", incrValue)};
		
		try {
			add(new Key(args.namespace, args.setName, keyStart + keyIdx), bins);
			
			if (args.validate) {
				// Warning: validate only works when keyIdx < Integer.MAX_VALUE.
				this.expectedValues[(int)keyIdx].add(bins, incrValue);
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
	protected void doRead(long keyIdx, boolean multiBin) {
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
	 * Read the keys at the given indexex.
	 */
	protected void doRead(long[] keyIdxs, boolean multiBin) {
		try {
			Key[] keys = new Key[keyIdxs.length];
			for (int i = 0; i < keyIdxs.length; i++) {
				keys[i] = new Key(args.namespace, args.setName, keyStart + keyIdxs[i]);
			}
			
			if (multiBin) {
				switch (args.storeType) {
				case KVS:
					// Read all bins, maybe validate
					get(keys);			
					break;
					
				case LLIST:
					largeListGet(keys[0]);
					break;

				case LSTACK:
					largeStackPeek(keys[0]);
					break;
				}
			} 
			else {
				switch (args.storeType) {
				case KVS:
					// Read one bin, maybe validate
					get(keys, "0");
					break;
					
				case LLIST:
					largeListGet(keys[0]);
					break;

				case LSTACK:
					largeStackPeek(keys[0]);
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
			long keyIdx = random.nextLong(keyCount);
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
	protected void doReadLong(long keyIdx, boolean multiBin) {
		// Warning: read from file only works when keyStart + keyIdx < Integer.MAX_VALUE.
		long numKey = Long.parseLong(Main.keyList.get((int)(keyStart + keyIdx)));
		
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
	protected void doReadString(long keyIdx,boolean multiBin) {
		// Warning: read from file only works when keyStart + keyIdx < Integer.MAX_VALUE.
		String strKey = Main.keyList.get((int)(keyStart+keyIdx));

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
			int keyIdx = (int)(key.userKey.toLong() - keyStart);
			
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

	protected void put(Key key, Bin[] bins) {
		put(key, bins, args.writePolicy);
	}
	protected abstract void put(Key key, Bin[] bins, WritePolicy policy) throws AerospikeException;
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
