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

import java.util.Map;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
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
			// Get random key
			int key = random.nextInt(keyCount);
		
			try {
				switch (args.workload) {
				case READ_UPDATE:
					readUpdate(key);
					break;
					
				case READ_MODIFY_UPDATE:
					readModifyUpdate(key);		
					break;
					
				case READ_MODIFY_INCREMENT:
					readModifyIncrement(key);		
					break;
					
				case READ_MODIFY_DECREMENT:
					readModifyDecrement(key);		
					break;
				case READ_FROM_FILE:
					readFromFile(key);	
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
	
	private void readUpdate(int key) {
		if (random.nextDouble() < this.readPct) {
			boolean isMultiBin = random.nextDouble() < readMultiBinPct;
			doRead(key, isMultiBin);
		}
		else {
			boolean isMultiBin = random.nextDouble() < writeMultiBinPct;
			doWrite(key, isMultiBin);
		}		
	}
	
	private void readModifyUpdate(int key) {
		// Read all bins.
		doRead(key, true);
		// Write one bin.
		doWrite(key, false);
	}
	
	private void readModifyIncrement(int key) {
		// Read all bins.
		doRead(key, true);
		// Increment one bin.
		doIncrement(key, 1);
	}

	private void readModifyDecrement(int key) {
		// Read all bins.
		doRead(key, true);
		// Decrement one bin.
		doIncrement(key, -1);
	}
	
	private void readFromFile(int key){

		if (args.keyType == KeyType.STRING) {
		    doReadString(key,true);
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
				Key key = new Key(args.namespace, args.setName, Utils.genKey(keyStart + i, args.keySize));
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
		String key = Utils.genKey(keyStart + keyIdx, args.keySize);
		int binCount = multiBin ? args.nBins : 1;
		Bin[] bins;

		if (args.validate) {
			bins = Utils.genBins(random, binCount, args.objectSpec, this.expectedValues[keyIdx].generation+1);
		} else {
			bins = Utils.genBins(random, binCount, args.objectSpec, 0);
		}
		
		try {
			put(new Key(args.namespace, args.setName, key), bins);
			
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
		// get key
		String key = Utils.genKey(keyStart + keyIdx, args.keySize);
		
		// set up bin for increment
		Bin[] bins = new Bin[] {new Bin("", incrValue)};
		
		try {
			add(new Key(args.namespace, args.setName, key), bins);
			
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

	protected void doBatchRead(int keyIdx, boolean multiBin) {

		try {
			Key[] keys = new Key[args.pipeline];
			for (int i = 0; i < args.pipeline; i++) {
				String key = Utils.genKey(keyStart + keyIdx + i, args.keySize);
				keys[i] = new Key(args.namespace, args.setName, key);
			}
			get(keyIdx, args.pipeline, keys);
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}	
		catch (Exception e) {
			readFailure(e);
		}
	}
		
	/**
	 * Read the key at the given index.
	 */
	protected void doRead(int keyIdx, boolean multiBin) {

		if (args.pipeline > 1) {
			
			doBatchRead(keyIdx, multiBin);
			return;
		}
		String key = Utils.genKey(keyStart + keyIdx, args.keySize);

		try {
			if (multiBin) {
				// Read all bins, maybe validate
				get(keyIdx, new Key(args.namespace, args.setName, key));			
			} 
			else {
				// Read one bin, maybe validate
				get(keyIdx, new Key(args.namespace, args.setName, key), Integer.toString(0));			
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
	protected void doReadLong(int keyIdx,boolean multiBin) {
		long numKey = Long.parseLong(Main.keyList.get(keyStart + keyIdx));
		
		try {
			if (multiBin) {
				// Read all bins, maybe validate
				get(keyIdx,new Key(args.namespace, args.setName, numKey));			
			} 
			else {
				// Read one bin, maybe validate
				get(keyIdx,new Key(args.namespace, args.setName, numKey), Integer.toString(0));			
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
				get(keyIdx,new Key(args.namespace, args.setName, strKey));			
			} 
			else {
				// Read one bin, maybe validate
				get(keyIdx,new Key(args.namespace, args.setName, strKey), Integer.toString(0));			
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
		}	
		catch (Exception e) {
			readFailure(e);
		}
		
	}
	
	protected void validateRead(int keyIdx, Record record) {	
		if (! this.expectedValues[keyIdx].validate(record)) {
			this.counters.valueMismatchCnt.incrementAndGet();
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
	protected abstract void get(int keyIdx, Key key, String binName) throws AerospikeException;
	protected abstract void get(int keyIdx, Key key) throws AerospikeException;
	protected abstract void get(int keyIdx, int count, Key[] keys) throws AerospikeException;
}
