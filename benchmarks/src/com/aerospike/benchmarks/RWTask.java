/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.benchmarks;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

//
// Always generates random reads
// between start and end
//
public abstract class RWTask implements Runnable {

	final AerospikeClient client;
	final String namespace;
	final String setName;
	Random rgen;
	int nKeys;
	int startKey;
	int keySize;
	int nBins;
	DBObjectSpec[] objects;
	String cycleType;
	AtomicIntegerArray settingsArr;
	boolean validate;
	int runTime;
	ExpectedValue[] expectedValues;
	CounterStore counters;
	AtomicInteger timeElapsed;
	WritePolicy policy;
	WritePolicy writePolicyGeneration;
	boolean debug;

	public RWTask(
		AerospikeClient client, 
		String namespace,
		String setName,
		int nKeys, 
		int startKey, 
		int keySize, 
		DBObjectSpec[] objects, 
		int nBins, 
		String cycleType, 
		WritePolicy policy, 
		AtomicIntegerArray settingsArr, 
		boolean validate, 
		int runTime, 
		CounterStore counters, 
		boolean debug
	) {
		this.client      = client;
		this.namespace   = namespace;
		this.setName     = setName;
		this.nKeys       = nKeys;
		this.startKey    = startKey;
		this.keySize     = keySize;
		this.objects     = objects;
		this.nBins       = nBins;
		this.cycleType   = cycleType;
		this.policy      = policy;
		this.settingsArr = settingsArr;
		this.validate    = validate;
		this.runTime     = runTime;
		this.counters    = counters;
		this.debug       = debug;

		this.timeElapsed = counters.timeElapsed;
		
		// Use default constructor which uses a different seed for each invocation.
		// Do not use System.currentTimeMillis() for a seed because it is often
		// the same across concurrent threads, thus causing hot keys.
		this.rgen = new Random();
				
		writePolicyGeneration = new WritePolicy();
		writePolicyGeneration.timeout = policy.timeout;
		writePolicyGeneration.maxRetries = policy.maxRetries;
		writePolicyGeneration.sleepBetweenRetries = policy.sleepBetweenRetries;
		writePolicyGeneration.recordExistsAction = RecordExistsAction.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;		
	}	
	
	public void run() {
 
		// if we're going to be validating, load the data.
		if (this.validate) {
			setupValidation();
		}

		// set up parameters...
		int throughputget         = settingsArr.get(0);
		double readPct            = settingsArr.get(1) / 100.0;
		double singleBinReadPct   = settingsArr.get(2) / 100.0;
		double singleBinUpdatePct = settingsArr.get(3) / 100.0;

		// Now run...
		while (runTime == 0 || this.timeElapsed.get() < runTime) {
					
			// Get random key
			int curKeyIdx = rgen.nextInt(this.nKeys);
		
			// Check - is it a read or a write?
			double randnum = rgen.nextDouble();
			boolean isWrite = false;
			if (randnum >= readPct) {
				isWrite = true;
			}

			// Single bin or multibin?
			boolean isMultiBin = false;
			randnum = rgen.nextDouble();
			if (isWrite && (randnum < singleBinUpdatePct)) {
				isMultiBin = true;
			} else if (randnum < singleBinReadPct) {
				isMultiBin = true;
			}		

			// now do the work
			try {
				if (this.cycleType.equals("RU")) {
					if (isWrite) {
						doWrite(curKeyIdx, isMultiBin);
					}else{
						doRead( curKeyIdx, isMultiBin);
					}
				} else if (this.cycleType.equals("RMU") || this.cycleType.equals("RMI") || this.cycleType.equals("RMD")) {
					// read all bins
					doRead(curKeyIdx, true);

					// write all bins
					if (this.cycleType.equals("RMU")) {
						doWrite(curKeyIdx, true);
					} else if (this.cycleType.equals("RMI")) {
						doIncrement(curKeyIdx, 1);
					} else if (this.cycleType.equals("RMD")) {
						doIncrement(curKeyIdx, -1);
					}
				}	 
			} catch (Exception e) {
				if (!this.debug) {
					System.out.println("Exception - " + e.toString());
				}else{
					e.printStackTrace();
				}
			}		 

			// throttle throughput
			long getCounter = counters.tcounter.incrementAndGet();
			if (throughputget != 0) {
				long sleepfor = 0;
				long t = System.currentTimeMillis();
				if ((t - this.counters.start_time) < (getCounter * 1000 / throughputget)) {
					sleepfor = (getCounter*1000/throughputget - (t-counters.start_time));
				} 
				
				try {
					Thread.sleep(sleepfor);
				} catch (Exception e) {
				}
			}
		}
	}
	
	/**
	 * Read existing values from the database, save them away in our validation arrays.
	 */
	private void setupValidation() {
		this.expectedValues = new ExpectedValue[this.nKeys];
		
		// load starting values
		for (int i = 0; i < this.nKeys; i++) {
			Bin[] bins = null;
			int generation = 0;
			
			try {
				Key key = new Key(this.namespace, this.setName, Utils.genKey(this.startKey+i, this.keySize));
				Record record = client.get(this.policy, key);
				
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

		// wait for all tasks to be finished loading
		while(! this.counters.loadValuesFinished.get()) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				System.out.println("can't sleep while waiting for all values to load");
			}
		}
	}
	
	/**
	 * Write the key at the given index
	 */
	protected void doWrite(int keyIdx, boolean multiBin) {
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);
		Bin[] bins;

		if (this.validate) {
			bins = Utils.genBins(rgen, multiBin ? 1 : this.nBins, objects, this.expectedValues[keyIdx].generation+1);
		} else {
			bins = Utils.genBins(rgen, multiBin ? 1 : this.nBins, objects, 0);
		}
		
		try {
			put(new Key(this.namespace, this.setName, key), bins);
			
			if (this.validate) {
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
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);
		
		// set up bin for increment
		Bin[] bins = new Bin[] {new Bin("", incrValue)};
		
		try {
			add(new Key(this.namespace, this.setName, key), bins);
			
			if (this.validate) {
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
		String key = Utils.genKey(this.startKey+keyIdx, this.keySize);

		try {
			if (multiBin) {
				// read all bins, maybe validate
				get(keyIdx, new Key(this.namespace, this.setName, key));			
			} 
			else {
				// read one bin, maybe validate
				get(keyIdx, new Key(this.namespace, this.setName, key), Integer.toString(0));			
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
		counters.write.fail.getAndIncrement();
		
		if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
			counters.generationErrCnt.getAndIncrement();					
		}
		
		if (debug && ae.getResultCode() != ResultCode.TIMEOUT) {
			//System.out.println(ae.getMessage());
			ae.printStackTrace();
		}
	}

	protected void writeFailure(Exception e) {
		counters.write.fail.getAndIncrement();
		
		if (debug) {
			//System.out.println(ae.getMessage());
			e.printStackTrace();
		}
	}
	
	protected void readFailure(AerospikeException ae) {
		counters.read.fail.getAndIncrement();
		
		if (debug && ae.getResultCode() != ResultCode.TIMEOUT) {
			//System.out.println(ae.getMessage());
			ae.printStackTrace();
		}
	}

	protected void readFailure(Exception e) {
		counters.read.fail.getAndIncrement();
		
		if (debug) {
			//System.out.println(ae.getMessage());
			e.printStackTrace();
		}
	}

	protected abstract void put(Key key, Bin[] bins) throws AerospikeException;
	protected abstract void add(Key key, Bin[] bins) throws AerospikeException;
	protected abstract void get(int keyIdx, Key key, String binName) throws AerospikeException;
	protected abstract void get(int keyIdx, Key key) throws AerospikeException;
}
