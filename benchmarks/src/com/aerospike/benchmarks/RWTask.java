/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;

/**
 * Random Read/Write workload task.
 */
public abstract class RWTask {

	final Arguments args;
	final CounterStore counters;
	final WritePolicy writePolicyGeneration;
	final long keyStart;
	final long keyCount;
	boolean valid;

	public RWTask(Arguments args, CounterStore counters, long keyStart, long keyCount) {
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
		this.valid = true;

		writePolicyGeneration = new WritePolicy(args.writePolicy);
		writePolicyGeneration.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicyGeneration.generation = 0;
	}

	public void stop() {
		valid = false;
	}

	protected void runCommand(RandomShift random) {
		try {
			switch (args.workload) {
				case READ_UPDATE:
				case READ_REPLACE:
					readUpdate(random);
					break;

				case READ_MODIFY_UPDATE:
					readModifyUpdate(random);
					break;

				case READ_MODIFY_INCREMENT:
					readModifyIncrement(random);
					break;

				case READ_MODIFY_DECREMENT:
					readModifyDecrement(random);
					break;

				case READ_FROM_FILE:
					readFromFile(random);
					break;

				case TRANSACTION:
					runTransaction(random);
					break;

				default:
					break;
			}
		}
		catch (Exception e) {
			if (args.debug) {
				e.printStackTrace();
			}
			else {
				System.out.println("Exception - " + e);
			}
		}
	}

	protected void runNextCommand() {
	}

	private void readUpdate(RandomShift random) {
		if (random.nextInt(100) < args.readPct) {
			boolean isMultiBin = random.nextInt(100) < args.readMultiBinPct;

			if (args.batchSize <= 1) {
				long key = random.nextLong(keyCount);
				doRead(key, isMultiBin);
			}
			else {
				doReadBatch(random, isMultiBin);
			}
		}
		else {
			boolean isMultiBin = random.nextInt(100) < args.writeMultiBinPct;

			// Perform Single record write even if in batch mode.
			long key = random.nextLong(keyCount);
			doWrite(random, key, isMultiBin, null);
		}
	}

	private void readModifyUpdate(RandomShift random) {
		long key = random.nextLong(keyCount);

		// Read all bins.
		doRead(key, true);
		// Write one bin.
		doWrite(random, key, false, null);
	}

	private void readModifyIncrement(RandomShift random) {
		long key = random.nextLong(keyCount);

		// Read all bins.
		doRead(key, true);
		// Increment one bin.
		doIncrement(key, 1);
	}

	private void readModifyDecrement(RandomShift random) {
		long key = random.nextLong(keyCount);

		// Read all bins.
		doRead(key, true);
		// Decrement one bin.
		doIncrement(key, -1);
	}

	private void readFromFile(RandomShift random) {
		long key = random.nextLong(keyCount);

		if (args.keyType == KeyType.STRING) {
			doReadString(key, true);
		}
		else if (args.keyType == KeyType.INTEGER) {
			doReadLong(key, true);
		}
	}

	private void runTransaction(RandomShift random) {
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
					doRead(getKeys(random, thisItem.getRepetitions()), true);
					break;
				case MULTI_BIN_REPLACE:
					key = random.nextLong(keyCount);
					doWrite(random, key, true, args.replacePolicy);
					break;
				case MULTI_BIN_UPDATE:
					key = random.nextLong(keyCount);
					doWrite(random, key, true, args.updatePolicy);
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
					doRead(getKeys(random, thisItem.getRepetitions()), false);
					break;
				case SINGLE_BIN_REPLACE:
					key = random.nextLong(keyCount);
					doWrite(random, key, false, args.replacePolicy);
					break;
				case SINGLE_BIN_UPDATE:
					key = random.nextLong(keyCount);
					doWrite(random, key, false, args.updatePolicy);
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

	private long[] getKeys(RandomShift random, int count) {
		long[] keys = new long[count];
		for (int i = 0; i < count; i++) {
			keys[i] = random.nextLong(keyCount);
		}
		return keys;
	}

	/**
	 * Write the key at the given index
	 */
	protected void doWrite(RandomShift random, long keyIdx, boolean multiBin, WritePolicy writePolicy) {
		Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);
		// Use predictable value for 0th bin same as key value
		Bin[] bins = args.getBins(random, multiBin, keyStart + keyIdx);

		try {
			put(writePolicy, key, bins);
		}
		catch (AerospikeException ae) {
			writeFailure(ae);
			runNextCommand();
		}
		catch (Exception e) {
			writeFailure(e);
			runNextCommand();
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
		}
		catch (AerospikeException ae) {
			writeFailure(ae);
			runNextCommand();
		}
		catch (Exception e) {
			writeFailure(e);
			runNextCommand();
		}
	}

	/**
	 * Read the key at the given index.
	 */
	protected void doRead(long keyIdx, boolean multiBin) {
		try {
			Key key = new Key(args.namespace, args.setName, keyStart + keyIdx);

			// if udf has been chosen call udf
			if (args.udfValues != null) {
				get(key,args.udfPackageName,args.udfFunctionName,args.udfValues);
			}
			else if (multiBin) {
				// Read all bins, maybe validate
				get(key);
			}
			else {
				// Read one bin, maybe validate
				get(key, "0");
			}
		}
		catch (AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}
		catch (Exception e) {
			readFailure(e);
			runNextCommand();
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
			runNextCommand();
		}
		catch (Exception e) {
			readFailure(e);
			runNextCommand();
		}
	}

	/**
	 * Read batch of keys in one call.
	 */
	protected void doReadBatch(RandomShift random, boolean multiBin) {
		Key[] keys = new Key[args.batchSize];

		if (args.batchNamespaces == null) {
			for (int i = 0; i < keys.length; i++) {
				long keyIdx = random.nextLong(keyCount);
				keys[i] = new Key(args.namespace, args.setName, keyStart + keyIdx);
			}
		}
		else {
			int nslen = args.batchNamespaces.length;

			for (int i = 0; i < keys.length; i++) {
				long keyIdx = random.nextLong(keyCount);
				keys[i] = new Key(args.batchNamespaces[i % nslen], args.setName, keyStart + keyIdx);
			}
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
			runNextCommand();
		}
		catch (Exception e) {
			readFailure(e);
			runNextCommand();
		}
	}

	/**
	 * Read the keys of type Integer from the file supplied.
	 */
	protected void doReadLong(long keyIdx, boolean multiBin) {
		// Warning: read from file only works when keyStart + keyIdx < Integer.MAX_VALUE.
		long numKey = Long.parseLong(AerospikeBenchmark.keyList.get((int)(keyStart + keyIdx)));


		try {
			// if udf has been chosen call udf
			if (args.udfValues != null) {
				get(new Key(args.namespace, args.setName, numKey),args.udfPackageName,args.udfFunctionName,args.udfValues);
			}
			else if (multiBin) {
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
			runNextCommand();
		}
		catch (Exception e) {
			readFailure(e);
			runNextCommand();
		}
	}

	/**
	 * Read the keys of type String from the file supplied.
	 */
	protected void doReadString(long keyIdx,boolean multiBin) {
		// Warning: read from file only works when keyStart + keyIdx < Integer.MAX_VALUE.
		String strKey = AerospikeBenchmark.keyList.get((int)(keyStart+keyIdx));

		try {
			if (args.udfValues != null) {
				get(new Key(args.namespace, args.setName, strKey),args.udfPackageName,args.udfFunctionName,args.udfValues);
			}
			else if (multiBin) {
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
			runNextCommand();
		}
		catch (Exception e) {
			readFailure(e);
			runNextCommand();
		}
	}

	protected void processRead(Key key, Record record) {
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();
		}
		else {
			counters.read.count.getAndIncrement();
		}
	}

	protected void processRead(Key key, Object udfReturnValue) {
		if (udfReturnValue == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();
		}
		else {
			counters.read.count.getAndIncrement();
		}
	}

	protected void processBatchRead() {
		counters.read.count.getAndIncrement();
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

	protected abstract void put(WritePolicy policy, Key key, Bin[] bins);
	protected abstract void add(Key key, Bin[] bins);
	protected abstract void get(Key key, String binName);
	protected abstract void get(Key key);
	protected abstract void get(Key key, String udfPackageName, String udfFunctionName, Value[] udfValues);
	protected abstract void get(Key[] keys);
	protected abstract void get(Key[] keys, String binName);
}
