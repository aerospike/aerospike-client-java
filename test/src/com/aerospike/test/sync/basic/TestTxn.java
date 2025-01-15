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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.aerospike.test.sync.TestSync;
import org.junit.Test;
import org.junit.BeforeClass;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.Txn;

public class TestTxn extends TestSync {
	public static final String binName = "bin";

	@BeforeClass
	public static void register() {
		// Transactions require strong consistency namespaces.
		org.junit.Assume.assumeTrue(args.scMode);
		RegisterTask task = client.register(null, TestUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Test
	public void txnWrite() {
		Key key = new Key(args.namespace, args.set, "mrtkey1");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, "val2"));

		client.commit(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void txnWriteTwice() {
		Key key = new Key(args.namespace, args.set, "mrtkey2");

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, "val1"));
		client.put(wp, key, new Bin(binName, "val2"));

		client.commit(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void txnWriteConflict() {
		Key key = new Key(args.namespace, args.set, "mrtkey21");

		Txn txn1 = new Txn();
		Txn txn2 = new Txn();

		WritePolicy wp1 = client.copyWritePolicyDefault();
		WritePolicy wp2 = client.copyWritePolicyDefault();
		wp1.txn = txn1;
		wp2.txn = txn2;

		client.put(wp1, key, new Bin(binName, "val1"));

		try {
			client.put(wp2, key, new Bin(binName, "val2"));
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.MRT_BLOCKED) {
				throw ae;
			}
		}

		client.commit(txn1);
		client.commit(txn2);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnWriteBlock() {
		Key key = new Key(args.namespace, args.set, "mrtkey3");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, "val2"));

		try {
			// This write should be blocked.
			client.put(null, key, new Bin(binName, "val3"));
			throw new AerospikeException("Unexpected success");
		}
		catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.MRT_BLOCKED) {
				throw e;
			}
		}

		client.commit(txn);
	}

	@Test
	public void txnWriteRead() {
		Key key = new Key(args.namespace, args.set, "mrtkey4");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, "val2"));

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");

		client.commit(txn);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void txnWriteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey5");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, "val2"));

		Policy p = client.copyReadPolicyDefault();
		p.txn = txn;
		Record record = client.get(p, key);
		assertBinEqual(key, record, binName, "val2");

		client.abort(txn);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnDelete() {
		Key key = new Key(args.namespace, args.set, "mrtkey6");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.commit(txn);

		Record record = client.get(null, key);
		assertNull(record);
	}

	@Test
	public void txnDeleteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey7");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.abort(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnDeleteTwice() {
		Key key = new Key(args.namespace, args.set, "mrtkey8");

		Txn txn = new Txn();

		client.put(null, key, new Bin(binName, "val1"));

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		wp.durableDelete = true;
		client.delete(wp, key);
		client.delete(wp, key);

		client.commit(txn);

		Record record = client.get(null, key);
		assertNull(record);
	}

	@Test
	public void txnTouch() {
		Key key = new Key(args.namespace, args.set, "mrtkey9");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.touch(wp, key);

		client.commit(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnTouchAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey10");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.touch(wp, key);

		client.abort(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnOperateWrite() {
		Key key = new Key(args.namespace, args.set, "mrtkey11");

		client.put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		Record record = client.operate(wp, key,
			Operation.put(new Bin(binName, "val2")),
			Operation.get("bin2")
		);
		assertBinEqual(key, record, "bin2", "bal1");

		client.commit(txn);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void txnOperateWriteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey12");

		client.put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		Record record = client.operate(wp, key,
			Operation.put(new Bin(binName, "val2")),
			Operation.get("bin2")
		);
		assertBinEqual(key, record, "bin2", "bal1");

		client.abort(txn);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnUDF() {
		Key key = new Key(args.namespace, args.set, "mrtkey13");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.execute(wp, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.commit(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void txnUDFAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey14");

		client.put(null, key, new Bin(binName, "val1"));

		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.execute(wp, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.abort(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void txnBatch() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, i);
			keys[i] = key;

			client.put(null, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Txn txn = new Txn();

		bin = new Bin(binName, 2);

		BatchPolicy bp = BatchPolicy.WriteDefault();
		bp.txn = txn;

		BatchResults bresults = client.operate(bp, null, keys, Operation.put(bin));

		if (!bresults.status) {
			StringBuilder sb = new StringBuilder();
			sb.append("Batch failed:");
			sb.append(System.lineSeparator());

			for (BatchRecord br : bresults.records) {
				if (br.resultCode == 0) {
					sb.append("Record: " + br.record);
				}
				else {
					sb.append("ResultCode: " + br.resultCode);
				}
				sb.append(System.lineSeparator());
			}

			throw new AerospikeException(sb.toString());
		}

		client.commit(txn);

		recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 2);
	}

	@Test
	public void txnBatchAbort() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, i);
			keys[i] = key;

			client.put(null, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Txn txn = new Txn();

		bin = new Bin(binName, 2);

		BatchPolicy bp = BatchPolicy.WriteDefault();
		bp.txn = txn;

		BatchResults bresults = client.operate(bp, null, keys, Operation.put(bin));

		if (!bresults.status) {
			StringBuilder sb = new StringBuilder();
			sb.append("Batch failed:");
			sb.append(System.lineSeparator());

			for (BatchRecord br : bresults.records) {
				if (br.resultCode == 0) {
					sb.append("Record: " + br.record);
				}
				else {
					sb.append("ResultCode: " + br.resultCode);
				}
				sb.append(System.lineSeparator());
			}

			throw new AerospikeException(sb.toString());
		}

		client.abort(txn);

		recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);
	}

	private void assertBatchEqual(Key[] keys, Record[] recs, int expected) {
		for (int i = 0; i < keys.length; i++) {
			Record rec = recs[i];

			assertNotNull(rec);

			int received = rec.getInt(binName);
			assertEquals(expected, received);
		}
	}
}
