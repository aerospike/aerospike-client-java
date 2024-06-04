/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.Tran;

public class TestTran extends TestSync {
	public static final String binName = "bin";

	@BeforeClass
	public static void register() {
		if (args.useProxyClient) {
			System.out.println("Skip TestTran.register");
			return;
		}
		RegisterTask task = client.register(null, TestUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Test
	public void tranWrite() {
		Key key = new Key(args.namespace, args.set, "mrtkey1");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void tranWriteTwice() {
		Key key = new Key(args.namespace, args.set, "mrtkey2");

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val1"));
		client.put(wp, key, new Bin(binName, "val2"));

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void tranWriteBlock() {
		Key key = new Key(args.namespace, args.set, "mrtkey3");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
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

		client.tranCommit(tran);
	}

	@Test
	public void tranWriteRead() {
		Key key = new Key(args.namespace, args.set, "mrtkey4");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");

		client.tranCommit(tran);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void tranWriteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey5");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		Policy p = client.copyReadPolicyDefault();
		p.tran = tran;
		Record record = client.get(p, key);
		assertBinEqual(key, record, binName, "val2");

		client.tranAbort(tran);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranDelete() {
		Key key = new Key(args.namespace, args.set, "mrtkey6");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertNull(record);
	}

	@Test
	public void tranDeleteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey7");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.tranAbort(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranDeleteTwice() {
		Key key = new Key(args.namespace, args.set, "mrtkey8");

		Tran tran = client.tranBegin();

		client.put(null, key, new Bin(binName, "val1"));

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		wp.durableDelete = true;
		client.delete(wp, key);
		client.delete(wp, key);

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertNull(record);
	}

	@Test
	public void tranTouch() {
		Key key = new Key(args.namespace, args.set, "mrtkey9");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.touch(wp, key);

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranTouchAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey10");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.touch(wp, key);

		client.tranAbort(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranOperateWrite() {
		Key key = new Key(args.namespace, args.set, "mrtkey11");

		client.put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		Record record = client.operate(wp, key,
			Operation.put(new Bin(binName, "val2")),
			Operation.get("bin2")
		);
		assertBinEqual(key, record, "bin2", "bal1");

		client.tranCommit(tran);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void tranOperateWriteAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey12");

		client.put(null, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		Record record = client.operate(wp, key,
			Operation.put(new Bin(binName, "val2")),
			Operation.get("bin2")
		);
		assertBinEqual(key, record, "bin2", "bal1");

		client.tranAbort(tran);

		record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranUDF() {
		Key key = new Key(args.namespace, args.set, "mrtkey13");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.execute(wp, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.tranCommit(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
	}

	@Test
	public void tranUDFAbort() {
		Key key = new Key(args.namespace, args.set, "mrtkey14");

		client.put(null, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.tran = tran;
		client.execute(wp, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.tranAbort(tran);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val1");
	}

	@Test
	public void tranBatch() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, i);
			keys[i] = key;

			client.put(null, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Tran tran = client.tranBegin();

		bin = new Bin(binName, 2);

		BatchPolicy bp = BatchPolicy.WriteDefault();
		bp.tran = tran;

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

		client.tranCommit(tran);

		recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 2);
	}

	@Test
	public void tranBatchAbort() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, i);
			keys[i] = key;

			client.put(null, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Tran tran = client.tranBegin();

		bin = new Bin(binName, 2);

		BatchPolicy bp = BatchPolicy.WriteDefault();
		bp.tran = tran;

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

		client.tranAbort(tran);

		recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);
	}

	private void assertBatchEqual(Key[] keys, Record[] recs, int expected) {
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			Record rec = recs[i];

			assertNotNull(rec);

			int received = rec.getInt(binName);
			assertEquals(expected, received);
		}
	}
}
