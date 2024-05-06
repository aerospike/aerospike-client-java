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
package com.aerospike.examples;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
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

public class MRT extends Example {
	public static final String binName = "bin";

	public MRT(Console console) {
		super(console);
	}

	/**
	 * Multi-record transaction.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		tranWrite(client, params);
		tranBlock(client, params);
		tranWriteRead(client, params);
		tranRollback(client, params);
		tranReadOutsideOfTran(client, params);
		tranDelete(client, params);
		tranDeleteAbort(client, params);
		tranTouch(client, params);
		tranTouchAbort(client, params);
		tranOperateWrite(client, params);
		tranOperateWriteAbort(client, params);
		tranUDF(client, params);
		tranUDFAbort(client, params);
		tranBatch(client, params);
		tranBatchAbort(client, params);
	}

	public void tranWrite(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey1");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		client.tranCommit(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val2");
	}

	public void tranBlock(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey2");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		try {
			// This write should be blocked.
			client.put(params.writePolicy, key, new Bin(binName, "val3"));
			throw new AerospikeException("Unexpected success");
		}
		catch (AerospikeException e) {
			if (e.getResultCode() != ResultCode.MRT_BLOCKED) {
				throw e;
			}
		}

		client.tranCommit(tran);
	}

	public void tranWriteRead(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey3");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");

		client.tranCommit(tran);

		record = client.get(params.policy, key);
		assertEqual(record, "val2");
	}

	public void tranRollback(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey4");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		Policy p = new Policy(params.policy);
		p.tran = tran;

		Record record = client.get(p, key);
		assertEqual(record, "val2");

		client.tranAbort(tran);

		record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranReadOutsideOfTran(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey5");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.put(wp, key, new Bin(binName, "val2"));

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");

		client.tranCommit(tran);
	}

	public void tranDelete(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey6");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.tranCommit(tran);

		Record record = client.get(params.policy, key);
		assertNull(record);
	}

	public void tranDeleteAbort(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey7");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		wp.durableDelete = true;
		client.delete(wp, key);

		client.tranAbort(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranTouch(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey8");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.touch(wp, key);

		client.tranCommit(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranTouchAbort(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey9");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		client.touch(wp, key);

		client.tranAbort(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranOperateWrite(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey10");

		client.put(params.writePolicy, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		Record record = client.operate(wp, key,
			Operation.put(new Bin(binName, "val2")),
			Operation.get("bin2")
			);

		assertEqual(record, "bin2", "bal1");

		client.tranCommit(tran);

		record = client.get(params.policy, key);
		assertEqual(record, "val2");
	}

	public void tranOperateWriteAbort(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mrtkey11");

		client.put(params.writePolicy, key, new Bin(binName, "val1"), new Bin("bin2", "bal1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;
		Record record = client.operate(wp, key,
				Operation.put(new Bin(binName, "val2")),
				Operation.get("bin2")
		);

		assertEqual(record, "bin2", "bal1");

		client.tranAbort(tran);

		record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranUDF(IAerospikeClient client, Parameters params) {
		if (! params.useProxyClient) {
			String filename = "record_example.lua";
			console.info("Register: " + filename);
			RegisterTask task = client.register(params.policy, "udf/record_example.lua", filename, Language.LUA);
			task.waitTillComplete();
		}

		Key key = new Key(params.namespace, params.set, "mrtkey12");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;

		client.execute(params.writePolicy, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.tranCommit(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val2");
	}

	public void tranUDFAbort(IAerospikeClient client, Parameters params) {
		if (! params.useProxyClient) {
			String filename = "record_example.lua";
			console.info("Register: " + filename);
			RegisterTask task = client.register(params.policy, "udf/record_example.lua", filename, Language.LUA);
			task.waitTillComplete();
		}

		Key key = new Key(params.namespace, params.set, "mrtkey13");

		client.put(params.writePolicy, key, new Bin(binName, "val1"));

		Tran tran = client.tranBegin();

		WritePolicy wp = new WritePolicy(params.writePolicy);
		wp.tran = tran;

		client.execute(wp, key, "record_example", "writeBin", Value.get(binName), Value.get("val2"));

		client.tranAbort(tran);

		Record record = client.get(params.policy, key);
		assertEqual(record, "val1");
	}

	public void tranBatch(IAerospikeClient client, Parameters params) {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(params.namespace, params.set, i);
			keys[i] = key;

			client.put(params.writePolicy, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Tran tran = client.tranBegin();

		bin = new Bin(binName, 2);

		BatchPolicy bp = new BatchPolicy().WriteDefault();
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

	public void tranBatchAbort(IAerospikeClient client, Parameters params) {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(params.namespace, params.set, i);
			keys[i] = key;

			client.put(params.writePolicy, key, bin);
		}

		Record[] recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);

		Tran tran = client.tranBegin();

		bin = new Bin(binName, 2);

		BatchPolicy bp = new BatchPolicy().WriteDefault();
		bp.tran = tran;

		BatchResults bresults = client.operate(bp, null, keys, Operation.put(bin));

		if (! bresults.status) {
			throw new AerospikeException("client operate failed");
		}

		bp = new BatchPolicy();
		bp.tran = tran;

		recs = client.get(bp, keys);
		assertBatchEqual(keys, recs, 2);

		client.tranAbort(tran);

		recs = client.get(null, keys);
		assertBatchEqual(keys, recs, 1);
	}

	private void assertBatchEqual(Key[] keys, Record[] recs, int expected) {
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			Record rec = recs[i];

			if (rec != null) {
				int received = rec.getInt(binName);

				if (expected != received) {
					throw new AerospikeException("Key " + key + ": Expected " + expected + " Received " + received);
				}
			}
			else {
				throw new AerospikeException("Key " + key + ": records[" + i + "] is null");
			}
		}
	}

	private void assertEqual(Record record, String expected) {
		assertNotNull(record);

		String val = record.getString(binName);

		if (!expected.equals(val)) {
			throw new AerospikeException("Expected " + expected + " Received " + val);
		}
	}

	private void assertEqual(Record record, String name, String expected) {
		assertNotNull(record);

		String val = record.getString(name);

		if (!expected.equals(val)) {
			throw new AerospikeException("Bin " + name + " Expected " + expected + " Received " + val);
		}
	}

	private void assertNotNull(Record record) {
		if (record == null) {
			throw new AerospikeException("Record is null");
		}
	}

	private void assertNull(Record record) {
		if (record != null) {
			throw new AerospikeException("Record is not null");
		}
	}
}
