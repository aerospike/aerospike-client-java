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
import com.aerospike.client.Txn;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

import java.util.Arrays;
import java.util.List;

public class TestTxn extends TestSync {
	public static final String binName = "bin1";

	@BeforeClass
	public static void register() {
		if (args.useProxyClient) {
			System.out.println("Skip TestTxn.register");
			return;
		}
		RegisterTask task = client.register(null, TestUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	//================================================================================================
	public static void queryKey(String expected_val) {

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.equal(binName, expected_val));

		RecordSet rs = client.query(null, stmt);

		int recordCount = 0;
		// Get the results
		try{
		    while(rs.next()){
		    	
		        Key key = rs.getKey();
		        Record record = rs.getRecord();
		        // Do something
		        System.out.format("Key: %s | Record: %s\\n", key.userKey, record.bins); 
		        recordCount++;
		    }
		}
		finally{
		    rs.close();
		}
		
	    if (recordCount == 0) {
	        throw new AssertionError("No records found for the given query!");
	    }
	}

	// Test1
	// No crash test
	// Steps:
	// 1- insert one key
	// 2- create sindex testsindex
	// 3- Begin Tran
	//  - Update bin
	//	- Commit
	// 4- No crash of server 
	@Test
	public void txnWriteWithSindex() {
		//insert one key
		Key key = new Key(args.namespace, args.set, "mrtkey1");
		String sindexname = "testsindex";

		client.put(null, key, new Bin(binName, 1));
		
		//create sindex
		try {
			IndexTask task = client.createIndex(args.indexPolicy, args.namespace, args.set, sindexname, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				IndexTask task = client.dropIndex(args.indexPolicy, args.namespace, args.set, sindexname);
				task.waitTillComplete();
				throw ae;
			}
		}
		
		//begin tran
		Txn txn = new Txn();

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;
		client.put(wp, key, new Bin(binName, 2));

		client.commit(txn);

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, 2);
		
		IndexTask task = client.dropIndex(args.indexPolicy, args.namespace, args.set, sindexname);
		task.waitTillComplete();
	}
	
	// Test2:
	// Query provisional and original record.
	// 1 Insert one record with Bin1 with value val1.
	// 2 Create sindex
	// 3 Begin Tran
	//	-	Query for Bin1 -> should get original value should get val1
	//	- 	Update Bin1 value to val2
	//	-	Query for Bin1 -> should get origin value i.e val1 (not commited yet)
	//	-commit tran
	//  - Query for Bin1 -> should get commited value i.e val2 (after commit)
	@Test
	public void txnWriteWithSindex1() {
		//insert one key
		Key key = new Key(args.namespace, args.set, "mrtkey1");

		client.put(null, key, new Bin(binName, "val1"));
		
		//create sindex
		try {
			IndexTask task = client.createIndex(args.indexPolicy, args.namespace, args.set, "testsindex", binName, IndexType.STRING);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				IndexTask task = client.dropIndex(args.indexPolicy, args.namespace, args.set, "testsindex");
				task.waitTillComplete();
				throw ae;
			}
		}
		
		//begin tran
//        System.out.println("Begin tran----s\\n"); 
		Txn txn = new Txn();

			
		WritePolicy wp = client.copyWriteMAX_ERROR_RATEPolicyDefault();
		wp.txn = txn;
//		System.out.println("query with binvalue as val1----s\\n");
		queryKey("val1");
		client.put(wp, key, new Bin(binName, "val2"));
		queryKey("val1");
		client.commit(txn);
		queryKey("val2");
		System.out.println("\ncommit tran success----s\\n"); 

		Record record = client.get(null, key);
		assertBinEqual(key, record, binName, "val2");
		
		IndexTask task = client.dropIndex(args.indexPolicy, args.namespace, args.set, "testsindex");
		task.waitTillComplete();
	}
	
	//================================================================================================

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
			Key key = keys[i];
			Record rec = recs[i];

			assertNotNull(rec);

			int received = rec.getInt(binName);
			assertEquals(expected, received);
		}
	}
}
