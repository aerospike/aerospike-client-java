/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;

public class BatchOperate extends Example {

	private static final String KeyPrefix = "bkey";
	private static final String BinName1 = "bin1";
	private static final String BinName2 = "bin2";
	private static final String BinName3 = "bin3";
	private static final String BinName4 = "bin4";
	private static final String ResultName1 = "result1";
	private static final String ResultName2 = "result2";
	private static final int RecordCount = 8;

	public BatchOperate(Console console) {
		super(console);
	}

	@Override
	public void runExample(IAerospikeClient client, Parameters params) {
		writeRecords(client, params);
		batchReadOperate(client, params);
		batchReadOperateComplex(client, params);
		batchListReadOperate(client, params);
		batchListWriteOperate(client, params);
		batchWriteOperateComplex(client, params);
	}

	private void writeRecords(
		IAerospikeClient client,
		Parameters params
	) {
		for (int i = 1; i <= RecordCount; i++) {
			Key key = new Key(params.namespace, params.set, KeyPrefix + i);
			Bin bin1 = new Bin(BinName1, i);
			Bin bin2 = new Bin(BinName2, i + 10);

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}
			Bin bin3 = new Bin(BinName3, list);

			console.info("Put: ns=%s set=%s key=%s val1=%s val2=%s val3=%s",
				key.namespace, key.setName, key.userKey, bin1.value, bin2.value, list.toString());

			client.put(params.writePolicy, key, bin1, bin2, bin3);
		}
	}

	/**
	 * Perform read operation expressions in one batch.
	 */
	private void batchReadOperate(IAerospikeClient client, Parameters params) {
		console.info("batchReadOperate");
		Key[] keys = new Key[RecordCount];
		for (int i = 0; i < RecordCount; i++) {
			keys[i] = new Key(params.namespace, params.set, KeyPrefix + (i + 1));
		}

		// bin1 * bin2
		Expression exp = Exp.build(Exp.mul(Exp.intBin(BinName1), Exp.intBin(BinName2)));

		Record[] records = client.get(null, keys, ExpOperation.read(ResultName1, exp, ExpReadFlags.DEFAULT));

		for (int i = 0; i < records.length; i++) {
			Record record = records[i];
			console.info("Result[%d]: %d", i, record.getInt(ResultName1));
		}
	}

	/**
	 * Read results using varying read operations in one batch.
	 */
	private void batchReadOperateComplex(IAerospikeClient client, Parameters params) {
		console.info("batchReadOperateComplex");
		Expression exp1 = Exp.build(Exp.mul(Exp.intBin(BinName1), Exp.intBin(BinName2)));
		Expression exp2 = Exp.build(Exp.add(Exp.intBin(BinName1), Exp.intBin(BinName2)));
		Expression exp3 = Exp.build(Exp.sub(Exp.intBin(BinName1), Exp.intBin(BinName2)));

		// Batch uses pointer reference to quickly determine if operations are repeated and can therefore
		// be optimized, but using varargs directly always creates a new reference. Therefore, save operation
		// array so we have one pointer reference per operation array.
		Operation[] ops1 = Operation.array(ExpOperation.read(ResultName1, exp1, ExpReadFlags.DEFAULT));
		Operation[] ops2 = Operation.array(ExpOperation.read(ResultName1, exp2, ExpReadFlags.DEFAULT));
		Operation[] ops3 = Operation.array(ExpOperation.read(ResultName1, exp3, ExpReadFlags.DEFAULT));
		Operation[] ops4 = Operation.array(ExpOperation.read(ResultName1, exp2, ExpReadFlags.DEFAULT),
										   ExpOperation.read(ResultName2, exp3, ExpReadFlags.DEFAULT));

		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 1), ops1));
		// The following record is optimized (namespace,set,ops are only sent once) because
		// namespace, set and ops all have the same pointer references as the previous entry.
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 2), ops1));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 3), ops2));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 4), ops3));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 5), ops4));

		// Execute batch.
		client.get(null, records);

		// Show results.
		int count = 0;
		for (BatchRead record : records) {
			if (record.resultCode == 0) {
				Record rec = record.record;
				Object v1 = rec.getValue(ResultName1);
				Object v2 = rec.getValue(ResultName2);
				console.info("Result[%d]: %s, %s", count++, v1, v2);
			}
			else {
				console.info("Result[%d]: error: %s", count++, ResultCode.getResultString(record.resultCode));
			}
		}
	}

	/**
	 * Perform list read operations in one batch.
	 */
	private void batchListReadOperate(IAerospikeClient client, Parameters params) {
		console.info("batchListReadOperate");
		Key[] keys = new Key[RecordCount];
		for (int i = 0; i < RecordCount; i++) {
			if (i == 5) {
				keys[i] = new Key(params.namespace, params.set, "not found");
				continue;
			}
			keys[i] = new Key(params.namespace, params.set, KeyPrefix + (i + 1));
		}

		// Get size and last element of list bin for all records.
		Record[] records = client.get(null, keys,
			ListOperation.size(BinName3),
			ListOperation.getByIndex(BinName3, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < records.length; i++) {
			Record record = records[i];
			//System.out.println(record);

			if (record != null) {
				List<?> results = record.getList(BinName3);
				long size = (Long)results.get(0);
				Object val = results.get(1);

				console.info("Result[%d]: %d,%s", i, size, val);
			}
			else {
				console.info("Result[%d]: null", i);
			}
		}
	}

	/**
	 * Perform list read/write operations in one batch.
	 */
	private void batchListWriteOperate(IAerospikeClient client, Parameters params) {
		console.info("batchListWriteOperate");
		Key[] keys = new Key[RecordCount];
		for (int i = 0; i < RecordCount; i++) {
			keys[i] = new Key(params.namespace, params.set, KeyPrefix + (i + 1));
		}

		// Add integer to list and get size and last element of list bin for all records.
		BatchResults bresults = client.operate(null, null, keys,
			ListOperation.append(ListPolicy.Default, BinName3, Value.get(999)),
			ListOperation.size(BinName3),
			ListOperation.getByIndex(BinName3, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < bresults.records.length; i++) {
			BatchRecord br = bresults.records[i];

			if (br.resultCode == 0) {
				Record rec = br.record;
				List<?> results = rec.getList(BinName3);
				long size = (Long)results.get(1);
				Object val = results.get(2);

				console.info("Result[%d]: %d,%s", i, size, val);
			}
			else {
				console.info("Result[%d]: error: %s", i, ResultCode.getResultString(br.resultCode));
			}
		}
	}

	/**
	 * Read/Write records using varying operations in one batch.
	 */
	private void batchWriteOperateComplex(IAerospikeClient client, Parameters params) {
		console.info("batchWriteOperateComplex");
		Expression wexp1 = Exp.build(Exp.add(Exp.intBin(BinName1), Exp.intBin(BinName2), Exp.val(1000)));
		Expression rexp1 = Exp.build(Exp.mul(Exp.intBin(BinName1), Exp.intBin(BinName2)));
		Expression rexp2 = Exp.build(Exp.add(Exp.intBin(BinName1), Exp.intBin(BinName2)));
		Expression rexp3 = Exp.build(Exp.sub(Exp.intBin(BinName1), Exp.intBin(BinName2)));

		Operation[] ops1 = Operation.array(
			Operation.put(new Bin(BinName4, 100)),
			ExpOperation.read(ResultName1, rexp1, ExpReadFlags.DEFAULT));

		Operation[] ops2 = Operation.array(ExpOperation.read(ResultName1, rexp1, ExpReadFlags.DEFAULT));
		Operation[] ops3 = Operation.array(ExpOperation.read(ResultName1, rexp2, ExpReadFlags.DEFAULT));

		Operation[] ops4 = Operation.array(
			ExpOperation.write(BinName1, wexp1, ExpWriteFlags.DEFAULT),
			ExpOperation.read(ResultName1, rexp3, ExpReadFlags.DEFAULT));

		Operation[] ops5 = Operation.array(
			ExpOperation.read(ResultName1, rexp2, ExpReadFlags.DEFAULT),
			ExpOperation.read(ResultName2, rexp3, ExpReadFlags.DEFAULT));

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(new BatchWrite(new Key(params.namespace, params.set, KeyPrefix + 1), ops1));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 2), ops2));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 3), ops3));
		records.add(new BatchWrite(new Key(params.namespace, params.set, KeyPrefix + 4), ops4));
		records.add(new BatchRead(new Key(params.namespace, params.set, KeyPrefix + 5), ops5));
		records.add(new BatchDelete(new Key(params.namespace, params.set, KeyPrefix + 6)));

		// Execute batch.
		client.operate(null, records);

		// Show results.
		int i = 0;
		for (BatchRecord record : records) {
			if (record.resultCode == 0) {
				Record rec = record.record;
				Object v1 = rec.getValue(ResultName1);
				Object v2 = rec.getValue(ResultName2);
				console.info("Result[%d]: %s, %s", i, v1, v2);
			}
			else {
				console.info("Result[%d]: error: %s", i, ResultCode.getResultString(record.resultCode));
			}
			i++;
		}
	}
}
