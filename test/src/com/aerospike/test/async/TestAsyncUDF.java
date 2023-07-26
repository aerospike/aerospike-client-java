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
package com.aerospike.test.async;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.task.RegisterTask;

public class TestAsyncUDF extends TestAsync {
	private static final String binName = "audfbin1";

	@BeforeClass
	public static void prepare() {
		if (args.useProxyClient) {
			System.out.println("Skip TestAsyncUDF.prepare");
			return;
		}
		RegisterTask rtask = client.register(null, TestAsyncUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		rtask.waitTillComplete();
	}

	@Test
	public void asyncUDF() {
		final Key key = new Key(args.namespace, args.set, "audfkey1");
		final Bin bin = new Bin(binName, "string value");

		client.execute(eventLoop, new ExecuteListener() {

			public void onSuccess(final Key key, final Object obj) {
				try {
					// Write succeeded.  Now call read using udf.
					client.execute(eventLoop, new ExecuteListener() {

						public void onSuccess(final Key key, final Object received) {
							Object expected = bin.value.getObject();

							if (assertNotNull(received)) {
								assertEquals(expected, received);
							}
							notifyComplete();
						}

						public void onFailure(AerospikeException e) {
							setError(e);
							notifyComplete();
						}

					}, null, key, "record_example", "readBin", Value.get(bin.name));
				}
				catch (Exception e) {
					setError(e);
					notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}

		}, null, key, "record_example", "writeBin", Value.get(bin.name), bin.value);

		waitTillComplete();
	}

	@Test
	public void asyncBatchUDF() {
		Key[] keys = new Key[] {
			new Key(args.namespace, args.set, 20000),
			new Key(args.namespace, args.set, 20001)
		};

		client.delete(null, null, keys);

		client.execute(null, new BatchRecordArrayListener() {
			public void onSuccess(BatchRecord[] records, boolean status) {
				try {
					if (assertTrue(status)) {
						for (BatchRecord r : records) {
							if (assertNotNull(r)) {
								assertEquals(0, r.resultCode);
							}
						}
					}
					notifyComplete();
				}
				catch (Exception e) {
					setError(e);
					notifyComplete();
				}
			}

			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				setError(ae);
				notifyComplete();
			}
		}, null, null, keys, "record_example", "writeBin", Value.get("B5"), Value.get("value5"));

		waitTillComplete();
	}

	@Test
	public void asyncBatchUDFComplex() {
		List<BatchRecord> records = new ArrayList<BatchRecord>();
		String bin = "B5";

		Value[] a1 = new Value[] {Value.get(bin), Value.get("value1")};
		Value[] a2 = new Value[] {Value.get(bin), Value.get(5)};
		Value[] a3 = new Value[] {Value.get(bin), Value.get(999)};

		BatchUDF b1 = new BatchUDF(new Key(args.namespace, args.set, 20014), "record_example", "writeBin", a1);
		BatchUDF b2 = new BatchUDF(new Key(args.namespace, args.set, 20015), "record_example", "writeWithValidation", a2);
		BatchUDF b3 = new BatchUDF(new Key(args.namespace, args.set, 20015), "record_example", "writeWithValidation", a3);

		records.add(b1);
		records.add(b2);
		records.add(b3);

		client.operate(null, new BatchRecordSequenceListener() {
			public void onRecord(BatchRecord br, int index) {
				try {
					switch (index) {
					case 0:
						assertBinEqual(br.key, br.record, bin, 0);
						break;

					case 1:
						assertBinEqual(br.key, br.record, bin, 0);
						break;

					case 2:
						assertEquals(ResultCode.UDF_BAD_RESPONSE, br.resultCode);
						break;
					}
				}
				catch (Exception e) {
					setError(e);
					notifyComplete();
				}
			}

			public void onSuccess() {
				BatchRead b4 = new BatchRead(new Key(args.namespace, args.set, 20014), true);
				BatchRead b5 = new BatchRead(new Key(args.namespace, args.set, 20015), true);
				records.clear();
				records.add(b4);
				records.add(b5);

				client.operate(null, new BatchRecordSequenceListener() {
					public void onRecord(BatchRecord br, int index) {
						try {
							switch (index) {
							case 0:
								assertBinEqual(br.key, br.record, bin, "value1");
								break;

							case 1:
								assertBinEqual(br.key, br.record, bin, 5);
								break;
							}
						}
						catch (Exception e) {
							setError(e);
							notifyComplete();
						}
					}

					public void onSuccess() {
						notifyComplete();
					}

					public void onFailure(AerospikeException ae) {
						setError(ae);
						notifyComplete();
					}
				}, null, records);
			}

			public void onFailure(AerospikeException ae) {
				setError(ae);
				notifyComplete();
			}
		}, null, records);

		waitTillComplete();
	}
}
