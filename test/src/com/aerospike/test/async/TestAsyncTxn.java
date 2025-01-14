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
package com.aerospike.test.async;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AbortStatus;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.listener.AbortListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.CommitListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.basic.TestUDF;

public class TestAsyncTxn extends TestAsync {
	public static final String binName = "bin";

	@BeforeClass
	public static void register() {
		// Transactions require strong consistency namespaces.
		org.junit.Assume.assumeTrue(args.scMode);
		RegisterTask task = client.register(null, TestUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Test
	public void asyncTxnWrite() {
		Key key = new Key(args.namespace, args.set, "asyncTxnWrite");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Put(txn, key, "val2"),
			new Commit(txn),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnWriteTwice() {
		Key key = new Key(args.namespace, args.set, "asyncTxnWriteTwice");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(txn, key, "val1"),
			new Put(txn, key, "val2"),
			new Commit(txn),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnWriteBlock() {
		Key key = new Key(args.namespace, args.set, "asyncTxnWriteBlock");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Put(txn, key, "val2"),
			new Put(null, key, "val3"), // Should be blocked
			new Commit(txn),
		};

		try {
			execute(cmds);
			throw new AerospikeException("Unexpected success");
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.MRT_BLOCKED) {
				throw ae;
			}
		}
	}

	@Test
	public void asyncTxnWriteRead() {
		Key key = new Key(args.namespace, args.set, "asyncTxnWriteRead");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Put(txn, key, "val2"),
			new GetExpect(null, key, "val1"),
			new Commit(txn),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnWriteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTxnWriteAbort");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Put(txn, key, "val2"),
			new GetExpect(txn, key, "val2"),
			new Abort(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnDelete() {
		Key key = new Key(args.namespace, args.set, "asyncTxnDelete");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Delete(txn, key),
			new Commit(txn),
			new GetExpect(null, key, null)
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnDeleteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTxnDeleteAbort");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Delete(txn, key),
			new Abort(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnDeleteTwice() {
		Key key = new Key(args.namespace, args.set, "asyncTxnDeleteTwice");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Delete(txn, key),
			new Delete(txn, key),
			new Commit(txn),
			new GetExpect(null, key, null)
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnTouch() {
		Key key = new Key(args.namespace, args.set, "asyncTxnTouch");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Touch(txn, key),
			new Commit(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnTouchAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTxnTouchAbort");
		Txn txn = new Txn();

		Runner[] cmds = new Runner[] {
			new Put(null, key, "val1"),
			new Touch(txn, key),
			new Abort(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnOperateWrite() {
		Key key = new Key(args.namespace, args.set, "asyncTxnOperateWrite3");
		Txn txn = new Txn();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[] {
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new OperateExpect(txn, key,
				bin2,
				Operation.put(new Bin(binName, "val2")),
				Operation.get(bin2.name)
			),
			new Commit(txn),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnOperateWriteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTxnOperateWriteAbort");
		Txn txn = new Txn();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[] {
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new OperateExpect(txn, key,
				bin2,
				Operation.put(new Bin(binName, "val2")),
				Operation.get(bin2.name)
			),
			new Abort(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnUDF() {
		Key key = new Key(args.namespace, args.set, "asyncTxnUDF");
		Txn txn = new Txn();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[] {
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new UDF(txn, key, "record_example", "writeBin", Value.get(binName), Value.get("val2")),
			new Commit(txn),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnUDFAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTxnUDFAbort");
		Txn txn = new Txn();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[] {
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new UDF(txn, key, "record_example", "writeBin", Value.get(binName), Value.get("val2")),
			new Abort(txn),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnBatch() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, "asyncTxnBatch" + i);
			keys[i] = key;
			client.put(null, key, bin);
		}

		Txn txn = new Txn();
		bin = new Bin(binName, 2);

		Runner[] cmds = new Runner[] {
			new BatchGetExpect(null, keys, 1),
			new BatchOperate(txn, keys, Operation.put(bin)),
			new Commit(txn),
			new BatchGetExpect(null, keys, 2),
		};

		execute(cmds);
	}

	@Test
	public void asyncTxnBatchAbort() {
		Key[] keys = new Key[10];
		Bin bin = new Bin(binName, 1);

		for (int i = 0; i < keys.length; i++) {
			Key key = new Key(args.namespace, args.set, "asyncTxnBatch" + i);
			keys[i] = key;
			client.put(null, key, bin);
		}

		Txn txn = new Txn();
		bin = new Bin(binName, 2);

		Runner[] cmds = new Runner[] {
			new BatchGetExpect(null, keys, 1),
			new BatchOperate(txn, keys, Operation.put(bin)),
			new Abort(txn),
			new BatchGetExpect(null, keys, 1),
		};

		execute(cmds);
	}

	private void execute(Runner[] cmdArray) {
		Cmds a = new Cmds(cmdArray);
		a.runNext();
		waitTillComplete();
	}

	private void onError(Exception e) {
		setError(e);
		notifyComplete();
	}

	private void onError() {
		// Error is located in monitor instance which is checked in waitTillComplete();
		notifyComplete();
	}

	private class Cmds implements Listener {
		final Runner[] cmds;
		int idx;

		private Cmds(Runner[] cmds) {
			this.cmds = cmds;
			this.idx = -1;
		}

		private void runNext() {
			if (++idx == cmds.length) {
				notifyComplete();
				return;
			}

			try {
				cmds[idx].run(this);
			}
			catch (Exception e) {
				onError(e);
			}
		}

		public void onSuccess() {
			runNext();
		}

		public void onFailure() {
			onError();
		}

		public void onFailure(Exception e) {
			onError(e);
		}
	}

	private class Commit implements Runner {
		private final Txn txn;

		private Commit(Txn txn) {
			this.txn = txn;
		}

		public void run(Listener listener) {
			CommitListener tcl = new CommitListener() {
				public void onSuccess(CommitStatus status) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException.Commit ae) {
					listener.onFailure(ae);
				}
			};

			client.commit(eventLoop, tcl, txn);
		}
	}

	private class Abort implements Runner {
		private final Txn txn;

		private Abort(Txn txn) {
			this.txn = txn;
		}

		public void run(Listener listener) {
			AbortListener tal = new AbortListener() {
				public void onSuccess(AbortStatus status) {
					listener.onSuccess();
				}
			};

			client.abort(eventLoop, tal, txn);
		}
	}

	private class Put implements Runner {
		private final Txn txn;
		private final Key key;
		private final Bin[] bins;

		private Put(Txn txn, Key key, String val) {
			this.txn = txn;
			this.key = key;
			this.bins = new Bin[] {new Bin(binName, val)};
		}

		private Put(Txn txn, Key key, Bin... bins) {
			this.txn = txn;
			this.key = key;
			this.bins = bins;
		}

		public void run(Listener listener) {
			WriteListener wl = new WriteListener() {
				public void onSuccess(final Key key) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			WritePolicy wp = null;

			if (txn != null) {
				wp = client.copyWritePolicyDefault();
				wp.txn = txn;
			}
			client.put(eventLoop, wl, wp, key, bins);
		}
	}

	private class GetExpect implements Runner {
		private final Txn txn;
		private final Key key;
		private final String expect;

		private GetExpect(Txn txn, Key key, String expect) {
			this.txn = txn;
			this.key = key;
			this.expect = expect;
		}

		public void run(Listener listener) {
			RecordListener rl = new RecordListener() {
				public void onSuccess(Key key, Record record) {
					if (expect != null) {
						if (assertBinEqual(key, record, binName, expect)) {
							listener.onSuccess();
						}
						else {
							listener.onFailure();
						}
					}
					else {
						if (assertRecordNotFound(key, record)) {
							listener.onSuccess();
						}
						else {
							listener.onFailure();
						}
					}
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			Policy p = null;

			if (txn != null) {
				p = client.copyReadPolicyDefault();
				p.txn = txn;
			}
			client.get(eventLoop, rl, p, key);
		}
	}

	private class OperateExpect implements Runner {
		private final Txn txn;
		private final Key key;
		private final Operation[] ops;
		private final Bin expect;

		private OperateExpect(Txn txn, Key key, Bin expect, Operation... ops) {
			this.txn = txn;
			this.key = key;
			this.expect = expect;
			this.ops = ops;
		}

		public void run(Listener listener) {
			RecordListener rl = new RecordListener() {
				public void onSuccess(Key key, Record record) {
					if (expect != null) {
						if (assertBinEqual(key, record, expect.name, expect.value.getObject())) {
							listener.onSuccess();
						}
						else {
							listener.onFailure();
						}
					}
					else {
						if (assertRecordNotFound(key, record)) {
							listener.onSuccess();
						}
						else {
							listener.onFailure();
						}
					}
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			WritePolicy wp = null;

			if (txn != null) {
				wp = client.copyWritePolicyDefault();
				wp.txn = txn;
			}
			client.operate(eventLoop, rl, wp, key, ops);
		}
	}

	private class UDF implements Runner {
		private final Txn txn;
		private final Key key;
		private final String packageName;
		private final String functionName;
		private final Value[] functionArgs;

		private UDF(
			Txn txn,
			Key key,
			String packageName,
			String functionName,
			Value... functionArgs
		) {
			this.txn = txn;
			this.key = key;
			this.packageName = packageName;
			this.functionName = functionName;
			this.functionArgs = functionArgs;
		}

		public void run(Listener listener) {
			ExecuteListener el = new ExecuteListener() {
				public void onSuccess(Key key, Object obj) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			WritePolicy wp = null;

			if (txn != null) {
				wp = client.copyWritePolicyDefault();
				wp.txn = txn;
			}
			client.execute(eventLoop, el, wp, key,  packageName, functionName, functionArgs);
		}
	}

	private class BatchGetExpect implements Runner {
		private final Txn txn;
		private final Key[] keys;
		private final int expected;

		private BatchGetExpect(Txn txn, Key[] keys, int expected) {
			this.txn = txn;
			this.keys = keys;
			this.expected = expected;
		}

		public void run(Listener listener) {
			RecordArrayListener ral = new RecordArrayListener() {
				public void onSuccess(Key[] keys, Record[] records) {
					if (assertBatchEqual(keys, records, binName, expected)) {
						listener.onSuccess();
					}
					else {
						listener.onFailure();
					}
				}

				public void onFailure(AerospikeException ae) {
					listener.onFailure(ae);
				}
			};

			BatchPolicy bp = null;

			if (txn != null) {
				bp = client.copyBatchPolicyDefault();
				bp.txn = txn;
			}
			client.get(eventLoop, ral, bp, keys);
		}
	}

	private class BatchOperate implements Runner {
		private final Txn txn;
		private final Key[] keys;
		private final Operation[] ops;

		private BatchOperate(Txn txn, Key[] keys, Operation... ops) {
			this.txn = txn;
			this.keys = keys;
			this.ops = ops;
		}

		public void run(Listener listener) {
			BatchRecordArrayListener bral = new BatchRecordArrayListener() {
				public void onSuccess(BatchRecord[] records, boolean status) {
					if (status) {
						listener.onSuccess();
					}
					else {
						StringBuilder sb = new StringBuilder();
						sb.append("Batch failed:");
						sb.append(System.lineSeparator());

						for (BatchRecord br : records) {
							if (br.resultCode == 0) {
								sb.append("Record: " + br.record);
							}
							else {
								sb.append("ResultCode: " + br.resultCode);
							}
							sb.append(System.lineSeparator());
						}
						listener.onFailure(new AerospikeException(sb.toString()));
					}
				}

				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					listener.onFailure(ae);
				}
			};


			BatchPolicy bp = null;

			if (txn != null) {
				bp = client.copyBatchParentPolicyWriteDefault();
				bp.txn = txn;
			}
			client.operate(eventLoop, bral, bp, null, keys, ops);
		}
	}

	private class Touch implements Runner {
		private final Txn txn;
		private final Key key;

		private Touch(Txn txn, Key key) {
			this.txn = txn;
			this.key = key;
		}

		public void run(Listener listener) {
			WriteListener wl = new WriteListener() {
				public void onSuccess(final Key key) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			WritePolicy wp = null;

			if (txn != null) {
				wp = client.copyWritePolicyDefault();
				wp.txn = txn;
			}
			client.touch(eventLoop, wl, wp, key);
		}
	}

	private class Delete implements Runner {
		private final Txn txn;
		private final Key key;

		private Delete(Txn txn, Key key) {
			this.txn = txn;
			this.key = key;
		}

		public void run(Listener listener) {
			DeleteListener dl = new DeleteListener() {
				public void onSuccess(final Key key, boolean existed) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};

			WritePolicy wp = null;

			if (txn != null) {
				wp = client.copyWritePolicyDefault();
				wp.txn = txn;
				wp.durableDelete = true;
			}
			client.delete(eventLoop, dl, wp, key);
		}
	}

	private interface Runner {
		void run(Listener listener);
	}

	private interface Listener {
		void onSuccess();
		void onFailure();
		void onFailure(Exception e);
	}
}
