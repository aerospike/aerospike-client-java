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
package com.aerospike.test.async;

import org.junit.Test;
import org.junit.BeforeClass;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Tran;
import com.aerospike.client.Value;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.TranAbortListener;
import com.aerospike.client.listener.TranCommitListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.basic.TestUDF;

public class TestAsyncTran extends TestAsync {
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
	public void asyncTranWrite() {
		Key key = new Key(args.namespace, args.set, "asyncTranWrite");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Put(tran, key, "val2"),
			new Commit(tran),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranWriteTwice() {
		Key key = new Key(args.namespace, args.set, "asyncTranWriteTwice");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(tran, key, "val1"),
			new Put(tran, key, "val2"),
			new Commit(tran),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranWriteBlock() {
		Key key = new Key(args.namespace, args.set, "asyncTranWriteBlock");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Put(tran, key, "val2"),
			new Put(null, key, "val3"), // Should be blocked
			new Commit(tran),
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
	public void asyncTranWriteRead() {
		Key key = new Key(args.namespace, args.set, "asyncTranWriteRead");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Put(tran, key, "val2"),
			new GetExpect(null, key, "val1"),
			new Commit(tran),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranWriteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTranWriteAbort");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Put(tran, key, "val2"),
			new GetExpect(tran, key, "val2"),
			new Abort(tran),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranDelete() {
		Key key = new Key(args.namespace, args.set, "asyncTranDelete");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Delete(tran, key),
			new Commit(tran),
			new GetExpect(null, key, null)
		};

		execute(cmds);
	}

	@Test
	public void asyncTranDeleteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTranDeleteAbort");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Delete(tran, key),
			new Abort(tran),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranDeleteTwice() {
		Key key = new Key(args.namespace, args.set, "asyncTranDeleteTwice");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Delete(tran, key),
			new Delete(tran, key),
			new Commit(tran),
			new GetExpect(null, key, null)
		};

		execute(cmds);
	}

	@Test
	public void asyncTranTouch() {
		Key key = new Key(args.namespace, args.set, "asyncTranTouch");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Touch(tran, key),
			new Commit(tran),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranTouchAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTranTouchAbort");
		Tran tran = new Tran();

		Runner[] cmds = new Runner[]{
			new Put(null, key, "val1"),
			new Touch(tran, key),
			new Abort(tran),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranOperateWrite() {
		Key key = new Key(args.namespace, args.set, "asyncTranOperateWrite3");
		Tran tran = new Tran();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[]{
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new OperateExpect(tran, key,
				bin2,
				Operation.put(new Bin(binName, "val2")),
				Operation.get(bin2.name)
			),
			new Commit(tran),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranOperateWriteAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTranOperateWriteAbort");
		Tran tran = new Tran();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[]{
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new OperateExpect(tran, key,
				bin2,
				Operation.put(new Bin(binName, "val2")),
				Operation.get(bin2.name)
			),
			new Abort(tran),
			new GetExpect(null, key, "val1")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranUDF() {
		Key key = new Key(args.namespace, args.set, "asyncTranUDF");
		Tran tran = new Tran();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[]{
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new UDF(tran, key, "record_example", "writeBin", Value.get(binName), Value.get("val2")),
			new Commit(tran),
			new GetExpect(null, key, "val2")
		};

		execute(cmds);
	}

	@Test
	public void asyncTranUDFAbort() {
		Key key = new Key(args.namespace, args.set, "asyncTranUDFAbort");
		Tran tran = new Tran();
		Bin bin2 = new Bin("bin2", "bal1");

		Runner[] cmds = new Runner[]{
			new Put(null, key, new Bin(binName, "val1"), bin2),
			new UDF(tran, key, "record_example", "writeBin", Value.get(binName), Value.get("val2")),
			new Abort(tran),
			new GetExpect(null, key, "val1")
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
		private final Tran tran;

		private Commit(Tran tran) {
			this.tran = tran;
		}

		public void run(Listener listener) {
			TranCommitListener tcl = new TranCommitListener() {
				private AerospikeException verifyException;

				public void onSuccess() {
					listener.onSuccess();
				}

				public void onCommitFailure(BatchRecord[] records, AerospikeException ae) {
					listener.onFailure(ae);
				}

				public void onVerifyFailure(BatchRecord[] records, AerospikeException ae) {
					verifyException = ae;
				}

				public void onAbort() {
					listener.onFailure(verifyException);
				}

				public void onAbortFailure(BatchRecord[] records, AerospikeException ae) {
					verifyException.addSuppressed(ae);
					listener.onFailure(verifyException);
				}
			};

			client.tranCommit(eventLoop, tcl, tran);
		}
	}

	private class Abort implements Runner {
		private final Tran tran;

		private Abort(Tran tran) {
			this.tran = tran;
		}

		public void run(Listener listener) {
			TranAbortListener tal = new TranAbortListener() {
				public void onSuccess() {
					listener.onSuccess();
				}

				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					listener.onFailure(ae);
				}
			};

			client.tranAbort(eventLoop, tal, tran);
		}
	}

	private class Put implements Runner {
		private final Tran tran;
		private final Key key;
		private final Bin[] bins;

		private Put(Tran tran, Key key, String val) {
			this.tran = tran;
			this.key = key;
			this.bins = new Bin[] {new Bin(binName, val)};
		}

		private Put(Tran tran, Key key, Bin... bins) {
			this.tran = tran;
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

			if (tran != null) {
				wp = client.copyWritePolicyDefault();
				wp.tran = tran;
			}
			client.put(eventLoop, wl, wp, key, bins);
		}
	}

	private class GetExpect implements Runner {
		private final Tran tran;
		private final Key key;
		private final String expect;

		private GetExpect(Tran tran, Key key, String expect) {
			this.tran = tran;
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

			if (tran != null) {
				p = client.copyReadPolicyDefault();
				p.tran = tran;
			}
			client.get(eventLoop, rl, p, key);
		}
	}

	private class OperateExpect implements Runner {
		private final Tran tran;
		private final Key key;
		private final Operation[] ops;
		private final Bin expect;

		private OperateExpect(Tran tran, Key key, Bin expect, Operation... ops) {
			this.tran = tran;
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

			if (tran != null) {
				wp = client.copyWritePolicyDefault();
				wp.tran = tran;
			}
			client.operate(eventLoop, rl, wp, key, ops);
		}
	}

	private class UDF implements Runner {
		private final Tran tran;
		private final Key key;
		private final String packageName;
		private final String functionName;
		private final Value[] functionArgs;

		private UDF(
			Tran tran,
			Key key,
			String packageName,
			String functionName,
			Value... functionArgs
		) {
			this.tran = tran;
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

			if (tran != null) {
				wp = client.copyWritePolicyDefault();
				wp.tran = tran;
			}
			client.execute(eventLoop, el, wp, key,  packageName, functionName, functionArgs);
		}
	}

	/*
	private class Batch implements Runner {
		private final Tran tran;
		private final Key[] keys;
		private final Operation[] ops;

		private Batch(Tran tran, Key[] keys, Operation... ops) {
			this.tran = tran;
			this.keys = keys;
			this.ops = ops;
		}

		public void run(Listener listener) {
			BatchRecordArrayListener bral = new BatchRecordArrayListener() {
				public void onSuccess(Key key, Object obj) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			};


			WritePolicy wp = null;

			if (tran != null) {
				BatchPolicy bp = client.BatchPolicy.WriteDefault();
				bp.tran = tran;
			}
			client.operate(eventLoop, bral, wp, keys, ops);
		}
	}
	 */

	private class Touch implements Runner {
		private final Tran tran;
		private final Key key;

		private Touch(Tran tran, Key key) {
			this.tran = tran;
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

			if (tran != null) {
				wp = client.copyWritePolicyDefault();
				wp.tran = tran;
			}
			client.touch(eventLoop, wl, wp, key);
		}
	}

	private class Delete implements Runner {
		private final Tran tran;
		private final Key key;

		private Delete(Tran tran, Key key) {
			this.tran = tran;
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

			if (tran != null) {
				wp = client.copyWritePolicyDefault();
				wp.tran = tran;
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
