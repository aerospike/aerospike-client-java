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
import com.aerospike.client.Record;
import com.aerospike.client.Tran;
import com.aerospike.client.listener.RecordListener;
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
			//new Put(tran, key, "val2"),
			//new Commit(tran),
			//new GetExpect(null, key, "val2")
		};

		Cmds cmd = new Cmds(cmds);
		cmd.runNext();
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
		final Tran tran;
		final Runner[] cmds;
		int idx;

		private Cmds(Runner[] cmds) {
			this.tran = new Tran();
			this.cmds = cmds;
			this.idx = -1;
		}

		private void runNext() {
			if (++idx == cmds.length) {
				System.out.println("Done");
				notifyComplete();
				return;
			}

			try {
				System.out.println("Run command");
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

	private class Put implements Runner {
		private final Tran tran;
		private final Key key;
		private final String val;

		private Put(Tran tran, Key key, String val) {
			this.tran = tran;
			this.key = key;
			this.val = val;
		}

		public void run(Listener listener) {
			System.out.println("Run put");
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
			client.put(eventLoop, wl, wp, key, new Bin(binName, val));
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
					if (assertBinEqual(key, record, binName, expect)) {
						listener.onSuccess();
					}
					else {
						listener.onFailure();
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

	private interface Runner {
		void run(Listener listener);
	}

	private interface Listener {
		void onSuccess();
		void onFailure();
		void onFailure(Exception e);
	}
}
