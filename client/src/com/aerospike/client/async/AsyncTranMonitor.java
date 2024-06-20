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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Tran;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.TranMonitor;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.TranAbortListener;
import com.aerospike.client.listener.TranCommitListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AsyncTranMonitor {
	private static final String VerifyFail = "MRT verify failed. Transaction aborted.";
	private static final String VerifyAbortFail = "MRT verify and abort failed";
	private static final String VerifyCloseFail = "MRT verify and close failed";
	private static final String CloseFail = "MRT close failed";
	private static final String WillCommitFail = "MRT will commit failed";
	private static final String CommitFail = "MRT commit failed";

	private final Cluster cluster;
	private final EventLoop eventLoop;
	private final BatchPolicy verifyPolicy;
	private final BatchPolicy rollPolicy;
	private final WritePolicy writePolicy;
	private final Tran tran;
	private final Key tranKey;
	private TranCommitListener commitListener;
	private TranAbortListener abortListener;
	private BatchRecord[] verifyRecords;
	private BatchRecord[] rollRecords;
	private AerospikeException verifyException;

	public AsyncTranMonitor(
		Cluster cluster,
		EventLoop eventLoop,
		BatchPolicy verifyPolicy,
		BatchPolicy rollPolicy,
		Tran tran
	) {
		this.cluster = cluster;
		this.eventLoop = eventLoop;
		this.verifyPolicy = verifyPolicy;
		this.rollPolicy = rollPolicy;
		this.writePolicy = new WritePolicy(rollPolicy);
		this.tran = tran;
		this.tranKey = TranMonitor.getTranMonitorKey(tran);
	}

	public void commit(TranCommitListener listener) {
		commitListener = listener;

		BatchRecordArrayListener verifyListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				verifyRecords = records;

				if (status) {
					Set<Key> keySet = tran.getWrites();

					if (keySet.isEmpty()) {
						// There is nothing to roll-forward. Remove MRT monitor if it exists.
						if (tran.getNamespace() != null) {
							close(true);
						}
						else {
							notifySuccess();
						}
						return;
					}

					willRollForward();
				}
				else {
					rollBack();
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				verifyRecords = records;
				verifyException = ae;
				rollBack();
			}
		};

		verify(verifyListener);
	}

	public void abort(TranAbortListener listener) {
		abortListener = listener;

		BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				rollRecords = records;
				close(true);
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				rollRecords = records;
				notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, "MRT abort failed", ae));
			}
		};

		roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
	}

	private void verify(BatchRecordArrayListener verifyListener) {
		// Validate record versions in a batch.
		Set<Map.Entry<Key,Long>> reads = tran.getReads();
		int max = reads.size();

		if (max == 0) {
			verifyListener.onSuccess(new BatchRecord[0], true);
			return;
		}

		BatchRecord[] records = new BatchRecord[max];
		Key[] keys = new Key[max];
		Long[] versions = new Long[max];
		int count = 0;

		for (Map.Entry<Key,Long> entry : reads) {
			Key key = entry.getKey();
			keys[count] = key;
			records[count] = new BatchRecord(key, false);
			versions[count] = entry.getValue();
			count++;
		}

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, verifyListener, records);

		List<BatchNode> bns = BatchNodeList.generate(cluster, verifyPolicy, keys, records, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.TranVerify(
					executor, cluster, verifyPolicy, versions[i], records[i], bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.TranVerify(
					executor, bn, verifyPolicy, keys, versions, records);
			}
		}
		executor.execute(commands);
	}

	private void willRollForward() {
		// Tell MRT monitor that a roll-forward will commence.
		try {
			WriteListener writeListener = new WriteListener() {
				@Override
				public void onSuccess(Key key) {
					rollForward();
				}

				@Override
				public void onFailure(AerospikeException ae) {
					notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, WillCommitFail, ae));
				}
			};

			AsyncTranWillRoll command = new AsyncTranWillRoll(cluster, writeListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, WillCommitFail, t));
		}
	}

	private void rollForward() {
		try {
			BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
				@Override
				public void onSuccess(BatchRecord[] records, boolean status) {
					rollRecords = records;

					if (status) {
						close(true);
					}
					else {
						notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, CommitFail));
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, CommitFail, ae));
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_FORWARD);
		}
		catch (Throwable t) {
			notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, CommitFail, t));
		}
	}

	private void rollBack() {
		try {
			BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
				@Override
				public void onSuccess(BatchRecord[] records, boolean status) {
					rollRecords = records;

					if (status) {
						close(false);
					}
					else {
						notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, VerifyAbortFail));
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, VerifyAbortFail, ae));
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
		}
		catch (Throwable t) {
			notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, VerifyAbortFail, t));
		}
	}

	private void roll(BatchRecordArrayListener rollListener, int tranAttr) {
		Set<Key> keySet = tran.getWrites();

		if (keySet.isEmpty()) {
			rollListener.onSuccess(new BatchRecord[0], true);
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		// Copy tran roll policy because it needs to be modified.
		BatchPolicy batchPolicy = new BatchPolicy(rollPolicy);

		BatchAttr attr = new BatchAttr();
		attr.setTran(tranAttr);

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, rollListener, records);

		// generate() requires a null tran instance.
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, true, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		// Batch roll forward requires the tran instance.
		batchPolicy.tran = tran;

		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.TranRoll(
					executor, cluster, batchPolicy, records[i], bn.node, tranAttr);
			}
			else {
				commands[count++] = new AsyncBatch.TranRoll(
					executor, bn, batchPolicy, keys, records, attr);
			}
		}
		executor.execute(commands);
	}

	private void close(boolean verified) {
		try {
			DeleteListener deleteListener = new DeleteListener() {
				@Override
				public void onSuccess(Key key, boolean existed) {
					if (verified) {
						notifySuccess();
					}
					else {
						// Record verification failed and MRT was aborted.
						notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, VerifyFail));
					}
				}

				@Override
				public void onFailure(AerospikeException ae) {
					String msg = verified?  CloseFail : VerifyCloseFail;
					notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, msg, ae));
				}
			};

			AsyncDelete command = new AsyncDelete(cluster, deleteListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			String msg = verified?  CloseFail : VerifyCloseFail;
			notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, msg, t));
		}
	}

	private void notifySuccess() {
		tran.close();

		try {
			if (commitListener != null) {
				commitListener.onSuccess();
			}
			else {
				abortListener.onSuccess();
			}
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyFailure(AerospikeException ae) {
		try {
			if (commitListener != null) {
				if (verifyException != null) {
					ae.addSuppressed(verifyException);
				}

				if (rollRecords == null) {
					rollRecords = new BatchRecord[0];
				}

				commitListener.onFailure(verifyRecords, rollRecords, ae);
			}
			else {
				abortListener.onFailure(rollRecords, ae);
			}
		}
		catch (Throwable t) {
			Log.error("Tran listener onFailure() failed: " + Util.getStackTrace(t));
		}
	}
}
