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

import com.aerospike.client.AbortStatus;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Txn;
import com.aerospike.client.CommitError;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.TxnMonitor;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.AbortListener;
import com.aerospike.client.listener.CommitListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AsyncTxnRoll {
	private final Cluster cluster;
	private final EventLoop eventLoop;
	private final BatchPolicy verifyPolicy;
	private final BatchPolicy rollPolicy;
	private final WritePolicy writePolicy;
	private final Txn txn;
	private final Key tranKey;
	private CommitListener commitListener;
	private AbortListener abortListener;
	private BatchRecord[] verifyRecords;
	private BatchRecord[] rollRecords;
	private AerospikeException verifyException;

	public AsyncTxnRoll(
		Cluster cluster,
		EventLoop eventLoop,
		BatchPolicy verifyPolicy,
		BatchPolicy rollPolicy,
		Txn txn
	) {
		this.cluster = cluster;
		this.eventLoop = eventLoop;
		this.verifyPolicy = verifyPolicy;
		this.rollPolicy = rollPolicy;
		this.writePolicy = new WritePolicy(rollPolicy);
		this.txn = txn;
		this.tranKey = TxnMonitor.getTxnMonitorKey(txn);
	}

	public void commit(CommitListener listener) {
		commitListener = listener;

		BatchRecordArrayListener verifyListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				verifyRecords = records;

				if (status) {
					if (txn.monitorExists()) {
						markRollForward();
					}
					else {
						// There is nothing to roll-forward.
						closeOnCommit(true);
					}
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

	public void abort(AbortListener listener) {
		abortListener = listener;

		BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				rollRecords = records;

				if (status) {
					closeOnAbort();
				}
				else {
					notifyAbortSuccess(AbortStatus.ROLL_BACK_ABANDONED);
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				rollRecords = records;
				notifyAbortSuccess(AbortStatus.ROLL_BACK_ABANDONED);
			}
		};

		roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
	}

	private void verify(BatchRecordArrayListener verifyListener) {
		// Validate record versions in a batch.
		Set<Map.Entry<Key,Long>> reads = txn.getReads();
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
				commands[count++] = new AsyncBatchSingle.TxnVerify(
					executor, cluster, verifyPolicy, txn, versions[i], records[i], bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.TxnVerify(
					executor, bn, verifyPolicy, txn, keys, versions, records);
			}
		}
		executor.execute(commands);
	}

	private void markRollForward() {
		// Tell MRT monitor that a roll-forward will commence.
		try {
			WriteListener writeListener = new WriteListener() {
				@Override
				public void onSuccess(Key key) {
					rollForward();
				}

				@Override
				public void onFailure(AerospikeException ae) {
					notifyCommitFailure(CommitError.MARK_ROLL_FORWARD_ABANDONED, ae, true);
				}
			};

			AsyncTxnMarkRollForward command = new AsyncTxnMarkRollForward(cluster, txn, writeListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			notifyCommitFailure(CommitError.MARK_ROLL_FORWARD_ABANDONED, t, false);
		}
	}

	private void rollForward() {
		try {
			BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
				@Override
				public void onSuccess(BatchRecord[] records, boolean status) {
					rollRecords = records;

					if (status) {
						closeOnCommit(true);
					}
					else {
						notifyCommitSuccess(CommitStatus.ROLL_FORWARD_ABANDONED);
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyCommitSuccess(CommitStatus.ROLL_FORWARD_ABANDONED);
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_FORWARD);
		}
		catch (Throwable t) {
			notifyCommitSuccess(CommitStatus.ROLL_FORWARD_ABANDONED);
		}
	}

	private void rollBack() {
		try {
			BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
				@Override
				public void onSuccess(BatchRecord[] records, boolean status) {
					rollRecords = records;

					if (status) {
						closeOnCommit(false);
					}
					else {
						notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, null, false);
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, ae, false);
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
		}
		catch (Throwable t) {
			notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, t, false);
		}
	}

	private void roll(BatchRecordArrayListener rollListener, int txnAttr) {
		Set<Key> keySet = txn.getWrites();

		if (keySet.isEmpty()) {
			rollListener.onSuccess(new BatchRecord[0], true);
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		// Copy transaction roll policy because it needs to be modified.
		BatchPolicy batchPolicy = new BatchPolicy(rollPolicy);

		BatchAttr attr = new BatchAttr();
		attr.setTxn(txnAttr);

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, rollListener, records);

		// generate() requires a null transaction instance.
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, true, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		// Batch roll forward requires the transaction instance.
		batchPolicy.txn = txn;

		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.TxnRoll(
					executor, cluster, batchPolicy, records[i], bn.node, txnAttr);
			}
			else {
				commands[count++] = new AsyncBatch.TxnRoll(
					executor, bn, batchPolicy, keys, records, attr);
			}
		}
		executor.execute(commands);
	}

	private void closeOnCommit(boolean verified) {
		if (! txn.monitorMightExist()) {
			// There is no MRT monitor to remove.
			if (verified) {
				notifyCommitSuccess(CommitStatus.OK);
			}
			else {
				// Record verification failed and MRT was aborted.
				notifyCommitFailure(CommitError.VERIFY_FAIL, null, false);
			}
			return;
		}

		try {
			DeleteListener deleteListener = new DeleteListener() {
				@Override
				public void onSuccess(Key key, boolean existed) {
					if (verified) {
						notifyCommitSuccess(CommitStatus.OK);
					}
					else {
						// Record verification failed and MRT was aborted.
						notifyCommitFailure(CommitError.VERIFY_FAIL, null, false);
					}
				}

				@Override
				public void onFailure(AerospikeException ae) {
					if (verified) {
						notifyCommitSuccess(CommitStatus.CLOSE_ABANDONED);
					}
					else {
						notifyCommitFailure(CommitError.VERIFY_FAIL_CLOSE_ABANDONED, ae, false);
						
					}
				}
			};

			AsyncTxnClose command = new AsyncTxnClose(cluster, txn, deleteListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			if (verified) {
				notifyCommitSuccess(CommitStatus.CLOSE_ABANDONED);
			}
			else {
				notifyCommitFailure(CommitError.VERIFY_FAIL_CLOSE_ABANDONED, t, false);
			}
		}
	}

	private void closeOnAbort() {
		if (! txn.monitorMightExist()) {
			// There is no MRT monitor record to remove.
			notifyAbortSuccess(AbortStatus.OK);
			return;
		}

		try {
			DeleteListener deleteListener = new DeleteListener() {
				@Override
				public void onSuccess(Key key, boolean existed) {
					notifyAbortSuccess(AbortStatus.OK);
				}

				@Override
				public void onFailure(AerospikeException ae) {
					notifyAbortSuccess(AbortStatus.CLOSE_ABANDONED);
				}
			};

			AsyncTxnClose command = new AsyncTxnClose(cluster, txn, deleteListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			notifyAbortSuccess(AbortStatus.CLOSE_ABANDONED);
		}
	}

	private void notifyCommitSuccess(CommitStatus status) {
		txn.clear();

		try {
			commitListener.onSuccess(status);
		}
		catch (Throwable t) {
			Log.error("CommitListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyCommitFailure(CommitError error, Throwable cause, boolean setInDoubt) {
		try {
			AerospikeException.Commit aec = (cause == null) ?
				new AerospikeException.Commit(error, verifyRecords, rollRecords) :
				new AerospikeException.Commit(error, verifyRecords, rollRecords, cause);

			if (verifyException != null) {
				aec.addSuppressed(verifyException);
			}

			if (cause instanceof AerospikeException) {
				AerospikeException src = (AerospikeException)cause;
				aec.setNode(src.getNode());
				aec.setPolicy(src.getPolicy());
				aec.setIteration(src.getIteration());
				
				if (setInDoubt) {
					aec.setInDoubt(src.getInDoubt());
				}
			}

			commitListener.onFailure(aec);
		}
		catch (Throwable t) {
			Log.error("CommitListener onFailure() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyAbortSuccess(AbortStatus status) {
		txn.clear();

		try {
			abortListener.onSuccess(status);
		}
		catch (Throwable t) {
			Log.error("AbortListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}
}
