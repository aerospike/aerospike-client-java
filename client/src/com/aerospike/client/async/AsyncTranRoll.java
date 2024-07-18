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

import com.aerospike.client.AbortError;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Tran;
import com.aerospike.client.CommitError;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.TranMonitor;
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

public final class AsyncTranRoll {
	private final Cluster cluster;
	private final EventLoop eventLoop;
	private final BatchPolicy verifyPolicy;
	private final BatchPolicy rollPolicy;
	private final WritePolicy writePolicy;
	private final Tran tran;
	private final Key tranKey;
	private CommitListener commitListener;
	private AbortListener abortListener;
	private BatchRecord[] verifyRecords;
	private BatchRecord[] rollRecords;
	private AerospikeException verifyException;

	public AsyncTranRoll(
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

	public void commit(CommitListener listener) {
		commitListener = listener;

		BatchRecordArrayListener verifyListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				verifyRecords = records;

				if (status) {
					Set<Key> keySet = tran.getWrites();

					if (! keySet.isEmpty()) {
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
					notifyAbortFailure(AbortError.ROLL_BACK_ABANDONED, null);
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				rollRecords = records;
				notifyAbortFailure(AbortError.ROLL_BACK_ABANDONED, ae);
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
					executor, cluster, verifyPolicy, tran, versions[i], records[i], bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.TranVerify(
					executor, bn, verifyPolicy, tran, keys, versions, records);
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
					notifyCommitFailure(CommitError.MARK_ROLL_FORWARD_ABANDONED, ae);
				}
			};

			AsyncTranMarkRollForward command = new AsyncTranMarkRollForward(cluster, tran, writeListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			notifyCommitFailure(CommitError.MARK_ROLL_FORWARD_ABANDONED, t);
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
						notifyCommitFailure(CommitError.ROLL_FORWARD_ABANDONED, null);
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyCommitFailure(CommitError.ROLL_FORWARD_ABANDONED, ae);
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_FORWARD);
		}
		catch (Throwable t) {
			notifyCommitFailure(CommitError.ROLL_FORWARD_ABANDONED, t);
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
						notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, null);
					}
				}

				@Override
				public void onFailure(BatchRecord[] records, AerospikeException ae) {
					rollRecords = records;
					notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, ae);
				}
			};

			roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
		}
		catch (Throwable t) {
			notifyCommitFailure(CommitError.VERIFY_FAIL_ABORT_ABANDONED, t);
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

	private void closeOnCommit(boolean verified) {
		if (tran.getDeadline() == 0) {
			// There is no MRT monitor to remove.
			if (verified) {
				notifyCommitSuccess();
			}
			else {
				// Record verification failed and MRT was aborted.
				notifyCommitFailure(CommitError.VERIFY_FAIL, null);
			}
			return;
		}

		try {
			DeleteListener deleteListener = new DeleteListener() {
				@Override
				public void onSuccess(Key key, boolean existed) {
					if (verified) {
						notifyCommitSuccess();
					}
					else {
						// Record verification failed and MRT was aborted.
						notifyCommitFailure(CommitError.VERIFY_FAIL, null);
					}
				}

				@Override
				public void onFailure(AerospikeException ae) {
					CommitError error = verified?  CommitError.CLOSE_ABANDONED : CommitError.VERIFY_FAIL_CLOSE_ABANDONED;
					notifyCommitFailure(error, ae);
				}
			};

			AsyncTranClose command = new AsyncTranClose(cluster, tran, deleteListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			CommitError error = verified?  CommitError.CLOSE_ABANDONED : CommitError.VERIFY_FAIL_CLOSE_ABANDONED;
			notifyCommitFailure(error, t);
		}
	}

	private void closeOnAbort() {
		if (tran.getDeadline() == 0) {
			// There is no MRT monitor record to remove.
			notifyAbortSuccess();
			return;
		}

		try {
			DeleteListener deleteListener = new DeleteListener() {
				@Override
				public void onSuccess(Key key, boolean existed) {
					notifyAbortSuccess();
				}

				@Override
				public void onFailure(AerospikeException ae) {
					notifyAbortFailure(AbortError.CLOSE_ABANDONED, ae);
				}
			};

			AsyncTranClose command = new AsyncTranClose(cluster, tran, deleteListener, writePolicy, tranKey);
			eventLoop.execute(cluster, command);
		}
		catch (Throwable t) {
			notifyAbortFailure(AbortError.CLOSE_ABANDONED, t);
		}
	}

	private void notifyCommitSuccess() {
		tran.close();

		try {
			commitListener.onSuccess();
		}
		catch (Throwable t) {
			Log.error("CommitListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyCommitFailure(CommitError error, Throwable cause) {
		try {
			AerospikeException.Commit aet = (cause == null) ?
				new AerospikeException.Commit(error, verifyRecords, rollRecords) :
				new AerospikeException.Commit(error, verifyRecords, rollRecords, cause);

			if (verifyException != null) {
				aet.addSuppressed(verifyException);
			}

			commitListener.onFailure(aet);
		}
		catch (Throwable t) {
			Log.error("CommitListener onFailure() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyAbortSuccess() {
		tran.close();

		try {
			abortListener.onSuccess();
		}
		catch (Throwable t) {
			Log.error("AbortListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyAbortFailure(AbortError error, Throwable cause) {
		try {
			AerospikeException.Abort aet = (cause == null) ?
				new AerospikeException.Abort(error, rollRecords) :
				new AerospikeException.Abort(error, rollRecords, cause);

			abortListener.onFailure(aet);
		}
		catch (Throwable t) {
			Log.error("AbortListener onFailure() failed: " + Util.getStackTrace(t));
		}
	}
}
