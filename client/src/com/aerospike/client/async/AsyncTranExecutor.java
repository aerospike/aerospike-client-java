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
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Tran;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.TranCommitListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.util.Util;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AsyncTranExecutor {
	private final Cluster cluster;
	private final EventLoop eventLoop;
	private final TranCommitListener listener;
	private final BatchPolicy verifyPolicy;
	private final BatchPolicy rollPolicy;
	private final Tran tran;

	public AsyncTranExecutor(
		Cluster cluster,
		EventLoop eventLoop,
		TranCommitListener listener,
		BatchPolicy verifyPolicy,
		BatchPolicy rollPolicy,
		Tran tran
	) {
		this.cluster = cluster;
		this.eventLoop = eventLoop;
		this.listener = listener;
		this.verifyPolicy = verifyPolicy;
		this.rollPolicy = rollPolicy;
		this.tran = tran;
	}

	public void commit() {
		BatchRecordArrayListener verifyListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				if (status) {
					try {
						rollForward();
					}
					catch (AerospikeException ae) {
						notifyCommitFailure(new BatchRecord[0], ae);
					}
					catch (Throwable t) {
						notifyCommitFailure(new BatchRecord[0],
							new AerospikeException(ResultCode.TRAN_FAILED, "Tran commit failed", t));
					}
				}
				else {
					notifyVerifyFailure(records,
						new AerospikeException(ResultCode.TRAN_FAILED, "Tran verify failed"));

					try {
						rollBack();
					}
					catch (AerospikeException ae) {
						notifyAbortFailure(new BatchRecord[0], ae);
					}
					catch (Throwable t) {
						notifyAbortFailure(new BatchRecord[0],
							new AerospikeException(ResultCode.TRAN_FAILED, "Tran abort failed", t));
					}
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				notifyVerifyFailure(records, ae);

				try {
					rollBack();
				}
				catch (Throwable t) {
					ae.addSuppressed(t);
					notifyAbortFailure(new BatchRecord[0], ae);
				}
			}
		};

		verify(verifyListener);
	}

	public void abort(BatchRecordArrayListener rollListener) {
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

	private void rollForward() {
		BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				if (status) {
					notifySuccess();
				}
				else {
					notifyCommitFailure(records, new AerospikeException(ResultCode.TRAN_FAILED, "Tran commit failed"));
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				notifyCommitFailure(records, ae);
			}
		};

		roll(rollListener, Command.INFO4_MRT_ROLL_FORWARD);
	}

	private void rollBack() {
		BatchRecordArrayListener rollListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				if (status) {
					notifyAbort();
				}
				else {
					notifyAbortFailure(records, new AerospikeException(ResultCode.TRAN_FAILED, "Tran abort failed"));
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				notifyAbortFailure(records, ae);
			}
		};

		roll(rollListener, Command.INFO4_MRT_ROLL_BACK);
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

		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, true, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

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

	private void notifySuccess() {
		try {
			listener.onSuccess();
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onSuccess() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyCommitFailure(BatchRecord[] records, AerospikeException ae) {
		try {
			listener.onCommitFailure(records, ae);
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onCommitFailure() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyVerifyFailure(BatchRecord[] records, AerospikeException ae) {
		try {
			listener.onVerifyFailure(records, ae);
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onVerifyFailure() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyAbort() {
		try {
			listener.onAbort();
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onAbort() failed: " + Util.getStackTrace(t));
		}
	}

	private void notifyAbortFailure(BatchRecord[] records, AerospikeException ae) {
		try {
			listener.onAbortFailure(records, ae);
		}
		catch (Throwable t) {
			Log.error("TranCommitListener onAbortFailure() failed: " + Util.getStackTrace(t));
		}
	}
}
