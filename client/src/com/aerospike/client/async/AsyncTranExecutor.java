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
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.util.Util;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AsyncTranExecutor {
	public static void commit(
		Cluster cluster,
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy verifyPolicy,
		BatchPolicy rollPolicy,
		Tran tran
	){
		BatchRecordArrayListener verifyListener = new BatchRecordArrayListener() {
			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				if (status) {
					System.out.println("Tran version matched: " + tran.id);
					roll(cluster, eventLoop, listener, rollPolicy, tran, Command.INFO4_MRT_ROLL_FORWARD);
				} else {
					try {
						abort(cluster, eventLoop, listener, rollPolicy, tran, records, null);
					} catch (AerospikeException ae) {
						tranNotifyFailure(listener, records, ae);
					} catch (Throwable t) {
						tranNotifyFailure(listener, records, new AerospikeException(t));
					}
				}
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException ae) {
				try {
					abort(cluster, eventLoop, listener, rollPolicy, tran, records, ae);
				} catch (Throwable t) {
					ae.addSuppressed(t);
					tranNotifyFailure(listener, records, ae);
				}
			}
		};

		verify(cluster, eventLoop, verifyListener, verifyPolicy, tran);
	}

	public static void abort(
		Cluster cluster,
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy policy,
		Tran tran,
		BatchRecord[] recordsVerify,
		AerospikeException aeVerify
	) {
		BatchRecordArrayListener abortListener = new BatchRecordArrayListener() {
			private static final String verifyFailed = "Batch record verification failed. Tran aborted: ";
			private static final String verifyAbortFailed = "Batch record verification and tran abort failed: ";

			@Override
			public void onSuccess(BatchRecord[] records, boolean status) {
				AerospikeException ae;

				if (status) {
					if (aeVerify == null) {
						ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyFailed + tran.id);
					}
					else {
						ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyFailed + tran.id, aeVerify);
					}
				}
				else {
					if (aeVerify == null) {
						ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyAbortFailed + tran.id);
					}
					else {
						ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyAbortFailed + tran.id, aeVerify);
					}
				}
				tranNotifyFailure(listener, recordsVerify, ae);
			}

			@Override
			public void onFailure(BatchRecord[] records, AerospikeException aeAbort) {
				AerospikeException ae;

				if (aeVerify == null) {
					ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyAbortFailed + tran.id, aeAbort);
				}
				else {
					aeVerify.addSuppressed(aeAbort);
					ae = new AerospikeException(ResultCode.TRAN_FAILED, verifyAbortFailed + tran.id, aeVerify);
				}
				tranNotifyFailure(listener, recordsVerify, ae);
			}
		};

		roll(cluster, eventLoop, abortListener, policy, tran, Command.INFO4_MRT_ROLL_BACK);
	}

	private static void tranNotifyFailure(BatchRecordArrayListener listener, BatchRecord[] records, AerospikeException ae) {
		try {
			listener.onFailure(records, ae);
		}
		catch (Throwable t) {
			Log.error("Tran onFailure() failed: " + Util.getStackTrace(t));
		}
	}

	private static void verify(
		Cluster cluster,
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy policy,
		Tran tran
	) {
		// Validate record versions in a batch.
		Set<Map.Entry<Key,Long>> reads = tran.getReads();
		int max = reads.size();

		if (max <= 0) {
			listener.onSuccess(new BatchRecord[0], true);
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
			eventLoop, cluster, listener, records);

		List<BatchNode> bns = BatchNodeList.generate(cluster, policy, keys, records, false, executor);
		AsyncCommand[] commands = new AsyncCommand[bns.size()];

		count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new AsyncBatchSingle.TranVerify(
					executor, cluster, policy, versions[i], records[i], bn.node);
			}
			else {
				commands[count++] = new AsyncBatch.TranVerify(
					executor, bn, policy, keys, versions, records);
			}
		}
		executor.execute(commands);
	}

	public static void roll(
		Cluster cluster,
		EventLoop eventLoop,
		BatchRecordArrayListener listener,
		BatchPolicy policy,
		Tran tran,
		int tranAttr
	) {
		Set<Key> keySet = tran.getWrites();

		if (keySet.isEmpty()) {
			listener.onSuccess(new BatchRecord[0], true);
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		// Copy tran roll policy because it needs to be modified.
		BatchPolicy batchPolicy = new BatchPolicy(policy);

		BatchAttr attr = new BatchAttr();
		attr.setTran(tranAttr);

		AsyncBatchExecutor.BatchRecordArray executor = new AsyncBatchExecutor.BatchRecordArray(
			eventLoop, cluster, listener, records);

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
}
