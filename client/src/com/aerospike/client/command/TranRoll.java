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
package com.aerospike.client.command;

import com.aerospike.client.AbortError;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Tran;
import com.aerospike.client.CommitError;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TranRoll {
	private final Cluster cluster;
	private final Tran tran;
	private BatchRecord[] verifyRecords;
	private BatchRecord[] rollRecords;

	public TranRoll(Cluster cluster, Tran tran) {
		this.cluster = cluster;
		this.tran = tran;
	}

	public void commit(BatchPolicy verifyPolicy, BatchPolicy rollPolicy) {
		try {
			// Verify read versions in batch.
			verify(verifyPolicy);
		}
		catch (Throwable t) {
			// Verify failed. Abort.
			try {
				roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Throwable t2) {
				// Throw combination of verify and roll exceptions.
				t.addSuppressed(t2);
				throw new AerospikeException.Commit(CommitError.VERIFY_FAIL_ABORT_ABANDONED, verifyRecords, rollRecords, t);
			}

			if (tran.getDeadline() != 0) {
				try {
					WritePolicy writePolicy = new WritePolicy(rollPolicy);
					Key tranKey = TranMonitor.getTranMonitorKey(tran);
					close(writePolicy, tranKey);
				}
				catch (Throwable t3) {
					// Throw combination of verify and close exceptions.
					t.addSuppressed(t3);
					throw new AerospikeException.Commit(CommitError.VERIFY_FAIL_CLOSE_ABANDONED, verifyRecords, rollRecords, t);
				}
			}

			// Throw original exception when abort succeeds.
			throw new AerospikeException.Commit(CommitError.VERIFY_FAIL, verifyRecords, rollRecords, t);
		}

		WritePolicy writePolicy = new WritePolicy(rollPolicy);
		Key tranKey = TranMonitor.getTranMonitorKey(tran);
		Set<Key> keySet = tran.getWrites();

		if (!keySet.isEmpty()) {
			// Tell MRT monitor that a roll-forward will commence.
			try {
				markRollForward(writePolicy, tranKey);
			}
			catch (Throwable t) {
				throw new AerospikeException.Commit(CommitError.MARK_ROLL_FORWARD_ABANDONED, verifyRecords, rollRecords, t);
			}

			// Roll-forward writes in batch.
			try {
				roll(rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);
			}
			catch (Throwable t) {
				throw new AerospikeException.Commit(CommitError.ROLL_FORWARD_ABANDONED, verifyRecords, rollRecords, t);
			}
		}

		if (tran.getDeadline() != 0) {
			// Remove MRT monitor.
			try {
				close(writePolicy, tranKey);
			}
			catch (Throwable t) {
				throw new AerospikeException.Commit(CommitError.CLOSE_ABANDONED, verifyRecords, rollRecords, t);
			}
		}
	}

	public void abort(BatchPolicy rollPolicy) {
		Set<Key> keySet = tran.getWrites();

		if (! keySet.isEmpty()) {
			try {
				roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Throwable t) {
				throw new AerospikeException.Abort(AbortError.ROLL_BACK_ABANDONED, rollRecords, t);
			}
		}

		if (tran.getDeadline() != 0) {
			try {
				WritePolicy writePolicy = new WritePolicy(rollPolicy);
				Key tranKey = TranMonitor.getTranMonitorKey(tran);
				close(writePolicy, tranKey);
			}
			catch (Throwable t) {
				throw new AerospikeException.Abort(AbortError.CLOSE_ABANDONED, rollRecords, t);
			}
		}
	}

	private void verify(BatchPolicy verifyPolicy) {
		// Validate record versions in a batch.
		Set<Map.Entry<Key, Long>> reads = tran.getReads();
		int max = reads.size();

		if (max == 0) {
			return;
		}

		BatchRecord[] records = new BatchRecord[max];
		Key[] keys = new Key[max];
		Long[] versions = new Long[max];
		int count = 0;

		for (Map.Entry<Key, Long> entry : reads) {
			Key key = entry.getKey();
			keys[count] = key;
			records[count] = new BatchRecord(key, false);
			versions[count] = entry.getValue();
			count++;
		}

		this.verifyRecords = records;

		BatchStatus status = new BatchStatus(true);
		List<BatchNode> bns = BatchNodeList.generate(cluster, verifyPolicy, keys, records, false, status);
		IBatchCommand[] commands = new IBatchCommand[bns.size()];

		count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new BatchSingle.TranVerify(
					cluster, verifyPolicy, tran, versions[i], records[i], status, bn.node);
			}
			else {
				commands[count++] = new Batch.TranVerify(
					cluster, bn, verifyPolicy, tran, keys, versions, records, status);
			}
		}

		BatchExecutor.execute(cluster, verifyPolicy, commands, status);

		if (!status.getStatus()) {
			throw new RuntimeException("Failed to verify one or more record versions");
		}
	}

	private void markRollForward(WritePolicy writePolicy, Key tranKey) {
		// Tell MRT monitor that a roll-forward will commence.
		TranMarkRollForward cmd = new TranMarkRollForward(cluster, tran, writePolicy, tranKey);
		cmd.execute();
	}

	private void roll(BatchPolicy rollPolicy, int tranAttr) {
		Set<Key> keySet = tran.getWrites();

		if (keySet.isEmpty()) {
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		this.rollRecords = records;

		// Copy tran roll policy because it needs to be modified.
		BatchPolicy batchPolicy = new BatchPolicy(rollPolicy);

		BatchAttr attr = new BatchAttr();
		attr.setTran(tranAttr);

		BatchStatus status = new BatchStatus(true);

		// generate() requires a null tran instance.
		List<BatchNode> bns = BatchNodeList.generate(cluster, batchPolicy, keys, records, true, status);
		IBatchCommand[] commands = new IBatchCommand[bns.size()];

		// Batch roll forward requires the tran instance.
		batchPolicy.tran = tran;

		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new BatchSingle.TranRoll(
					cluster, batchPolicy, records[i], status, bn.node, tranAttr);
			}
			else {
				commands[count++] = new Batch.TranRoll(
					cluster, bn, batchPolicy, keys, records, attr, status);
			}
		}
		BatchExecutor.execute(cluster, batchPolicy, commands, status);

		if (!status.getStatus()) {
			String rollString = tranAttr == Command.INFO4_MRT_ROLL_FORWARD? "commit" : "abort";
			throw new RuntimeException("Failed to " + rollString + " one or more records");
		}
	}

	private void close(WritePolicy writePolicy, Key tranKey) {
		// Delete MRT monitor on server.
		TranClose cmd = new TranClose(cluster, tran, writePolicy, tranKey);
		cmd.execute();

		// Reset MRT on client.
		tran.close();
	}
}
