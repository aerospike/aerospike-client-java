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

import com.aerospike.client.AbortStatus;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Txn;
import com.aerospike.client.CommitError;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TxnRoll {
	private final Cluster cluster;
	private final Txn txn;
	private BatchRecord[] verifyRecords;
	private BatchRecord[] rollRecords;

	public TxnRoll(Cluster cluster, Txn txn) {
		this.cluster = cluster;
		this.txn = txn;
	}

	public void verify(BatchPolicy verifyPolicy, BatchPolicy rollPolicy) {
		try {
			// Verify read versions in batch.
			verifyRecordVersions(verifyPolicy);
		}
		catch (Throwable t) {
			// Verify failed. Abort.
			txn.setState(Txn.State.ABORTED);

			try {
				roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Throwable t2) {
				// Throw combination of verify and roll exceptions.
				t.addSuppressed(t2);
				throw onCommitError(CommitError.VERIFY_FAIL_ABORT_ABANDONED, t, false);
			}

			if (txn.monitorMightExist()) {
				try {
					WritePolicy writePolicy = new WritePolicy(rollPolicy);
					Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
					close(writePolicy, txnKey);
				}
				catch (Throwable t3) {
					// Throw combination of verify and close exceptions.
					t.addSuppressed(t3);
					throw onCommitError(CommitError.VERIFY_FAIL_CLOSE_ABANDONED, t, false);
				}
			}

			// Throw original exception when abort succeeds.
			throw onCommitError(CommitError.VERIFY_FAIL, t, false);
		}
		
		txn.setState(Txn.State.VERIFIED);
	}
	
	public CommitStatus commit(BatchPolicy rollPolicy) {
		WritePolicy writePolicy = new WritePolicy(rollPolicy);
		Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
		
		if (txn.monitorExists()) {
			// Tell MRT monitor that a roll-forward will commence.
			try {
				markRollForward(writePolicy, txnKey);
			}
			catch (Throwable t) {
				throw onCommitError(CommitError.MARK_ROLL_FORWARD_ABANDONED, t, true);
			}
		}
		
		txn.setState(Txn.State.COMMITTED);

		// Roll-forward writes in batch.
		try {
			roll(rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);
		}
		catch (Throwable t) {
			return CommitStatus.ROLL_FORWARD_ABANDONED;
		}

		if (txn.monitorMightExist()) {
			// Remove MRT monitor.
			try {
				close(writePolicy, txnKey);
			}
			catch (Throwable t) {
				return CommitStatus.CLOSE_ABANDONED;
			}
		}
		return CommitStatus.OK;
	}
	
	private AerospikeException.Commit onCommitError(CommitError error, Throwable cause, boolean setInDoubt) {
		AerospikeException.Commit aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, cause);
		
		if (cause instanceof AerospikeException) {
			AerospikeException src = (AerospikeException)cause;
			aec.setNode(src.getNode());
			aec.setPolicy(src.getPolicy());
			aec.setIteration(src.getIteration());
			
			if (setInDoubt) {
				aec.setInDoubt(src.getInDoubt());
			}
		}
		return aec;
	}

	public AbortStatus abort(BatchPolicy rollPolicy) {
		txn.setState(Txn.State.ABORTED);

		try {
			roll(rollPolicy, Command.INFO4_MRT_ROLL_BACK);
		}
		catch (Throwable t) {
			return AbortStatus.ROLL_BACK_ABANDONED;
		}

		if (txn.monitorMightExist()) {
			try {
				WritePolicy writePolicy = new WritePolicy(rollPolicy);
				Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
				close(writePolicy, txnKey);
			}
			catch (Throwable t) {
				return AbortStatus.CLOSE_ABANDONED;
			}
		}
		return AbortStatus.OK;
	}

	private void verifyRecordVersions(BatchPolicy verifyPolicy) {
		// Validate record versions in a batch.
		Set<Map.Entry<Key, Long>> reads = txn.getReads();
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
				commands[count++] = new BatchSingle.TxnVerify(
					cluster, verifyPolicy, versions[i], records[i], status, bn.node);
			}
			else {
				commands[count++] = new Batch.TxnVerify(
					cluster, bn, verifyPolicy, keys, versions, records, status);
			}
		}

		BatchExecutor.execute(cluster, verifyPolicy, commands, status);

		if (!status.getStatus()) {
			throw new RuntimeException("Failed to verify one or more record versions");
		}
	}

	private void markRollForward(WritePolicy writePolicy, Key txnKey) {
		// Tell MRT monitor that a roll-forward will commence.
		TxnMarkRollForward cmd = new TxnMarkRollForward(cluster, writePolicy, txnKey);
		cmd.execute();
	}

	private void roll(BatchPolicy rollPolicy, int txnAttr) {
		Set<Key> keySet = txn.getWrites();

		if (keySet.isEmpty()) {
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		this.rollRecords = records;

		BatchAttr attr = new BatchAttr();
		attr.setTxn(txnAttr);

		BatchStatus status = new BatchStatus(true);

		List<BatchNode> bns = BatchNodeList.generate(cluster, rollPolicy, keys, records, true, status);
		IBatchCommand[] commands = new IBatchCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new BatchSingle.TxnRoll(
					cluster, rollPolicy, txn, records[i], status, bn.node, txnAttr);
			}
			else {
				commands[count++] = new Batch.TxnRoll(
					cluster, bn, rollPolicy, txn, keys, records, attr, status);
			}
		}
		BatchExecutor.execute(cluster, rollPolicy, commands, status);

		if (!status.getStatus()) {
			String rollString = txnAttr == Command.INFO4_MRT_ROLL_FORWARD? "commit" : "abort";
			throw new RuntimeException("Failed to " + rollString + " one or more records");
		}
	}

	private void close(WritePolicy writePolicy, Key txnKey) {
		// Delete MRT monitor on server.
		TxnClose cmd = new TxnClose(cluster, txn, writePolicy, txnKey);
		cmd.execute();

		// Reset MRT on client.
		txn.clear();
	}
}
