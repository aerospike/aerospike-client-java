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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Tran;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TranMonitor {
	private static final ListPolicy OrderedListPolicy = new ListPolicy(ListOrder.ORDERED,
		ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

	public static void addKey(Cluster cluster, WritePolicy policy, Key cmdKey) {
		Tran tran = policy.tran;

		if (tran.getWrites().contains(cmdKey)) {
			// Transaction monitor already contains this key.
			return;
		}

		tran.setNamespace(cmdKey.namespace);

		Operation[] ops = getTranOps(tran, cmdKey);
		addWriteKeys(cluster, tran, policy, ops);
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, Key[] keys) {
		Tran tran = policy.tran;
		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			tran.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}

		Operation[] ops = getTranOps(tran, list);
		addWriteKeys(cluster, tran, policy, ops);
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, List<BatchRecord> records) {
		Tran tran = policy.tran;
		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			if (br.hasWrite) {
				Key key = br.key;
				tran.setNamespace(key.namespace);
				list.add(Value.get(key.digest));
			}
		}

		Operation[] ops = getTranOps(tran, list);
		addWriteKeys(cluster, tran, policy, ops);
	}

	public static Operation[] getTranOps(Tran tran, Key cmdKey) {
		return new Operation[]{
			Operation.put(new Bin("id", tran.getId())),
			ListOperation.append(OrderedListPolicy, "keyds", Value.get(cmdKey.digest))
		};
	}

	public static Operation[] getTranOps(Tran tran, ArrayList<Value> list) {
		return new Operation[] {
			Operation.put(new Bin("id", tran.getId())),
			ListOperation.appendItems(OrderedListPolicy, "keyds", list)
		};
	}

	private static void addWriteKeys(Cluster cluster, Tran tran, Policy policy, Operation[] ops) {
		Key tranKey = getTranMonitorKey(tran);
		WritePolicy wp = copyTimeoutPolicy(policy);
		OperateArgs args = new OperateArgs(wp, null, null, ops);
		TranAddKeys cmd = new TranAddKeys(cluster, tranKey, args);
		cmd.execute();
	}

	public static WritePolicy copyTimeoutPolicy(Policy policy) {
		// Inherit some fields from the original command's policy.
		WritePolicy wp = new WritePolicy();
		wp.tran = policy.tran;
		wp.connectTimeout = policy.connectTimeout;
		wp.socketTimeout = policy.socketTimeout;
		wp.totalTimeout = policy.totalTimeout;
		wp.timeoutDelay = policy.timeoutDelay;
		wp.maxRetries = policy.maxRetries;
		wp.sleepBetweenRetries = policy.sleepBetweenRetries;
		wp.compress = policy.compress;
		wp.respondAllOps = true;
		return wp;
	}

	public static void commit(Cluster cluster, Tran tran, BatchPolicy verifyPolicy, BatchPolicy rollPolicy) {
		// Verify read versions in batch.
		verify(cluster, tran, verifyPolicy, rollPolicy);

		WritePolicy writePolicy = new WritePolicy(rollPolicy);
		Key tranKey = getTranMonitorKey(tran);
		Set<Key> keySet = tran.getWrites();

		if (keySet.isEmpty()) {
			// There is nothing to roll-forward. Remove MRT monitor if it exists.
			if (tran.getNamespace() != null) {
				close(cluster, tran, writePolicy, tranKey);
			}
			return;
		}

		// Tell MRT monitor that a roll-forward will commence.
		willRollForward(cluster, writePolicy, tranKey);

		// Roll-forward writes in batch.
		roll(cluster, tran, rollPolicy, Command.INFO4_MRT_ROLL_FORWARD);

		// Remove MRT monitor.
		close(cluster, tran, writePolicy, tranKey);
	}

	public static void abort(Cluster cluster, Tran tran, BatchPolicy rollPolicy) {
		roll(cluster, tran, rollPolicy, Command.INFO4_MRT_ROLL_BACK);

		WritePolicy writePolicy = new WritePolicy(rollPolicy);
		Key tranKey = getTranMonitorKey(tran);
		close(cluster, tran, writePolicy, tranKey);
	}

	public static Key getTranMonitorKey(Tran tran) {
		return new Key(tran.getNamespace(), "AE", tran.getId());
	}

	private static void verify(Cluster cluster, Tran tran, BatchPolicy verifyPolicy, BatchPolicy rollPolicy) {
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

		try {
			BatchStatus status = new BatchStatus(true);
			List<BatchNode> bns = BatchNodeList.generate(cluster, verifyPolicy, keys, records, false, status);
			IBatchCommand[] commands = new IBatchCommand[bns.size()];

			count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.TranVerify(
						cluster, verifyPolicy, versions[i], records[i], status, bn.node);
				}
				else {
					commands[count++] = new Batch.TranVerify(
						cluster, bn, verifyPolicy, keys, versions, records, status);
				}
			}

			BatchExecutor.execute(cluster, verifyPolicy, commands, status);

			if (!status.getStatus()) {
				throw new Exception("Failed to verify one or more record versions");
			}
		} catch (Throwable e) {
			// Read verification failed. Abort transaction.
			try {
				roll(cluster, tran, rollPolicy, Command.INFO4_MRT_ROLL_BACK);
			}
			catch (Throwable e2) {
				// Throw combination of tranAbort and original exception.
				e.addSuppressed(e2);
				throw new AerospikeException.BatchRecordArray(records,
					"Batch record verification and tran abort failed: " + tran.getId(), e);
			}

			// Throw original exception when abort succeeds.
			throw new AerospikeException.BatchRecordArray(records,
				"Batch record verification failed. Tran aborted: " + tran.getId(), e);
		}
	}

	private static void willRollForward(Cluster cluster, WritePolicy writePolicy, Key tranKey) {
		// Tell MRT monitor that a roll-forward will commence.
		Bin[] bins = new Bin[] {new Bin("fwd", true)};
		WriteCommand cmd = new WriteCommand(cluster, writePolicy, tranKey, bins, Operation.Type.WRITE);
		cmd.execute();
	}

	private static void roll(Cluster cluster, Tran tran, BatchPolicy rollPolicy, int tranAttr) {
		Set<Key> keySet = tran.getWrites();

		if (keySet.isEmpty()) {
			return;
		}

		Key[] keys = keySet.toArray(new Key[keySet.size()]);
		BatchRecord[] records = new BatchRecord[keys.length];

		for (int i = 0; i < keys.length; i++) {
			records[i] = new BatchRecord(keys[i], true);
		}

		try {
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
				throw new Exception("Failed to " + rollString + " one or more records");
			}
		}
		catch (Throwable e) {
			String rollString = tranAttr == Command.INFO4_MRT_ROLL_FORWARD? "commit" : "abort";
			throw new AerospikeException.BatchRecordArray(records,
				"Tran " + rollString + " failed: " + tran.getId(), e);
		}
	}

	private static void close(Cluster cluster, Tran tran, WritePolicy writePolicy, Key tranKey) {
		// Delete MRT monitor on server.
		DeleteCommand cmd = new DeleteCommand(cluster, writePolicy, tranKey);
		cmd.execute();

		// Reset MRT on client.
		tran.close();
	}
}
