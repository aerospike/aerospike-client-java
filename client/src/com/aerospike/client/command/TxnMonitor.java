/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Txn;
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

public final class TxnMonitor {
	private static final ListPolicy OrderedListPolicy = new ListPolicy(ListOrder.ORDERED,
		ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

	private static final String BinNameId = "id";
	private static final String BinNameDigests = "keyds";

	public static void addKey(Cluster cluster, WritePolicy policy, Key cmdKey) {
		Txn txn = policy.txn;

		if (txn.getWrites().contains(cmdKey)) {
			// Transaction monitor already contains this key.
			return;
		}

		if (! txn.monitorExists()) {
			// Ensure monitor record create occurs exactly once.
			synchronized(txn) {
				Operation[] ops = getOps(txn, cmdKey);
				addWriteKeys(cluster, policy, ops);
			}
		}
		else {
			Operation[] ops = getUpdateOps(txn, cmdKey);
			addWriteKeys(cluster, policy, ops);
		}
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, Key[] keys) {
		Txn txn = policy.txn;

		if (! txn.monitorExists()) {
			// Ensure monitor record create occurs exactly once.
			synchronized(txn) {
				ArrayList<Value> digests = prepareDigests(txn, keys);
				Operation[] ops = getOps(txn, digests);
				addWriteKeys(cluster, policy, ops);
			}
		}
		else {
			ArrayList<Value> digests = prepareDigests(txn, keys);
			Operation[] ops = getUpdateOps(digests);
			addWriteKeys(cluster, policy, ops);
		}
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, List<BatchRecord> records) {
		Txn txn = policy.txn;

		if (! txn.monitorExists()) {
			// Ensure monitor record create occurs exactly once.
			synchronized(txn) {
				ArrayList<Value> digests = prepareDigests(txn, records);

				if (digests.size() > 0) {
					Operation[] ops = getOps(txn, digests);
					addWriteKeys(cluster, policy, ops);
				}
			}
		}
		else {
			ArrayList<Value> digests = prepareDigests(txn, records);

			if (digests.size() > 0) {
				Operation[] ops = getUpdateOps(digests);
				addWriteKeys(cluster, policy, ops);
			}
		}
	}

	private static ArrayList<Value> prepareDigests(Txn txn, Key[] keys) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			txn.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return list;
	}

	private static ArrayList<Value> prepareDigests(Txn txn, List<BatchRecord> records) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			txn.setNamespace(br.key.namespace);

			if (br.hasWrite) {
				list.add(Value.get(br.key.digest));
			}
		}
		return list;
	}

	public static Operation[] getOps(Txn txn, Key cmdKey) {
		if (! txn.monitorExists()) {
			return getCreateOps(txn, cmdKey);
		}
		else {
			return getUpdateOps(txn, cmdKey);
		}
	}

	public static Operation[] getCreateOps(Txn txn, Key cmdKey) {
		txn.verifyCommand();
		txn.setNamespace(cmdKey.namespace);

		return new Operation[] {
			Operation.put(new Bin(BinNameId, txn.getId())),
			ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
		};
	}

	public static Operation[] getUpdateOps(Txn txn, Key cmdKey) {
		txn.verifyCommand();
		txn.setNamespace(cmdKey.namespace);

		return new Operation[] {
			ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
		};
	}

	private static Operation[] getOps(Txn txn, ArrayList<Value> digests) {
		if (! txn.monitorExists()) {
			return getCreateOps(txn, digests);
		}
		else {
			return getUpdateOps(digests);
		}
	}

	private static Operation[] getCreateOps(Txn txn, List<Value> digests) {
		return new Operation[] {
			Operation.put(new Bin(BinNameId, txn.getId())),
			ListOperation.appendItems(OrderedListPolicy, BinNameDigests, digests)
		};
	}

	private static Operation[] getUpdateOps(List<Value> digests) {
		return new Operation[] {
			ListOperation.appendItems(OrderedListPolicy, BinNameDigests, digests)
		};
	}

	private static void addWriteKeys(Cluster cluster, Policy policy, Operation[] ops) {
		Txn txn = policy.txn;
		Key txnKey = getTxnMonitorKey(txn);
		WritePolicy wp = copyTimeoutPolicy(policy);
		OperateArgs args = new OperateArgs(wp, null, null, ops);
		TxnAddKeys cmd = new TxnAddKeys(cluster, txnKey, args, txn);
		cmd.execute();
	}

	public static Key getTxnMonitorKey(Txn txn) {
		return new Key(txn.getNamespace(), "<ERO~MRT", txn.getId());
	}

	public static WritePolicy copyTimeoutPolicy(Policy policy) {
		// Inherit some fields from the original command's policy.
		WritePolicy wp = new WritePolicy();
		wp.connectTimeout = policy.connectTimeout;
		wp.socketTimeout = policy.socketTimeout;
		wp.totalTimeout = policy.totalTimeout;
		wp.timeoutDelay = policy.timeoutDelay;
		wp.maxRetries = policy.maxRetries;
		wp.sleepBetweenRetries = policy.sleepBetweenRetries;
		wp.compress = policy.compress;
		wp.respondAllOps = true;

		// Note that the server only accepts the timeout on MRT monitor record create.
		// The server ignores the MRT timeout field on successive MRT monitor record
		// updates.
		wp.expiration = policy.txn.getTimeout();
		return wp;
	}

	public static void runTest(Cluster cluster) {
		Txn txn = new Txn();
		txn.setNamespace("test");

		Key key = new Key("test", "set", 1);
		ArrayList<Value> digests = new ArrayList<Value>();
		digests.add(Value.get(key.digest));
		Operation[] ops = getCreateOps(txn, digests);

		Key key2 = new Key("test", "set", 2);
		ArrayList<Value> digests2 = new ArrayList<Value>();
		digests2.add(Value.get(key2.digest));
		Operation[] ops2 = getCreateOps(txn, digests2);

		Policy p = new Policy();
		p.txn = txn;

		addWriteKeys(cluster, p, ops);
		txn.setDeadline(0);
		addWriteKeys(cluster, p, ops2);
	}
}
