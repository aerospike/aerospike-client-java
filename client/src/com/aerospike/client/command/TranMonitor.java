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

public final class TranMonitor {
	private static final ListPolicy OrderedListPolicy = new ListPolicy(ListOrder.ORDERED,
		ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

	private static final String BinNameId = "id";
	private static final String BinNameDigests = "keyds";

	public static void addKey(Cluster cluster, WritePolicy policy, Key cmdKey) {
		Tran tran = policy.tran;

		if (tran.getWrites().contains(cmdKey)) {
			// Transaction monitor already contains this key.
			return;
		}

		Operation[] ops = getTranOps(tran, cmdKey);
		addWriteKeys(cluster, policy, ops);
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, Key[] keys) {
		Operation[] ops = getTranOps(policy.tran, keys);
		addWriteKeys(cluster, policy, ops);
	}

	public static void addKeys(Cluster cluster, BatchPolicy policy, List<BatchRecord> records) {
		Operation[] ops = getTranOps(policy.tran, records);

		if (ops != null) {
			addWriteKeys(cluster, policy, ops);
		}
	}

	public static Operation[] getTranOps(Tran tran, Key cmdKey) {
		tran.setNamespace(cmdKey.namespace);

		if (tran.getDeadline() == 0) {
			// No existing monitor record.
			return new Operation[] {
				Operation.put(new Bin(BinNameId, tran.getId())),
				ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
			};
		}
		else {
			return new Operation[] {
				ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
			};
		}
	}

	public static Operation[] getTranOps(Tran tran, Key[] keys) {
		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			tran.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return getTranOps(tran, list);
	}

	public static Operation[] getTranOps(Tran tran, List<BatchRecord> records) {
		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			tran.setNamespace(br.key.namespace);

			if (br.hasWrite) {
				list.add(Value.get(br.key.digest));
			}
		}

		if (list.size() == 0) {
			// Readonly batch does not need to add key digests.
			return null;
		}
		return getTranOps(tran, list);
	}

	private static Operation[] getTranOps(Tran tran, ArrayList<Value> list) {
		if (tran.getDeadline() == 0) {
			// No existing monitor record.
			return new Operation[] {
				Operation.put(new Bin(BinNameId, tran.getId())),
				ListOperation.appendItems(OrderedListPolicy, BinNameDigests, list)
			};
		}
		else {
			return new Operation[] {
				ListOperation.appendItems(OrderedListPolicy, BinNameDigests, list)
			};
		}
	}

	private static void addWriteKeys(Cluster cluster, Policy policy, Operation[] ops) {
		Key tranKey = getTranMonitorKey(policy.tran);
		WritePolicy wp = copyTimeoutPolicy(policy);
		OperateArgs args = new OperateArgs(wp, null, null, ops);
		TranAddKeys cmd = new TranAddKeys(cluster, tranKey, args);
		cmd.execute();
	}

	public static Key getTranMonitorKey(Tran tran) {
		return new Key(tran.getNamespace(), "<ERO~MRT", tran.getId());
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
}
