/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;

public final class AsyncBatchSingle {
	public static final class Delete extends AsyncBaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;

		public Delete(
			AsyncBatchExecutor parent,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Node node
		) {
			super(parent, cluster, policy, record.key, node, true);
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setDelete(policy, record.key, attr);
		}

		@Override
		protected boolean parseResult() {
			validateHeaderSize();

			int resultCode = dataBuffer[5] & 0xFF;
			int generation = Buffer.bytesToInt(dataBuffer, 6);
			int expiration = Buffer.bytesToInt(dataBuffer, 10);

			if (resultCode == 0) {
				record.setRecord(new Record(null, generation, expiration));
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				parent.setRowError();
			}
			return true;
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Async Batch Base Command
	//-------------------------------------------------------

	static abstract class AsyncBaseCommand extends AsyncCommand {
		final AsyncBatchExecutor parent;
		final Cluster cluster;
		final Key key;
		Node node;
		int sequence;
		final boolean hasWrite;

		public AsyncBaseCommand(
			AsyncBatchExecutor parent,
			Cluster cluster,
			Policy policy,
			Key key,
			Node node,
			boolean hasWrite
		) {
			super(policy, true);
			this.parent = parent;
			this.cluster = cluster;
			this.key = key;
			this.node = node;
			this.hasWrite = hasWrite;
		}

		@Override
		boolean isWrite() {
			return hasWrite;
		}

		@Override
		protected Node getNode(Cluster cluster) {
			return node;
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.BATCH;
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (hasWrite) {
				Partition p = Partition.write(cluster, policy, key);
				p.sequence = sequence;
				p.prevNode = node;
				p.prepareRetryWrite(timeout);
				node = p.getNodeWrite(cluster);
				sequence = p.sequence;
			}
			else {
				Partition p = Partition.read(cluster, policy, key);
				p.sequence = sequence;
				p.prevNode = node;
				p.prepareRetryRead(timeout);
				node = p.getNodeRead(cluster);
				sequence = p.sequence;
			}
			return true;
		}

		@Override
		protected void onSuccess() {
			parent.childSuccess();
		}

		@Override
		protected void onFailure(AerospikeException e) {
			if (e.getInDoubt()) {
				setInDoubt();
			}
			parent.childFailure(e);
		}

		public void setInDoubt() {
		}
	}
}
