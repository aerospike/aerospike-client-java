/*
 * Copyright 2012-2019 Aerospike, Inc.
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

import java.io.IOException;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.Replica;

public final class Batch {
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public static final class ReadListCommand extends BatchCommand {
		private final List<BatchRead> records;

		public ReadListCommand(Executor parent, BatchNode batch, BatchPolicy policy, List<BatchRead> records) {
			super(parent, batch, policy);
			this.records = records;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, records, batch);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (resultCode == 0) {
				BatchRead record = records.get(batchIndex);
				record.record = parseRecord();
			}
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new ReadListCommand(parent, batchNode, policy, records);
		}

		@Override
		protected List<BatchNode> generateBatchNodes(Cluster cluster) {
			return BatchNode.generateList(cluster, policy, records, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public static final class GetArrayCommand extends BatchCommand {
		private final Key[] keys;
		private final String[] binNames;
		private final Record[] records;
		private final int readAttr;

		public GetArrayCommand(
			Executor parent,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			String[] binNames,
			Record[] records,
			int readAttr
		) {
			super(parent, batch, policy);
			this.keys = keys;
			this.binNames = binNames;
			this.records = records;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, binNames, readAttr);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (resultCode == 0) {
				records[batchIndex] = parseRecord();
			}
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new GetArrayCommand(parent, batchNode, policy, keys, binNames, records, readAttr);
		}

		@Override
		protected List<BatchNode> generateBatchNodes(Cluster cluster) {
			return BatchNode.generateList(cluster, policy, keys, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public static final class ExistsArrayCommand extends BatchCommand {
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayCommand(
			Executor parent,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(parent, batch, policy);
			this.keys = keys;
			this.existsArray = existsArray;
		}

		@Override
		protected void writeBuffer() {
			setBatchRead(policy, keys, batch, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
		}

		@Override
		protected void parseRow(Key key) throws IOException {
			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new ExistsArrayCommand(parent, batchNode, policy, keys, existsArray);
		}

		@Override
		protected List<BatchNode> generateBatchNodes(Cluster cluster) {
			return BatchNode.generateList(cluster, policy, keys, sequenceAP, sequenceSC, batch);
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	private static abstract class BatchCommand extends MultiCommand {
		final Executor parent;
		final BatchNode batch;
		final BatchPolicy policy;
		int sequenceAP;
		int sequenceSC;

		public BatchCommand(Executor parent, BatchNode batch, BatchPolicy policy) {
			super(batch.node, false);
			this.parent = parent;
			this.batch = batch;
			this.policy = policy;
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (! ((policy.replica == Replica.SEQUENCE || policy.replica == Replica.PREFER_RACK) &&
				   (parent == null || ! parent.isDone()))) {
				// Perform regular retry to same node.
				return true;
			}
			sequenceAP++;

			if (! timeout || policy.readModeSC != ReadModeSC.LINEARIZE) {
				sequenceSC++;
			}
			return false;
		}

		@Override
		protected boolean retryBatch(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			long deadline,
			int iteration,
			int commandSentCounter
		) {
			// Retry requires keys for this node to be split among other nodes.
			// This is both recursive and exponential.
			List<BatchNode> batchNodes = generateBatchNodes(cluster);

			if (batchNodes.size() == 1 && batchNodes.get(0).node == batch.node) {
				// Batch node is the same.  Go through normal retry.
				return false;
			}

			// Run batch requests sequentially in same thread.
			for (BatchNode batchNode : batchNodes) {
				BatchCommand command = createCommand(batchNode);
				command.sequenceAP = sequenceAP;
				command.sequenceSC = sequenceSC;
				command.execute(cluster, policy, true, socketTimeout, totalTimeout, deadline, iteration, commandSentCounter);
			}
			return true;
		}

		abstract BatchCommand createCommand(BatchNode batchNode);
		abstract List<BatchNode> generateBatchNodes(Cluster cluster);
	}
}
