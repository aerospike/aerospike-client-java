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
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.AsyncBatchExecutor.BatchRecordSequence;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public final class AsyncBatchSingle {
	//-------------------------------------------------------
	// Delete
	//-------------------------------------------------------

	public static final class DeleteArray extends AsyncBaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;

		public DeleteArray(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, true);
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
				executor.setRowError();
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

	public static final class DeleteSequence extends AsyncBaseCommand {
		private final BatchRecordSequence parent;
		private final BatchRecordSequenceListener listener;
		private final Key key;
		private final BatchAttr attr;
		private final int index;

		public DeleteSequence(
			BatchRecordSequence executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			Key key,
			BatchAttr attr,
			Node node,
			int index
		) {
			super(executor, cluster, policy, key, node, true);
			this.parent = executor;
			this.listener = listener;
			this.key = key;
			this.attr = attr;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setDelete(policy, key, attr);
		}

		@Override
		protected boolean parseResult() {
			validateHeaderSize();

			int resultCode = dataBuffer[5] & 0xFF;
			int generation = Buffer.bytesToInt(dataBuffer, 6);
			int expiration = Buffer.bytesToInt(dataBuffer, 10);

			BatchRecord record;

			if (resultCode == 0) {
				record = new BatchRecord(key, new Record(null, generation, expiration), true);
			}
			else {
				record = new BatchRecord(key, null, resultCode, Command.batchInDoubt(true, commandSentCounter), true);
				executor.setRowError();
			}
			parent.setSent(index);
			AsyncBatch.onRecord(listener, record, index);
			return true;
		}

		@Override
		public void setInDoubt() {
			if (! parent.exchangeSent(index)) {
				BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, true, true);
				AsyncBatch.onRecord(listener, record, index);
			}
		}
	}

	//-------------------------------------------------------
	// Exists
	//-------------------------------------------------------

	public static final class ExistsArray extends AsyncBaseCommand {
		private final boolean[] existsArray;
		private final int index;

		public ExistsArray(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			boolean[] existsArray,
			int index
		) {
			super(executor, cluster, policy, key, node, false);
			this.existsArray = existsArray;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected boolean parseResult() {
			validateHeaderSize();

			int resultCode = dataBuffer[5] & 0xFF;
			int opCount = Buffer.bytesToShort(dataBuffer, 20);

			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[index] = resultCode == 0;
			return true;
		}
	}

	public static final class ExistsSequence extends AsyncBaseCommand {
		private final ExistsSequenceListener listener;
		private final Key key;

		public ExistsSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			ExistsSequenceListener listener,
			Key key,
			Node node
		) {
			super(executor, cluster, policy, key, node, false);
			this.listener = listener;
			this.key = key;
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected boolean parseResult() {
			validateHeaderSize();

			int resultCode = dataBuffer[5] & 0xFF;
			int opCount = Buffer.bytesToShort(dataBuffer, 20);

			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			try {
				listener.onExists(key, resultCode == 0);
			}
			catch (Throwable e) {
				Log.error("Unexpected exception from onExists(): " + Util.getErrorMessage(e));
			}
			return true;
		}
	}

	//-------------------------------------------------------
	// Read Record
	//-------------------------------------------------------

	public static final class ReadRecord extends AsyncBaseCommand {
		private final BatchRead record;

		public ReadRecord(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, false);
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, record);
		}

		@Override
		protected final boolean parseResult() {
			validateHeaderSize();

			RecordParser rp = new RecordParser(dataBuffer, 5);

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(record.ops != null));
			}
			else {
				record.setError(rp.resultCode, false);
			}
			return true;
		}
	}

	public static final class ReadSequence extends AsyncBaseCommand {
		private final BatchSequenceListener listener;
		private final BatchRead record;

		public ReadSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchSequenceListener listener,
			BatchRead record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, false);
			this.listener = listener;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, record);
		}

		@Override
		protected final boolean parseResult() {
			validateHeaderSize();

			RecordParser rp = new RecordParser(dataBuffer, 5);

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(record.ops != null));
			}
			else {
				record.setError(rp.resultCode, false);
			}
			listener.onRecord(record);
			return true;
		}
	}

	//-------------------------------------------------------
	// Async Batch Base Command
	//-------------------------------------------------------

	static abstract class AsyncBaseCommand extends AsyncCommand {
		final AsyncBatchExecutor executor;
		final Cluster cluster;
		final Key key;
		Node node;
		int sequence;
		final boolean hasWrite;

		public AsyncBaseCommand(
			AsyncBatchExecutor executor,
			Cluster cluster,
			Policy policy,
			Key key,
			Node node,
			boolean hasWrite
		) {
			super(policy, true);
			this.executor = executor;
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
			executor.childSuccess();
		}

		@Override
		protected void onFailure(AerospikeException e) {
			if (e.getInDoubt()) {
				setInDoubt();
			}
			executor.childFailure(e);
		}

		public void setInDoubt() {
		}
	}
}
