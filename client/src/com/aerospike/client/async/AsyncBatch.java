/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.BatchNode;
import com.aerospike.client.command.BatchNodeList;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.util.Util;

public final class AsyncBatch {
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------

	public static final class ReadListExecutor extends AsyncBatchExecutor {
		private final BatchListListener listener;
		private final List<BatchRead> records;

		public ReadListExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchListListener listener,
			List<BatchRead> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new ReadListCommand(this, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(records);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class ReadListCommand extends AsyncBatchCommand {
		private final List<BatchRead> records;

		public ReadListCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			List<BatchRead> records
		) {
			super(parent, batch, batchPolicy, true);
			this.records = records;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				setBatchOperate(batchPolicy, records, batch);
			}
			else {
				setBatchRead(batchPolicy, records, batch);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRead record = records.get(batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else {
				record.setError(resultCode, false);
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new ReadListCommand(parent, batchNode, batchPolicy, records);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// ReadSequence
	//-------------------------------------------------------

	public static final class ReadSequenceExecutor extends AsyncBatchExecutor {
		private final BatchSequenceListener listener;

		public ReadSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new ReadSequenceCommand(this, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class ReadSequenceCommand extends AsyncBatchCommand {
		private final BatchSequenceListener listener;
		private final List<BatchRead> records;

		public ReadSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchSequenceListener listener,
			List<BatchRead> records
		) {
			super(parent, batch, batchPolicy, true);
			this.listener = listener;
			this.records = records;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				setBatchOperate(batchPolicy, records, batch);
			}
			else {
				setBatchRead(batchPolicy, records, batch);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRead record = records.get(batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else {
				record.setError(resultCode, false);
			}
			listener.onRecord(record);
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new ReadSequenceCommand(parent, batchNode, batchPolicy, listener, records);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public static final class GetArrayExecutor extends AsyncBatchExecutor {
		private final RecordArrayListener listener;
		private final Key[] keys;
		private final Record[] recordArray;

		public GetArrayExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			RecordArrayListener listener,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			int readAttr,
			boolean isOperation
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
			this.keys = keys;
			this.recordArray = new Record[keys.length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new GetArrayCommand(this, batchNode, policy, keys, binNames, ops, recordArray, readAttr, isOperation);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(keys, recordArray);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(new AerospikeException.BatchRecords(recordArray, ae));
		}
	}

	private static final class GetArrayCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final String[] binNames;
		private final Operation[] ops;
		private final Record[] records;
		private final int readAttr;

		public GetArrayCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			Record[] records,
			int readAttr,
			boolean isOperation
		) {
			super(parent, batch, batchPolicy, isOperation);
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.records = records;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(batchPolicy, readAttr, ops);
				setBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			if (resultCode == 0) {
				records[batchIndex] = parseRecord();
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new GetArrayCommand(parent, batchNode, batchPolicy, keys, binNames, ops, records, readAttr, isOperation);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// GetSequence
	//-------------------------------------------------------

	public static final class GetSequenceExecutor extends AsyncBatchExecutor {
		private final RecordSequenceListener listener;

		public GetSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			int readAttr,
			boolean isOperation
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new GetSequenceCommand(this, batchNode, policy, keys, binNames, ops, listener, readAttr, isOperation);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}

		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class GetSequenceCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final String[] binNames;
		private final Operation[] ops;
		private final RecordSequenceListener listener;
		private final int readAttr;

		public GetSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			RecordSequenceListener listener,
			int readAttr,
			boolean isOperation
		) {
			super(parent, batch, batchPolicy, isOperation);
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.listener = listener;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(batchPolicy, readAttr, ops);
				setBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			Key keyOrig = keys[batchIndex];

			if (resultCode == 0) {
				Record record = parseRecord();
				listener.onRecord(keyOrig, record);
			}
			else {
				listener.onRecord(keyOrig, null);
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new GetSequenceCommand(parent, batchNode, batchPolicy, keys, binNames, ops, listener, readAttr, isOperation);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public static final class ExistsArrayExecutor extends AsyncBatchExecutor {
		private final ExistsArrayListener listener;
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			ExistsArrayListener listener
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;
			this.keys = keys;
			this.existsArray = new boolean[keys.length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new ExistsArrayCommand(this, batchNode, policy, keys, existsArray);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(keys, existsArray);
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(new AerospikeException.BatchExists(existsArray, ae));
		}
	}

	private static final class ExistsArrayCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			boolean[] existsArray
		) {
			super(parent, batch, batchPolicy, false);
			this.keys = keys;
			this.existsArray = existsArray;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				setBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			existsArray[batchIndex] = resultCode == 0;
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new ExistsArrayCommand(parent, batchNode, batchPolicy, keys, existsArray);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// ExistsSequence
	//-------------------------------------------------------

	public static final class ExistsSequenceExecutor extends AsyncBatchExecutor {
		private final ExistsSequenceListener listener;

		public ExistsSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			Key[] keys,
			ExistsSequenceListener listener
		) {
			super(eventLoop, cluster, false);
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, false, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new ExistsSequenceCommand(this, batchNode, policy, keys, listener);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class ExistsSequenceCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final ExistsSequenceListener listener;

		public ExistsSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			ExistsSequenceListener listener
		) {
			super(parent, batch, batchPolicy, false);
			this.keys = keys;
			this.listener = listener;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(batchPolicy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				setBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			if (opCount > 0) {
				throw new AerospikeException.Parse("Received bins that were not requested!");
			}

			Key keyOrig = keys[batchIndex];
			listener.onExists(keyOrig, resultCode == 0);
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new ExistsSequenceCommand(parent, batchNode, batchPolicy, keys, listener);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, parent);
		}
	}

	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public static final class OperateListExecutor extends AsyncBatchExecutor {
		private final BatchOperateListListener listener;
		private final List<BatchRecord> records;

		public OperateListExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchOperateListListener listener,
			List<BatchRecord> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = records;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new OperateListCommand(this, batchNode, policy, records);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(records, getStatus());
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class OperateListCommand extends AsyncBatchCommand {
		private final List<BatchRecord> records;

		public OperateListCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			List<BatchRecord> records
		) {
			super(parent, batch, batchPolicy, true);
			this.records = records;
		}

		@Override
		protected boolean isWrite() {
			// This method is only called to set inDoubt on node level errors.
			// setError() will filter out reads when setting record level inDoubt.
			return true;
		}

		@Override
		protected void writeBuffer() {
			setBatchOperate(batchPolicy, records, batch);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records.get(batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.batchInDoubt(record.hasWrite, commandSentCounter);
					parent.setRowError();
					return;
				}
			}

			record.setError(resultCode, Command.batchInDoubt(record.hasWrite, commandSentCounter));
			parent.setRowError();
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			if (!inDoubt) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records.get(index);

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = record.hasWrite;
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new OperateListCommand(parent, batchNode, batchPolicy, records);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// OperateSequence
	//-------------------------------------------------------

	public static final class OperateSequenceExecutor extends AsyncBatchExecutor {
		private final BatchRecordSequenceListener listener;

		public OperateSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, records, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new OperateSequenceCommand(this, batchNode, policy, listener, records);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess();
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class OperateSequenceCommand extends AsyncBatchCommand {
		private final BatchRecordSequenceListener listener;
		private final List<BatchRecord> records;

		public OperateSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			BatchRecordSequenceListener listener,
			List<BatchRecord> records
		) {
			super(parent, batch, batchPolicy, true);
			this.listener = listener;
			this.records = records;
		}

		@Override
		protected boolean isWrite() {
			// This method is only called to set inDoubt on node level errors.
			// setError() will filter out reads when setting record level inDoubt.
			return true;
		}

		@Override
		protected void writeBuffer() {
			setBatchOperate(batchPolicy, records, batch);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records.get(batchIndex);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.batchInDoubt(record.hasWrite, commandSentCounter);
				}
				else {
					record.setError(resultCode, Command.batchInDoubt(record.hasWrite, commandSentCounter));
				}
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(record.hasWrite, commandSentCounter));
			}
			AsyncBatch.onRecord(listener, record, batchIndex);
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			if (!inDoubt) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records.get(index);

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					// Set inDoubt, but do not call onRecord() because user already has access to full
					// BatchRecord list and can examine each record for inDoubt when the exception occurs.
					record.inDoubt = record.hasWrite;
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new OperateSequenceCommand(parent, batchNode, batchPolicy, listener, records);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, parent);
		}
	}

	//-------------------------------------------------------
	// OperateRecordArray
	//-------------------------------------------------------

	public static final class OperateRecordArrayExecutor extends AsyncBatchExecutor {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] records;

		public OperateRecordArrayExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordArrayListener listener,
			Key[] keys,
			Operation[] ops,
			BatchAttr attr
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.records = new BatchRecord[keys.length];

			for (int i = 0; i < keys.length; i++) {
				this.records[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, records, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new OperateRecordArrayCommand(this, batchNode, policy, keys, ops, records, attr);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(records, getStatus());
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(records, ae);
		}
	}

	private static final class OperateRecordArrayCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchRecord[] records;
		private final BatchAttr attr;

		public OperateRecordArrayCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr
		) {
			super(parent, batch, batchPolicy, ops != null);
			this.keys = keys;
			this.ops = ops;
			this.records = records;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				parent.setRowError();
			}
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			if (!inDoubt || !attr.hasWrite) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = inDoubt;
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new OperateRecordArrayCommand(parent, batchNode, batchPolicy, keys, ops, records, attr);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// OperateRecordSequence
	//-------------------------------------------------------

	public static final class OperateRecordSequenceExecutor extends AsyncBatchExecutor {
		private final BatchRecordSequenceListener listener;
		private final boolean[] sent;

		public OperateRecordSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			Key[] keys,
			Operation[] ops,
			BatchAttr attr
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.sent = new boolean[keys.length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new OperateRecordSequenceCommand(this, batchNode, policy, keys, ops, sent, listener, attr);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		@Override
		public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
			BatchRecord record = new BatchRecord(key, null, ae.getResultCode(), inDoubt, hasWrite);
			sent[index] = true;
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}

		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class OperateRecordSequenceCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final Operation[] ops;
		private final boolean[] sent;
		private final BatchRecordSequenceListener listener;
		private final BatchAttr attr;

		public OperateRecordSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			boolean[] sent,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(parent, batch, batchPolicy, ops != null);
			this.keys = keys;
			this.ops = ops;
 			this.sent = sent;
			this.listener = listener;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			Key keyOrig = keys[batchIndex];
			BatchRecord record;

			if (resultCode == 0) {
				record = new BatchRecord(keyOrig, parseRecord(), attr.hasWrite);
			}
			else {
				record = new BatchRecord(keyOrig, null, resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
			}
			sent[batchIndex] = true;
			AsyncBatch.onRecord(listener, record, batchIndex);
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			// Set inDoubt for all unsent records, so the listener receives a full set of records.
			for (int index : batch.offsets) {
				if (! sent[index]) {
					Key key = keys[index];
					BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, attr.hasWrite && inDoubt, attr.hasWrite);
					sent[index] = true;
					AsyncBatch.onRecord(listener, record, index);
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new OperateRecordSequenceCommand(parent, batchNode, batchPolicy, keys, ops, sent, listener, attr);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sent, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// UDFArray
	//-------------------------------------------------------

	public static final class UDFArrayExecutor extends AsyncBatchExecutor {
		private final BatchRecordArrayListener listener;
		private final BatchRecord[] recordArray;

		public UDFArrayExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordArrayListener listener,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchAttr attr
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.recordArray = new BatchRecord[keys.length];

			for (int i = 0; i < keys.length; i++) {
				this.recordArray[i] = new BatchRecord(keys[i], attr.hasWrite);
			}

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, recordArray, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new UDFArrayCommand(this, batchNode, policy, keys, packageName, functionName, argBytes, recordArray, attr);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		protected void onSuccess() {
			listener.onSuccess(recordArray, getStatus());
		}

		protected void onFailure(AerospikeException ae) {
			listener.onFailure(recordArray, ae);
		}
	}

	public static final class UDFArrayCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final BatchRecord[] records;
		private final BatchAttr attr;

		public UDFArrayCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr
		) {
			super(parent, batch, batchPolicy, false);
 			this.keys = keys;
 			this.packageName = packageName;
 			this.functionName = functionName;
 			this.argBytes = argBytes;
			this.records = records;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0) {
				record.setRecord(parseRecord());
				return;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.batchInDoubt(attr.hasWrite, commandSentCounter);
					parent.setRowError();
					return;
				}
			}

			record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
			parent.setRowError();
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			if (!inDoubt || !attr.hasWrite) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = inDoubt;
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new UDFArrayCommand(parent, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// UDFSequence
	//-------------------------------------------------------

	public static final class UDFSequenceExecutor extends AsyncBatchExecutor {
		private final BatchRecordSequenceListener listener;
		private final boolean[] sent;

		public UDFSequenceExecutor(
			EventLoop eventLoop,
			Cluster cluster,
			BatchPolicy policy,
			BatchRecordSequenceListener listener,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchAttr attr
		) {
			super(eventLoop, cluster, true);
			this.listener = listener;
			this.sent = new boolean[keys.length];

			// Create commands.
			List<BatchNode> batchNodes = BatchNodeList.generate(cluster, policy, keys, null, attr.hasWrite, this);
			AsyncBatchCommand[] tasks = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				tasks[count++] = new UDFSequenceCommand(this, batchNode, policy, keys, packageName, functionName, argBytes, sent, listener, attr);
			}
			// Dispatch commands to nodes.
			execute(tasks);
		}

		@Override
		public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
			BatchRecord record = new BatchRecord(key, null, ae.getResultCode(), inDoubt, hasWrite);
			sent[index] = true;
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		protected void onSuccess() {
			listener.onSuccess();
		}

		@Override
		protected void onFailure(AerospikeException ae) {
			listener.onFailure(ae);
		}
	}

	private static final class UDFSequenceCommand extends AsyncBatchCommand {
		private final Key[] keys;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final boolean[] sent;
		private final BatchRecordSequenceListener listener;
		private final BatchAttr attr;

		public UDFSequenceCommand(
			AsyncBatchExecutor parent,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			boolean[] sent,
			BatchRecordSequenceListener listener,
			BatchAttr attr
		) {
			super(parent, batch, batchPolicy, false);
			this.keys = keys;
 			this.packageName = packageName;
 			this.functionName = functionName;
 			this.argBytes = argBytes;
 			this.sent = sent;
			this.listener = listener;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		@Override
		protected void parseRow() {
			skipKey(fieldCount);

			Key keyOrig = keys[batchIndex];
			BatchRecord record;

			if (resultCode == 0) {
				record = new BatchRecord(keyOrig, parseRecord(), attr.hasWrite);
			}
			else if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record = new BatchRecord(keyOrig, r, resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				}
				else {
					record = new BatchRecord(keyOrig, null, resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				}
			}
			else {
				record = new BatchRecord(keyOrig, null, resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
			}
			sent[batchIndex] = true;
			AsyncBatch.onRecord(listener, record, batchIndex);
		}

		@Override
		protected void setInDoubt(boolean inDoubt) {
			// Set inDoubt for all unsent records, so the listener receives a full set of records.
			for (int index : batch.offsets) {
				if (! sent[index]) {
					Key key = keys[index];
					BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, attr.hasWrite && inDoubt, attr.hasWrite);
					sent[index] = true;
					AsyncBatch.onRecord(listener, record, index);
				}
			}
		}

		@Override
		protected AsyncBatchCommand createCommand(BatchNode batchNode) {
			return new UDFSequenceCommand(parent, batchNode, batchPolicy, keys, packageName, functionName, argBytes, sent, listener, attr);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(parent.cluster, batchPolicy, keys, sent, sequenceAP, sequenceSC, batch, attr.hasWrite, parent);
		}
	}

	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	static abstract class AsyncBatchCommand extends AsyncMultiCommand {
		final AsyncBatchExecutor parent;
		final BatchNode batch;
		final BatchPolicy batchPolicy;
		int sequenceAP;
		int sequenceSC;

		public AsyncBatchCommand(AsyncBatchExecutor parent, BatchNode batch, BatchPolicy batchPolicy, boolean isOperation) {
			super(batch.node, batchPolicy, isOperation);
			this.parent = parent;
			this.batch = batch;
			this.batchPolicy = batchPolicy;
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (parent.done || ! (policy.replica == Replica.SEQUENCE || policy.replica == Replica.PREFER_RACK)) {
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
		protected boolean retryBatch(Runnable other, long deadline) {
			// Retry requires keys for this node to be split among other nodes.
			// This can cause an exponential number of commands.
			List<BatchNode> batchNodes = generateBatchNodes();

			if (batchNodes.size() == 0 || (batchNodes.size() == 1 && batchNodes.get(0).node == batch.node)) {
				// Go through normal retry.
				return false;
			}

			AsyncBatchCommand[] cmds = new AsyncBatchCommand[batchNodes.size()];
			int count = 0;

			for (BatchNode batchNode : batchNodes) {
				AsyncBatchCommand cmd = createCommand(batchNode);
				cmd.sequenceAP = sequenceAP;
				cmd.sequenceSC = sequenceSC;
				cmds[count++] = cmd;
			}
			parent.executeBatchRetry(cmds, this, other, deadline);
			return true;
		}

		@Override
		protected void onSuccess() {
			parent.childSuccess();
		}

		@Override
		protected void onFailure(AerospikeException e) {
			setInDoubt(e.getInDoubt());
			parent.childFailure(e);
		}

		protected void setInDoubt(boolean inDoubt) {
			// Do nothing by default. Batch writes will override this method.
		}

		abstract AsyncBatchCommand createCommand(BatchNode batchNode);
		abstract List<BatchNode> generateBatchNodes();
	}

	private static void onRecord(BatchRecordSequenceListener listener, BatchRecord record, int index) {
		try {
			listener.onRecord(record, index);
		}
		catch (Throwable e) {
			Log.error("Unexpected exception from onRecord(): " + Util.getErrorMessage(e));
		}
	}
}
