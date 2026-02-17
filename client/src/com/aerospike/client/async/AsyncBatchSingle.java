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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.async.AsyncBatchExecutor.BatchRecordSequence;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.BatchAttr;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.RecordParser;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

public final class AsyncBatchSingle {
	//-------------------------------------------------------
	// Read
	//-------------------------------------------------------

	public static final class ReadGetSequence extends Read {
		private final BatchSequenceListener listener;

		public ReadGetSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node,
			BatchSequenceListener listener
		) {
			super(executor, cluster, policy, record, node);
			this.listener = listener;
		}

		@Override
		protected void parseResult(RecordParser rp) {
			super.parseResult(rp);

			try {
				listener.onRecord(record);
			}
			catch (Throwable e) {
				Log.error("Unexpected exception from onRecord(): " + Util.getErrorMessage(e));
			}
		}
	}

	public static final class ReadSequence extends Read {
		private final BatchRecordSequenceListener listener;
		private final int index;

		public ReadSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchRead record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, record, node);
			this.listener = listener;
			this.index = index;
		}

		@Override
		protected void parseResult(RecordParser rp) {
			super.parseResult(rp);
			AsyncBatch.onRecord(listener, record, index);
		}
	}

	public static class Read extends AsyncBaseCommand {
		final BatchRead record;

		public Read(
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
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(record.ops != null));
			}
			else {
				record.setError(rp.resultCode, false);
				executor.setRowError();
			}
		}
	}

	//-------------------------------------------------------
	// Operate/Get
	//-------------------------------------------------------

	public static final class OperateGet extends Get {
		private final Operation[] ops;

		public OperateGet(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Operation[] ops,
			Record[] records,
			Node node,
			int index
		) {
			super(executor, cluster, policy, key, null, records, node, index, true);
			this.ops = ops;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, ops);
		}
	}

	public static class Get extends AsyncBaseCommand {
		private final String[] binNames;
		private final Record[] records;
		private final int index;
		private final boolean isOperation;

		public Get(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			String[] binNames,
			Record[] records,
			Node node,
			int index,
			boolean isOperation
		) {
			super(executor, cluster, policy, key, node, false);
			this.binNames = binNames;
			this.records = records;
			this.index = index;
			this.isOperation = isOperation;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, binNames);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				records[index] = rp.parseRecord(isOperation);
			}
		}
	}

	public static final class OperateGetSequence extends GetSequence {
		private final Operation[] ops;

		public OperateGetSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key key,
			Operation[] ops,
			Node node
		) {
			super(executor, cluster, policy, listener, key, null, node, true);
			this.ops = ops;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, ops);
		}
	}

	public static class GetSequence extends AsyncBaseCommand {
		private final RecordSequenceListener listener;
		private final String[] binNames;
		private final boolean isOperation;

		public GetSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			RecordSequenceListener listener,
			Key key,
			String[] binNames,
			Node node,
			boolean isOperation
		) {
			super(executor, cluster, policy, key, node, false);
			this.listener = listener;
			this.binNames = binNames;
			this.isOperation = isOperation;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, binNames);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			Record record = null;

			if (rp.resultCode == ResultCode.OK) {
				record = rp.parseRecord(isOperation);
			}
			AsyncBatch.onRecord(listener, key, record);
		}
	}

	//-------------------------------------------------------
	// Read Header
	//-------------------------------------------------------

	public static class ReadHeaderSequence extends AsyncBaseCommand {
		private final RecordSequenceListener listener;

		public ReadHeaderSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			RecordSequenceListener listener
		) {
			super(executor, cluster, policy, key, node, false);
			this.listener = listener;
		}

		@Override
		protected void writeBuffer() {
			setReadHeader(policy, key);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			Record record = null;

			if (rp.resultCode == ResultCode.OK) {
				record = rp.parseRecord(false);
			}
			AsyncBatch.onRecord(listener, key, record);
		}
	}

	public static class ReadHeader extends AsyncBaseCommand {
		private final Record[] records;
		private final int index;

		public ReadHeader(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Record[] records,
			Node node,
			int index
		) {
			super(executor, cluster, policy, key, node, false);
			this.records = records;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setReadHeader(policy, key);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				records[index] = rp.parseRecord(false);
			}
		}
	}

	//-------------------------------------------------------
	// Exists
	//-------------------------------------------------------

	public static final class ExistsSequence extends AsyncBaseCommand {
		private final ExistsSequenceListener listener;

		public ExistsSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Node node,
			ExistsSequenceListener listener
		) {
			super(executor, cluster, policy, key, node, false);
			this.listener = listener;
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			try {
				listener.onExists(key, rp.resultCode == 0);
			}
			catch (Throwable e) {
				Log.error("Unexpected exception from onExists(): " + Util.getErrorMessage(e));
			}
		}
	}

	public static final class Exists extends AsyncBaseCommand {
		private final boolean[] existsArray;
		private final int index;

		public Exists(
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
		protected void parseResult(RecordParser rp) {
			existsArray[index] = rp.resultCode == 0;
		}
	}

	//-------------------------------------------------------
	// Operate
	//-------------------------------------------------------

	public static final class OperateSequence extends AsyncBaseCommand {
		private final BatchRecordSequence parent;
		private final BatchRecordSequenceListener listener;
		private final BatchAttr attr;
		private final Operation[] ops;
		private final int index;

		public OperateSequence(
			BatchRecordSequence executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			Operation[] ops,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, key, node, attr.hasWrite);
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.ops = ops;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setOperate(policy, attr, key, ops);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			BatchRecord record;

			if (rp.resultCode == 0) {
				record = new BatchRecord(key, rp.parseRecord(true), attr.hasWrite);
			}
			else {
				record = new BatchRecord(key, null, rp.resultCode,
					Command.batchInDoubt(attr.hasWrite, commandSentCounter), attr.hasWrite);
				executor.setRowError();
			}
			parent.setSent(index);
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		public void setInDoubt() {
			if (! parent.exchangeSent(index)) {
				BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, true, attr.hasWrite);
				AsyncBatch.onRecord(listener, record, index);
			}
		}
	}

	public static final class Operate extends AsyncBaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;
		private final Operation[] ops;

		public Operate(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Operation[] ops,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, attr.hasWrite);
			this.attr = attr;
			this.record = record;
			this.ops = ops;
		}

		@Override
		protected void writeBuffer() {
			setOperate(policy, attr, record.key, ops);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				executor.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Write
	//-------------------------------------------------------

	public static final class WriteSequence extends Write {
		private final BatchRecordSequenceListener listener;
		private final int index;

		public WriteSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchWrite record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, attr, record, node);
			this.listener = listener;
			this.index = index;
		}

		@Override
		protected void parseResult(RecordParser rp) {
			super.parseResult(rp);
			AsyncBatch.onRecord(listener, record, index);
		}

		// setInDoubt() is not overridden to call onRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.
	}

	public static class Write extends AsyncBaseCommand {
		private final BatchAttr attr;
		final BatchWrite record;

		public Write(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchWrite record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, true);
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setOperate(policy, attr, record.key, record.ops);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				executor.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public static final class UDFSequence extends UDF {
		private final BatchRecordSequenceListener listener;
		private final int index;

		public UDFSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchUDF record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, attr, record, node);
			this.listener = listener;
			this.index = index;
		}

		@Override
		protected void parseResult(RecordParser rp) {
			super.parseResult(rp);
			AsyncBatch.onRecord(listener, record, index);
		}

		// setInDoubt() is not overridden to call onRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.
	}

	public static class UDF extends AsyncBaseCommand {
		private final BatchAttr attr;
		final BatchUDF record;

		public UDF(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchUDF record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, true);
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setUdf(policy, attr, record.key, record.packageName, record.functionName, record.functionArgs);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(false));
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
				}
				record.resultCode = rp.resultCode;
				record.inDoubt = Command.batchInDoubt(true, commandSentCounter);
				executor.setRowError();
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				executor.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static final class UDFSequenceCommand extends AsyncBaseCommand {
		private final BatchRecordSequence parent;
		private final BatchRecordSequenceListener listener;
		private final BatchAttr attr;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final int index;

		public UDFSequenceCommand(
			BatchRecordSequence executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			String packageName,
			String functionName,
			byte[] argBytes,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, key, node, true);
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setUdf(policy, attr, key, packageName, functionName, argBytes);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			BatchRecord record;

			if (rp.resultCode == ResultCode.OK) {
				record = new BatchRecord(key, rp.parseRecord(true), attr.hasWrite);
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record = new BatchRecord(key, r, rp.resultCode, Command.batchInDoubt(true, commandSentCounter), true);
				}
				else {
					record = new BatchRecord(key, null, rp.resultCode, Command.batchInDoubt(true, commandSentCounter), true);
				}
				executor.setRowError();
			}
			else {
				record = new BatchRecord(key, null, rp.resultCode, Command.batchInDoubt(true, commandSentCounter), true);
				executor.setRowError();
			}
			parent.setSent(index);
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		public void setInDoubt() {
			if (! parent.exchangeSent(index)) {
				BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, true, true);
				AsyncBatch.onRecord(listener, record, index);
			}
		}
	}

	public static class UDFCommand extends AsyncBaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;

		public UDFCommand(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			String packageName,
			String functionName,
			byte[] argBytes,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, true);
			this.attr = attr;
			this.record = record;
			this.packageName = packageName;
			this.functionName = functionName;
			this.argBytes = argBytes;
		}

		@Override
		protected void writeBuffer() {
			setUdf(policy, attr, key, packageName, functionName, argBytes);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(false));
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
				}
				record.resultCode = rp.resultCode;
				record.inDoubt = Command.batchInDoubt(true, commandSentCounter);
				executor.setRowError();
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				executor.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Delete
	//-------------------------------------------------------

	public static final class DeleteSequenceSent extends AsyncBaseCommand {
		private final BatchRecordSequence parent;
		private final BatchRecordSequenceListener listener;
		private final BatchAttr attr;
		private final int index;

		public DeleteSequenceSent(
			BatchRecordSequence executor,
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			BatchAttr attr,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, key, node, true);
			this.parent = executor;
			this.listener = listener;
			this.attr = attr;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setDelete(policy, key, attr);
		}

		@Override
		protected void parseResult(RecordParser rp) {
			BatchRecord record;

			if (rp.resultCode == 0) {
				record = new BatchRecord(key, new Record(null, rp.generation, rp.expiration), true);
			}
			else {
				record = new BatchRecord(key, null, rp.resultCode, Command.batchInDoubt(true, commandSentCounter), true);
				executor.setRowError();
			}
			parent.setSent(index);
			AsyncBatch.onRecord(listener, record, index);
		}

		@Override
		public void setInDoubt() {
			if (! parent.exchangeSent(index)) {
				BatchRecord record = new BatchRecord(key, null, ResultCode.NO_RESPONSE, true, true);
				AsyncBatch.onRecord(listener, record, index);
			}
		}
	}

	public static final class DeleteSequence extends Delete {
		private final BatchRecordSequenceListener listener;
		private final int index;

		public DeleteSequence(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			Node node,
			BatchRecordSequenceListener listener,
			int index
		) {
			super(executor, cluster, policy, attr, record, node);
			this.listener = listener;
			this.index = index;
		}

		@Override
		protected void parseResult(RecordParser rp) {
			super.parseResult(rp);
			AsyncBatch.onRecord(listener, record, index);
		}

		// setInDoubt() is not overridden to call onRecord() because user already has access to full
		// BatchRecord list and can examine each record for inDoubt when the exception occurs.
	}

	public static class Delete extends AsyncBaseCommand {
		private final BatchAttr attr;
		final BatchRecord record;

		public Delete(
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
		protected void parseResult(RecordParser rp) {
			if (rp.resultCode == 0) {
				record.setRecord(new Record(null, rp.generation, rp.expiration));
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				executor.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public static class TxnVerify extends AsyncBaseCommand {
		private final long version;
		private final BatchRecord record;

		public TxnVerify(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			long version,
			BatchRecord record,
			Node node
		) {
			super(executor, cluster, policy, record.key, node, false);
			this.version = version;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setTxnVerify(record.key, version);
		}

		@Override
		protected boolean parseResult() {
			RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);

			if (rp.resultCode == ResultCode.OK) {
				record.resultCode = rp.resultCode;
			}
			else {
				record.setError(rp.resultCode, false);
				executor.setRowError();
			}
			return true;
		}

		@Override
		protected void parseResult(RecordParser rp) {
		}
	}

	public static class TxnRoll extends AsyncBaseCommand {
		private final Txn txn;
		private final BatchRecord record;
		private final int attr;

		public TxnRoll(
			AsyncBatchExecutor executor,
			Cluster cluster,
			BatchPolicy policy,
			Txn txn,
			BatchRecord record,
			Node node,
			int attr
		) {
			super(executor, cluster, policy, record.key, node, true);
			this.txn = txn;
			this.record = record;
			this.attr = attr;
		}

		@Override
		protected void writeBuffer() {
			setTxnRoll(record.key, txn, attr);
		}

		@Override
		protected boolean parseResult() {
			RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);

			if (rp.resultCode == ResultCode.OK) {
				record.resultCode = rp.resultCode;
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				executor.setRowError();
			}
			return true;
		}

		@Override
		protected void parseResult(RecordParser rp) {
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
			super(policy, true, key.namespace);
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
		void addSubException(AerospikeException ae) {
			executor.addSubException(ae);
		}

		@Override
		protected boolean parseResult() {
			RecordParser rp = new RecordParser(dataBuffer, dataOffset, receiveSize);
			rp.parseFields(policy.txn, key, hasWrite);
			parseResult(rp);
			return true;
		}

		protected abstract void parseResult(RecordParser rp);

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
