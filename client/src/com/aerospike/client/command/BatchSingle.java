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

import java.io.IOException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;

public final class BatchSingle {
	public static final class OperateRead extends Read {
		private final Operation[] ops;

		public OperateRead(
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Operation[] ops,
			Record[] records,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, key, null, records, index, status, node, true);
			this.ops = ops;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, ops);
		}
	}

	public static class Read extends BaseCommand {
		protected final Key key;
		private final String[] binNames;
		private final Record[] records;
		private final int index;
		private final boolean isOperation;

		public Read(
			Cluster cluster,
			Policy policy,
			Key key,
			String[] binNames,
			Record[] records,
			int index,
			BatchStatus status,
			Node node,
			boolean isOperation
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
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
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);

			if (rp.resultCode == ResultCode.OK) {
				records[index] = rp.parseRecord(isOperation);
			}
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
		}
	}

	public static final class ReadHeader extends BaseCommand {
		private final Key key;
		private final Record[] records;
		private final int index;

		public ReadHeader(
			Cluster cluster,
			Policy policy,
			Key key,
			Record[] records,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
			this.records = records;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setReadHeader(policy, key);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);

			if (rp.resultCode == 0) {
				records[index] = new Record(null, rp.generation, rp.expiration);
			}
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
		}
	}

	public static class ReadRecord extends BaseCommand {
		private final BatchRead record;

		public ReadRecord(
			Cluster cluster,
			Policy policy,
			BatchRead record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, false);
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, record);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, false);
				status.setRowError();
			}
		}
	}

	public static final class Exists extends BaseCommand {
		private final Key key;
		private final boolean[] existsArray;
		private final int index;

		public Exists(
			Cluster cluster,
			Policy policy,
			Key key,
			boolean[] existsArray,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
			this.existsArray = existsArray;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			existsArray[index] = rp.resultCode == 0;
		}
	}

	public static final class OperateBatchRecord extends BaseCommand {
		private final Operation[] ops;
		private final BatchAttr attr;
		private final BatchRecord record;

		public OperateBatchRecord(
			Cluster cluster,
			BatchPolicy policy,
			Operation[] ops,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, attr.hasWrite);
			this.ops = ops;
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setOperate(policy, attr, record.key, ops);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, record.hasWrite);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static final class Delete extends BaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;

		public Delete(
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, true);
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setDelete(policy, record.key, attr);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, true);

			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(new Record(null, rp.generation, rp.expiration));
			}
			else {
				// A KEY_NOT_FOUND_ERROR on a delete is benign, but still results in an overall
				// batch status of false to be consistent with the original batch code.
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static final class UDF extends BaseCommand {
		private final String packageName;
		private final String functionName;
		private final Value[] args;
		private final BatchAttr attr;
		private final BatchRecord record;

		public UDF(
			Cluster cluster,
			BatchPolicy policy,
			String packageName,
			String functionName,
			Value[] args,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, true);
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setUdf(policy, attr, record.key, packageName, functionName, args);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, true);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(false));
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = rp.resultCode;
					record.inDoubt = Command.batchInDoubt(true, commandSentCounter);
					status.setRowError();
				}
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
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

	public static final class TxnVerify extends BaseCommand {
		private final long version;
		private final BatchRecord record;

		public TxnVerify(
			Cluster cluster,
			BatchPolicy policy,
			long version,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, false);
			this.version = version;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setTxnVerify(record.key, version);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.resultCode = rp.resultCode;
			}
			else {
				record.setError(rp.resultCode, false);
				status.setRowError();
			}
		}
	}

	public static final class TxnRoll extends BaseCommand {
		private final Txn txn;
		private final BatchRecord record;
		private final int attr;

		public TxnRoll(
			Cluster cluster,
			BatchPolicy policy,
			Txn txn,
			BatchRecord record,
			BatchStatus status,
			Node node,
			int attr
		) {
			super(cluster, policy, status, record.key, node, true);
			this.txn = txn;
			this.record = record;
			this.attr = attr;
		}

		@Override
		protected void writeBuffer() {
			setTxnRoll(record.key, txn, attr);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.resultCode = rp.resultCode;
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static abstract class BaseCommand extends SyncCommand implements IBatchCommand {
		BatchStatus status;
		Key key;
		Node node;
		int sequence;
		boolean hasWrite;

		public BaseCommand(
			Cluster cluster,
			Policy policy,
			BatchStatus status,
			Key key,
			Node node,
			boolean hasWrite
		) {
			super(cluster, policy, key.namespace);
			this.status = status;
			this.key = key;
			this.node = node;
			this.hasWrite = hasWrite;
		}

		@Override
		public void run() {
			try {
				execute();
			}
			catch (AerospikeException ae) {
				if (ae.getInDoubt()) {
					setInDoubt();
				}
				status.setException(ae);
			}
			catch (Throwable e) {
				setInDoubt();
				status.setException(new AerospikeException(e));
			}
		}

		@Override
		protected boolean isWrite() {
			return hasWrite;
		}

		@Override
		protected Node getNode() {
			return node;
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.BATCH;
		}

		@Override
		protected void addSubException(AerospikeException ae) {
			status.addSubException(ae);
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

		public void setInDoubt() {
		}
	}
}
