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
package com.aerospike.client.command;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public final class BatchSingle {
	public static final class Delete extends BaseCommand {
		private final WritePolicy writePolicy;
		private final Key key;
		private final Partition partition;
		private final BatchRecord record;

		public Delete(Cluster cluster, WritePolicy writePolicy, Key key, BatchRecord record, BatchStatus status) {
			super(cluster, writePolicy, status);
			this.writePolicy = writePolicy;
			this.key = key;
			this.record = record;
			this.partition = Partition.write(cluster, writePolicy, key);
		}

		@Override
		protected boolean isWrite() {
			return true;
		}

		@Override
		protected Node getNode() {
			return partition.getNodeWrite(cluster);
		}

		@Override
		protected void writeBuffer() {
			setDelete(writePolicy, key);
		}

		@Override
		protected void parseResult(Connection conn) throws IOException {
			// Read header.
			conn.readFully(dataBuffer, Command.MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
			conn.updateLastUsed();

			int resultCode = dataBuffer[13] & 0xFF;
			int generation = Buffer.bytesToInt(dataBuffer, 14);
			int expiration = Buffer.bytesToInt(dataBuffer, 18);

			if (resultCode == ResultCode.OK || resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				record.setRecord(new Record(null, generation, expiration));
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			partition.prepareRetryWrite(timeout);
			return true;
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static final class Exists extends BaseCommand {
		private final Key key;
		private final Partition partition;
		private final boolean[] existsArray;
		private final int index;

		public Exists(Cluster cluster, Policy policy, Key key, boolean[] existsArray, int index, BatchStatus status) {
			super(cluster, policy, status);
			this.key = key;
			this.existsArray = existsArray;
			this.index = index;
			this.partition = Partition.read(cluster, policy, key);
		}

		@Override
		protected Node getNode() {
			return partition.getNodeRead(cluster);
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected void parseResult(Connection conn) throws IOException {
			// Read header.
			conn.readFully(dataBuffer, Command.MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
			conn.updateLastUsed();

			int resultCode = dataBuffer[13] & 0xFF;
			existsArray[index] = resultCode == 0;
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			partition.prepareRetryRead(timeout);
			return true;
		}
	}

	public static final class ReadHeader extends BaseCommand {
		private final Key key;
		private final Partition partition;
		private final Record[] records;
		private final int index;

		public ReadHeader(Cluster cluster, Policy policy, Key key, Record[] records, int index, BatchStatus status) {
			super(cluster, policy, status);
			this.key = key;
			this.records = records;
			this.index = index;
			this.partition = Partition.read(cluster, policy, key);
		}

		@Override
		protected Node getNode() {
			return partition.getNodeRead(cluster);
		}

		@Override
		protected void writeBuffer() {
			setReadHeader(policy, key);
		}

		@Override
		protected void parseResult(Connection conn) throws IOException {
			// Read header.
			conn.readFully(dataBuffer, Command.MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
			conn.updateLastUsed();

			int resultCode = dataBuffer[13] & 0xFF;

			if (resultCode == 0) {
				int generation = Buffer.bytesToInt(dataBuffer, 14);
				int expiration = Buffer.bytesToInt(dataBuffer, 18);
				records[index] = new Record(null, generation, expiration);
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			partition.prepareRetryRead(timeout);
			return true;
		}
	}

	public static class Read extends BaseCommand {
		protected final Key key;
		protected final Partition partition;
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
			BatchStatus status
		) {
			super(cluster, policy, status);
			this.key = key;
			this.binNames = binNames;
			this.records = records;
			this.index = index;
			this.isOperation = false;
			this.partition = Partition.read(cluster, policy, key);
		}

		public Read(
			Cluster cluster,
			Policy policy,
			Key key,
			Partition partition,
			boolean isOperation,
			Record[] records,
			int index,
			BatchStatus status
		) {
			super(cluster, policy, status);
			this.key = key;
			this.partition = partition;
			this.binNames = null;
			this.records = records;
			this.index = index;
			this.isOperation = isOperation;
		}

		@Override
		protected Node getNode() {
			return partition.getNodeRead(cluster);
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, binNames);
		}

		@Override
		protected void parseResult(Connection conn) throws IOException {
			// Read header.
			conn.readFully(dataBuffer, 8, Command.STATE_READ_HEADER);

			long sz = Buffer.bytesToLong(dataBuffer, 0);
			int receiveSize = (int)(sz & 0xFFFFFFFFFFFFL);

			if (receiveSize <= 0) {
				throw new AerospikeException("Invalid receive size: " + receiveSize);
			}

			// Read remaining message bytes.
			sizeBuffer(receiveSize);
			conn.readFully(dataBuffer, receiveSize, Command.STATE_READ_DETAIL);
			conn.updateLastUsed();

			long type = (sz >> 48) & 0xff;

			if (type == Command.AS_MSG_TYPE) {
				dataOffset = 5;
			}
			else if (type == Command.MSG_TYPE_COMPRESSED) {
				int usize = (int)Buffer.bytesToLong(dataBuffer, 0);
				byte[] buf = new byte[usize];

				Inflater inf = new Inflater();
				try {
					inf.setInput(dataBuffer, 8, receiveSize - 8);
					int rsize;

					try {
						rsize = inf.inflate(buf);
					}
					catch (DataFormatException dfe) {
						throw new AerospikeException.Serialize(dfe);
					}

					if (rsize != usize) {
						throw new AerospikeException("Decompressed size " + rsize + " is not expected " + usize);
					}

					dataBuffer = buf;
					dataOffset = 13;
				} finally {
					inf.end();
				}
			}
			else {
				throw new AerospikeException("Invalid proto type: " + type + " Expected: " + Command.AS_MSG_TYPE);
			}

			int resultCode = dataBuffer[dataOffset] & 0xFF;
			dataOffset++;
			int generation = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;
			int expiration = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 8;
			int fieldCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;
			int opCount = Buffer.bytesToShort(dataBuffer, dataOffset);
			dataOffset += 2;

			if (resultCode == 0) {
				Record record;

				if (opCount <= 0) {
					// Bin data was not returned.
					record = new Record(null, generation, expiration);
				}
				else {
					skipKey(fieldCount);
					record = parseRecord(opCount, generation, expiration, isOperation);
				}
				records[index] = record;
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			partition.prepareRetryRead(timeout);
			return true;
		}
	}

	public static final class OperateRecord extends Read {
		private final OperateArgs args;

		public OperateRecord(
			Cluster cluster,
			Key key,
			OperateArgs args,
			Record[] records,
			int index,
			BatchStatus status
		) {
			super(cluster, args.writePolicy, key, args.getPartition(cluster, key), true, records, index, status);
			this.args = args;
		}

		@Override
		protected boolean isWrite() {
			return args.hasWrite;
		}

		@Override
		protected Node getNode() {
			return args.hasWrite ? partition.getNodeWrite(cluster) : partition.getNodeRead(cluster);
		}

		@Override
		protected void writeBuffer() {
			setOperate(args.writePolicy, key, args);
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (args.hasWrite) {
				partition.prepareRetryWrite(timeout);
			}
			else {
				partition.prepareRetryRead(timeout);
			}
			return true;
		}
	}

	public static final class OperateBatchRecord extends BaseCommand {
		private final Key key;
		private final Partition partition;
		private final OperateArgs args;
		private final BatchRecord record;

		public OperateBatchRecord(
			Cluster cluster,
			Key key,
			OperateArgs args,
			BatchRecord record,
			BatchStatus status
		) {
			super(cluster, args.writePolicy, status);
			this.key = key;
			this.args = args;
			this.record = record;
			this.partition = args.getPartition(cluster, key);
		}

		@Override
		protected boolean isWrite() {
			return args.hasWrite;
		}

		@Override
		protected Node getNode() {
			return args.hasWrite ? partition.getNodeWrite(cluster) : partition.getNodeRead(cluster);
		}

		@Override
		protected void writeBuffer() {
			setOperate(args.writePolicy, key, args);
		}

		@Override
		protected void parseResult(Connection conn) throws IOException {
			// TODO: Add SyncCommand parseRecord() which includes compression support like read.
/*
			// Read header.
			conn.readFully(dataBuffer, Command.MSG_TOTAL_HEADER_SIZE, Command.STATE_READ_HEADER);
			conn.updateLastUsed();

			int resultCode = dataBuffer[13] & 0xFF;

			if (resultCode == ResultCode.OK) {
				int generation = Buffer.bytesToInt(dataBuffer, 14);
				int expiration = Buffer.bytesToInt(dataBuffer, 18);
				int opCount = Buffer.bytesToInt(dataBuffer, 18);
				Record r = parseRecord(opCount, generation, expiration, isOperation);
				record.setRecord(r);
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(args.hasWrite, commandSentCounter));
				status.setRowError();
			}


			int generation = Buffer.bytesToInt(dataBuffer, 14);
			int expiration = Buffer.bytesToInt(dataBuffer, 18);

			if (resultCode == ResultCode.OK || resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				record.setRecord(new Record(null, generation, expiration));
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}*/
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (args.hasWrite) {
				partition.prepareRetryWrite(timeout);
			}
			else {
				partition.prepareRetryRead(timeout);
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

	public static abstract class BaseCommand extends SyncCommand implements IBatchCommand {
		BatchExecutor parent;
		BatchStatus status;

		public BaseCommand(
			Cluster cluster,
			Policy policy,
			BatchStatus status

		) {
			super(cluster, policy);
			this.status = status;
		}

		public void setParent(BatchExecutor parent) {
			this.parent = parent;
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
			catch (RuntimeException re) {
				setInDoubt();
				status.setException(re);
			}
			catch (Throwable e) {
				setInDoubt();
				status.setException(new RuntimeException(e));
			}
			finally {
				parent.onComplete();
			}
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.BATCH;
		}

		public void setInDoubt() {
		}
	}
}
