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
