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

package com.aerospike.client.proxy;

import java.util.Collections;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.proxy.client.Kvs;

import io.grpc.MethodDescriptor;

/**
 * Base class for Scan and Query.
 */
abstract class ScanQueryBaseCommandProxy extends MultiCommandProxy {
	private final RecordSequenceListener listener;
	private final PartitionTracker partitionTracker;
	protected final PartitionTracker.NodePartitions dummyNodePartitions;
	private final boolean isScan;

	public ScanQueryBaseCommandProxy(boolean isScan, MethodDescriptor<Kvs.AerospikeRequestPayload, Kvs.AerospikeResponsePayload> methodDescriptor, GrpcCallExecutor executor, Policy policy, RecordSequenceListener listener,
									 PartitionTracker partitionTracker) {
		super(methodDescriptor, executor, policy);
		this.isScan = isScan;
		this.listener = listener;
		this.partitionTracker = partitionTracker;
		this.dummyNodePartitions = new PartitionTracker.NodePartitions(null, Node.PARTITIONS);
	}

	@Override
	protected void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	void parseResult(Parser parser) {
		RecordProxy recordProxy = parseRecordResult(parser, false, true, !isScan);

		if ((parser.info3 & Command.INFO3_PARTITION_DONE) != 0) {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partitionId.
			if (partitionTracker != null && recordProxy.resultCode != ResultCode.OK) {
				partitionTracker.partitionUnavailable(dummyNodePartitions, parser.generation);
			}
			return;
		}

		if (recordProxy.resultCode == ResultCode.OK && !super.hasNext) {
			if (partitionTracker != null && !partitionTracker.isComplete(false, policy, Collections.singletonList(dummyNodePartitions))) {
				retry();
				return;
			}

			// This is the end of scan marker record.
			listener.onSuccess();
			return;
		}

		if (recordProxy.resultCode != ResultCode.OK) {
			throw new AerospikeException(recordProxy.resultCode);
		}

		listener.onRecord(recordProxy.key, recordProxy.record);

		if (partitionTracker != null) {
			if (isScan) {
				partitionTracker.setDigest(dummyNodePartitions, recordProxy.key);
			}
			else {
				partitionTracker.setLast(dummyNodePartitions, recordProxy.key,
					recordProxy.bVal.val);
			}
		}
	}

	@Override
	void onFailure(AerospikeException ae) {
		if (partitionTracker != null && partitionTracker.shouldRetry(dummyNodePartitions, ae)) {
			if (retry()) {
				return;
			}

			// Retry failed. Notify the listener.
		}
		listener.onFailure(ae);
	}
}
