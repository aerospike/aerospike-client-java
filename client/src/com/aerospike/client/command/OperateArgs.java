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

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;

public final class OperateArgs {
	public final WritePolicy writePolicy;
	public final Operation[] operations;
	public final int size;
	public final int readAttr;
	public final int writeAttr;
	public final boolean hasWrite;

	public OperateArgs(
		WritePolicy policy,
		WritePolicy writeDefault,
		WritePolicy readDefault,
		Operation[] operations
	) {
		this.operations = operations;

		int dataOffset = 0;
		int rattr = 0;
		int wattr = 0;
		boolean write = false;
		boolean readBin = false;
		boolean readHeader = false;
		boolean respondAllOps = false;

		for (Operation operation : operations) {
			switch (operation.type) {
			case BIT_READ:
			case EXP_READ:
			case HLL_READ:
			case MAP_READ:
				// Map operations require respondAllOps to be true.
				respondAllOps = true;
				// Fall through to read.
			case CDT_READ:
			case READ:
				rattr |= Command.INFO1_READ;

				// Read all bins if no bin is specified.
				if (operation.binName == null) {
					rattr |= Command.INFO1_GET_ALL;
				}
				readBin = true;
				break;

			case READ_HEADER:
				rattr |= Command.INFO1_READ;
				readHeader = true;
				break;

			case BIT_MODIFY:
			case EXP_MODIFY:
			case HLL_MODIFY:
			case MAP_MODIFY:
				// Map operations require respondAllOps to be true.
				respondAllOps = true;
				// Fall through to write.
			default:
				wattr = Command.INFO2_WRITE;
				write = true;
				break;
			}
			dataOffset += Buffer.estimateSizeUtf8(operation.binName) + Command.OPERATION_HEADER_SIZE;
			dataOffset += operation.value.estimateSize();
		}
		size = dataOffset;
		hasWrite = write;

		if (readHeader && ! readBin) {
			rattr |= Command.INFO1_NOBINDATA;
		}
		readAttr = rattr;

		if (policy == null) {
			if (write) {
				writePolicy = writeDefault;
			}
			else {
				writePolicy = readDefault;
			}
		}
		else {
			writePolicy = policy;
		}

		// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
		if ((respondAllOps || writePolicy.respondAllOps) && (rattr & Command.INFO1_GET_ALL) == 0) {
			wattr |= Command.INFO2_RESPOND_ALL_OPS;
		}
		writeAttr = wattr;
	}

	public Partition getPartition(Cluster cluster, Key key) {
		if (hasWrite) {
			return Partition.write(cluster, writePolicy, key);
		}
		else {
			return Partition.read(cluster, writePolicy, key);
		}
	}

	public Partition getPartition(Partitions partitions, BatchPolicy policy, Key key) {
		if (hasWrite) {
			return Partition.write(partitions, policy, key);
		}
		else {
			return Partition.read(partitions, policy, key);
		}
	}
}
