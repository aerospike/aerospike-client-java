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
package com.aerospike.client;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.configuration.*;
import com.aerospike.client.configuration.serializers.*;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.Policy;

/**
 * Batch key and read/write operations with write policy.
 */
public final class BatchWrite extends BatchRecord {
	/**
	 * Optional write policy.
	 */
	public final BatchWritePolicy policy;

	/**
	 * Required operations for this key.
	 */
	public final Operation[] ops;

	/**
	 * Initialize batch key and read/write operations.
	 * <p>
	 * {@link Operation#get()} is not allowed because it returns a variable number of bins and
	 * makes it difficult (sometimes impossible) to lineup operations with results. Instead,
	 * use {@link Operation#get(String)} for each bin name.
	 */
	public BatchWrite(Key key, Operation[] ops) {
		super(key, true);
		this.ops = ops;
		this.policy = null;
	}

	/**
	 * Initialize policy, batch key and read/write operations.
	 * <p>
	 * {@link Operation#get()} is not allowed because it returns a variable number of bins and
	 * makes it difficult (sometimes impossible) to lineup operations with results. Instead,
	 * use {@link Operation#get(String)} for each bin name.
	 */
	public BatchWrite(BatchWritePolicy policy, Key key, Operation[] ops) {
		super(key, true);
		this.ops = ops;
		this.policy = policy;
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_WRITE;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	@Override
	public boolean equals(BatchRecord obj, ConfigurationProvider configProvider) {
		if (getClass() != obj.getClass())
			return false;

		BatchWrite other = (BatchWrite)obj;
		if (ops != other.ops || policy != other.policy) {
			return false;
		}

		boolean sendkey = false;
		if (policy != null) {
			sendkey = policy.sendKey;
		}
		if (configProvider != null) {
			Configuration config = configProvider.fetchConfiguration();
			if (config != null && config.hasDBWCsendKey()) {
				sendkey = config.dynamicConfiguration.dynamicBatchWriteConfig.sendKey.value;
			}
		}

		return !sendkey;

	}

	/**
	 * Return wire protocol size. For internal use only.
	 */
	@Override
	public int size(Policy parentPolicy, ConfigurationProvider configProvider) {
		int size = 2; // gen(2) = 2

		if (policy != null) {
			if (policy.filterExp != null) {
				size += policy.filterExp.size();
			}

			boolean sendkey;
			sendkey = policy.sendKey;
			if (configProvider != null) {
				Configuration config = configProvider.fetchConfiguration();
				if (config != null && config.hasDBWCsendKey()) {
					sendkey = config.dynamicConfiguration.dynamicBatchWriteConfig.sendKey.value;
				}
			}

			if (sendkey || parentPolicy.sendKey) {
				size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
			}
		}
		else if (parentPolicy.sendKey) {
			size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
		}

		boolean hasWrite = false;

		for (Operation op : ops) {
			if (op.type.isWrite) {
				hasWrite = true;
			}
			size += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
			size += op.value.estimateSize();
		}

		if (! hasWrite) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Batch write operations do not contain a write");
		}
		return size;
	}
}
