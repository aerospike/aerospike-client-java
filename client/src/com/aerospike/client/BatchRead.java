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
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.Policy;

/**
 * Batch key and read only operations with default policy.
 * Used in batch read commands where different bins are needed for each key.
 */
public final class BatchRead extends BatchRecord {
	/**
	 * Optional read policy.
	 */
	public final BatchReadPolicy policy;

	/**
	 * Bins to retrieve for this key. binNames are mutually exclusive with
	 * {@link com.aerospike.client.BatchRead#ops}.
	 */
	public final String[] binNames;

	/**
	 * Optional operations for this key. ops are mutually exclusive with
	 * {@link com.aerospike.client.BatchRead#binNames}. A binName can be emulated with
	 * {@link com.aerospike.client.Operation#get(String)}
	 */
	public final Operation[] ops;

	/**
	 * If true, ignore binNames and read all bins.
	 * If false and binNames are set, read specified binNames.
	 * If false and binNames are not set, read record header (generation, expiration) only.
	 */
	public final boolean readAllBins;

	/**
	 * Initialize batch key and bins to retrieve.
	 */
	public BatchRead(Key key, String[] binNames) {
		super(key, false);
		this.policy = null;
		this.binNames = binNames;
		this.ops = null;
		this.readAllBins = false;
	}

	/**
	 * Initialize batch key and readAllBins indicator.
	 */
	public BatchRead(Key key, boolean readAllBins) {
		super(key, false);
		this.policy = null;
		this.binNames = null;
		this.ops = null;
		this.readAllBins = readAllBins;
	}

	/**
	 * Initialize batch key and read operations.
	 */
	public BatchRead(Key key, Operation[] ops) {
		super(key, false);
		this.policy = null;
		this.binNames = null;
		this.ops = ops;
		this.readAllBins = false;
	}

	/**
	 * Initialize batch policy, key and bins to retrieve.
	 */
	public BatchRead(BatchReadPolicy policy, Key key, String[] binNames) {
		super(key, false);
		this.policy = policy;
		this.binNames = binNames;
		this.ops = null;
		this.readAllBins = false;
	}

	/**
	 * Initialize batch policy, key and readAllBins indicator.
	 */
	public BatchRead(BatchReadPolicy policy, Key key, boolean readAllBins) {
		super(key, false);
		this.policy = policy;
		this.binNames = null;
		this.ops = null;
		this.readAllBins = readAllBins;
	}

	/**
	 * Initialize batch policy, key and read operations.
	 */
	public BatchRead(BatchReadPolicy policy, Key key, Operation[] ops) {
		super(key, false);
		this.policy = policy;
		this.binNames = null;
		this.ops = ops;
		this.readAllBins = false;
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_READ;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	@Override
	public boolean equals(BatchRecord obj, ConfigurationProvider configProvider) {
		if (getClass() != obj.getClass())
			return false;

		BatchRead other = (BatchRead)obj;
		return binNames == other.binNames && ops == other.ops && policy == other.policy && readAllBins == other.readAllBins;
	}

	/**
	 * Return wire protocol size. For internal use only.
	 */
	@Override
	public int size(Policy parentPolicy, ConfigurationProvider configProvider) {
		int size = 0;

		if (policy != null) {
			if (policy.filterExp != null) {
				size += policy.filterExp.size();
			}
		}

		if (binNames != null) {
			for (String binName : binNames) {
				size += Buffer.estimateSizeUtf8(binName) + Command.OPERATION_HEADER_SIZE;
			}
		}
		else if (ops != null) {
			for (Operation op : ops) {
				if (op.type.isWrite) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
				}
				size += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
				size += op.value.estimateSize();
			}
		}
		return size;
	}
}
