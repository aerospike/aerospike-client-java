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
package com.aerospike.client;

import com.aerospike.client.command.Command;
import com.aerospike.client.configuration.*;
import com.aerospike.client.configuration.serializers.*;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.Policy;

/**
 * Batch delete operation.
 */
public final class BatchDelete extends BatchRecord {
	/**
	 * Optional delete policy.
	 */
	public final BatchDeletePolicy policy;

	/**
	 * Initialize key.
	 */
	public BatchDelete(Key key) {
		super(key, true);
		this.policy = null;
	}

	/**
	 * Initialize policy and key.
	 */
	public BatchDelete(BatchDeletePolicy policy, Key key) {
		super(key, true);
		this.policy = policy;
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_DELETE;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	@Override
	public boolean equals(BatchRecord obj, ConfigurationProvider configProvider) {
		if (getClass() != obj.getClass())
			return false;

		BatchDelete other = (BatchDelete)obj;
		if (policy != other.policy) {
			return false;
		}

		boolean sendkey = false;
		if (policy != null) {
			sendkey = policy.sendKey;
		}
		if (configProvider != null) {
			Configuration config = configProvider.fetchConfiguration();
			if (config != null && config.hasDBDCsendKey()) {
				sendkey = config.dynamicConfiguration.dynamicBatchDeleteConfig.sendKey.value;
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
				if (config != null && config.hasDBDCsendKey()) {
					sendkey = config.dynamicConfiguration.dynamicBatchDeleteConfig.sendKey.value;
				}
			}

			if (sendkey || parentPolicy.sendKey) {
				size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
			}
		}
		else if (parentPolicy.sendKey) {
			size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
		}
		return size;
	}
}
