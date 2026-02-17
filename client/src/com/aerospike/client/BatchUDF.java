/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Packer;

/**
 * Batch user defined functions.
 */
public final class BatchUDF extends BatchRecord {
	/**
	 * Optional UDF policy.
	 */
	public final BatchUDFPolicy policy;

	/**
	 * Package or lua module name.
	 */
	public final String packageName;

	/**
	 * Lua function name.
	 */
	public final String functionName;

	/**
	 * Optional arguments to lua function.
	 */
	public final Value[] functionArgs;

	/**
	 * Wire protocol bytes for function args. For internal use only.
	 */
	public byte[] argBytes;

	/**
	 * Constructor using default policy.
	 */
	public BatchUDF(Key key, String packageName, String functionName, Value[] functionArgs) {
		super(key, true);
		this.policy = null;
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
		// Do not set argBytes here because may not be necessary if batch repeat flag is used.
	}

	/**
	 * Constructor using specified policy.
	 */
	public BatchUDF(BatchUDFPolicy policy, Key key, String packageName, String functionName, Value[] functionArgs) {
		super(key, true);
		this.policy = policy;
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_UDF;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	@Override
	public boolean equals(BatchRecord obj, ConfigurationProvider configProvider) {
		if (getClass() != obj.getClass())
			return false;

		BatchUDF other = (BatchUDF)obj;

		if (functionName != other.functionName || functionArgs != other.functionArgs ||
				packageName != other.packageName || policy != other.policy) {
			return false;
		}

		boolean sendkey = false;
		if (policy != null) {
			sendkey = policy.sendKey;
		}
		if (configProvider != null) {
			Configuration config = configProvider.fetchConfiguration();
			if (config != null && config.hasDBUDFCsendKey()) {
				sendkey = config.dynamicConfiguration.dynamicBatchUDFconfig.sendKey.value;
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
				if (config != null && config.hasDBUDFCsendKey()) {
					sendkey = config.dynamicConfiguration.dynamicBatchUDFconfig.sendKey.value;
				}
			}

			if (sendkey || parentPolicy.sendKey) {
				size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
			}
		}
		else if (parentPolicy.sendKey) {
			size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
		}

		size += Buffer.estimateSizeUtf8(packageName) + Command.FIELD_HEADER_SIZE;
		size += Buffer.estimateSizeUtf8(functionName) + Command.FIELD_HEADER_SIZE;
		argBytes = Packer.pack(functionArgs);
		size += argBytes.length + Command.FIELD_HEADER_SIZE;
		return size;
	}
}
