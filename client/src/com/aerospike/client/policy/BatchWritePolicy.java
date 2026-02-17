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
package com.aerospike.client.policy;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchWriteConfig;
import com.aerospike.client.exp.Expression;

/**
 * Policy attributes used in batch write commands.
 */
public final class BatchWritePolicy {
	/**
	 * Optional expression filter. If filterExp exists and evaluates to false, the specific batch key
	 * request is not performed and {@link com.aerospike.client.BatchRecord#resultCode} is set to
	 * {@link com.aerospike.client.ResultCode#FILTERED_OUT}.
	 * <p>
	 * If exists, this filter overrides the batch parent filter {@link com.aerospike.client.policy.Policy#filterExp}
	 * for the specific key in batch commands that allow a different policy per key.
	 * Otherwise, this filter is ignored.
	 * <p>
	 * Default: null
	 */
	public Expression filterExp;

	/**
	 * Qualify how to handle writes where the record already exists.
	 * <p>
	 * Default: RecordExistsAction.UPDATE
	 */
	public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

	/**
	 * Desired consistency guarantee when committing a command on the server. The default
	 * (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	 * be successful before returning success to the client.
	 * <p>
	 * Default: CommitLevel.COMMIT_ALL
	 */
	public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

	/**
	 * Qualify how to handle record writes based on record generation. The default (NONE)
	 * indicates that the generation is not used to restrict writes.
	 * <p>
	 * The server does not support this field for UDF execute() calls. The read-modify-write
	 * usage model can still be enforced inside the UDF code itself.
	 * <p>
	 * Default: GenerationPolicy.NONE
	 */
	public GenerationPolicy generationPolicy = GenerationPolicy.NONE;

	/**
	 * Expected generation. Generation is the number of times a record has been modified
	 * (including creation) on the server. If a write operation is creating a record,
	 * the expected generation would be <code>0</code>. This field is only relevant when
	 * generationPolicy is not NONE.
	 * <p>
	 * The server does not support this field for UDF execute() calls. The read-modify-write
	 * usage model can still be enforced inside the UDF code itself.
	 * <p>
	 * Default: 0
	 */
	public int generation;

	/**
	 * Record expiration. Also known as ttl (time to live).
	 * Seconds record will live before being removed by the server.
	 * <p>
	 * Expiration values:
	 * <ul>
	 * <li>-2: Do not change ttl when record is updated.</li>
	 * <li>-1: Never expire.</li>
	 * <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
	 * <li>&gt; 0: Actual ttl in seconds.<br></li>
	 * </ul>
	 * <p>
	 * Default: 0
	 */
	public int expiration;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition only.
	 * <p>
	 * Default: false (do not tombstone deleted records).
	 */
	public boolean durableDelete;

	/**
	 * Execute the write command only if the record is not already locked by this transaction.
	 * If this field is true and the record is already locked by this transaction, the command
	 * will throw an exception with the {@link com.aerospike.client.ResultCode#MRT_ALREADY_LOCKED}
	 * error code.
	 * <p>
	 * This field is useful for safely retrying non-idempotent writes as an alternative to simply
	 * aborting the transaction.
	 * <p>
	 * Default: false.
	 */
	public boolean onLockingOnly;

	/**
	 * Send user defined key in addition to hash digest.
	 * If true, the key will be stored with the record on the server.
	 * <p>
	 * Default: false (do not send the user defined key)
	 */
	public boolean sendKey;

	/**
	 * Copy policy from another policy AND override certain policy attributes if they exist in the configProvider.
	 * Any policy overrides will not get logged.
	 */
	public BatchWritePolicy(BatchWritePolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy policy from another policy AND override certain policy attributes if they exist in the configProvider.
	 * Any default policy overrides will get logged.
	 */
	public BatchWritePolicy(BatchWritePolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

	/**
	 * Copy constructor.
	 */
	public BatchWritePolicy(BatchWritePolicy other) {
		this.filterExp = other.filterExp;
		this.recordExistsAction = other.recordExistsAction;
		this.commitLevel = other.commitLevel;
		this.generationPolicy = other.generationPolicy;
		this.generation = other.generation;
		this.expiration = other.expiration;
		this.durableDelete = other.durableDelete;
		this.onLockingOnly = other.onLockingOnly;
		this.sendKey = other.sendKey;
	}

	/**
	 * Default constructor.
	 */
	public BatchWritePolicy() {
	}

	private void updateFromConfig(ConfigurationProvider configProvider, boolean log) {
		boolean logUpdate = false;
		if (configProvider == null) {
			return;
		}
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		DynamicConfiguration dConfig = config.getDynamicConfiguration();
		if (dConfig == null) {
			return;
		}
		DynamicBatchWriteConfig dynBWC = dConfig.getDynamicBatchWriteConfig();
		if (dynBWC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynBWC.sendKey != null && this.sendKey != dynBWC.sendKey.value) {
			this.sendKey = dynBWC.sendKey.value;
			if (logUpdate) {
				Log.info("Set BatchWritePolicy.sendKey = " + this.sendKey);
			}
		}
		if (dynBWC.durableDelete != null && this.durableDelete != dynBWC.durableDelete.value) {
			this.durableDelete = dynBWC.durableDelete.value;
			if (logUpdate) {
				Log.info("Set BatchWritePolicy.durableDelete = " + this.durableDelete);
			}
		}
	}


	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setFilterExp(Expression filterExp) {
		this.filterExp = filterExp;
	}

	public void setRecordExistsAction(RecordExistsAction recordExistsAction) {
		this.recordExistsAction = recordExistsAction;
	}

	public void setCommitLevel(CommitLevel commitLevel) {
		this.commitLevel = commitLevel;
	}

	public void setGenerationPolicy(GenerationPolicy generationPolicy) {
		this.generationPolicy = generationPolicy;
	}

	public void setGeneration(int generation) {
		this.generation = generation;
	}

	public void setExpiration(int expiration) {
		this.expiration = expiration;
	}

	public void setDurableDelete(boolean durableDelete) {
		this.durableDelete = durableDelete;
	}

	public void setOnLockingOnly(boolean onLockingOnly) {
		this.onLockingOnly = onLockingOnly;
	}

	public void setSendKey(boolean sendKey) {
		this.sendKey = sendKey;
	}
}
