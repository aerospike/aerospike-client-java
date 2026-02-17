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

import java.util.Objects;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicWriteConfig;

/**
 * Container object for policy attributes used in write operations.
 * This object is passed into methods where database writes can occur.
 */
public final class WritePolicy extends Policy {
	/**
	 * Qualify how to handle writes where the record already exists.
	 * <p>
	 * Default: RecordExistsAction.UPDATE
	 */
	public RecordExistsAction recordExistsAction = RecordExistsAction.UPDATE;

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
	 * Desired consistency guarantee when committing a command on the server. The default
	 * (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
	 * be successful before returning success to the client.
	 * <p>
	 * Default: CommitLevel.COMMIT_ALL
	 */
	public CommitLevel commitLevel = CommitLevel.COMMIT_ALL;

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
	 * For client operate(), return a result for every operation.
	 * <p>
	 * Some operations do not return results by default (ListOperation.clear() for example).
	 * This can make it difficult to determine the desired result offset in the returned
	 * bin's result list.
	 * <p>
	 * Setting respondAllOps to true makes it easier to identify the desired result offset
	 * (result offset equals bin's operate sequence).  If there is a map operation in operate(),
	 * respondAllOps will be forced to true for that operate() call.
	 * <p>
	 * Default: false
	 */
	public boolean respondAllOps;

	/**
	 * If the command results in a record deletion, leave a tombstone for the record.
	 * This prevents deleted records from reappearing after node failures.
	 * Valid for Aerospike Server Enterprise Edition 3.10+ only.
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
	 * aborting the transaction. This field is not applicable to record delete commands.
	 * <p>
	 * Default: false.
	 */
	public boolean onLockingOnly;

	/**
	 * Operate in XDR mode.  Some external connectors may need to emulate an XDR client.
	 * If enabled, an XDR bit is set for writes in the wire protocol.
	 * <p>
	 * Default: false.
	 */
	public boolean xdr;

	/**
	 * Copy write policy from another write policy AND override certain policy attributes if they exist in the
	 * configProvider. Any policy overrides will not get logged.
	 */
	public WritePolicy(WritePolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false, "");
	}

	/**
	 * Copy write policy from another write policy AND override certain policy attributes if they exist in the
	 * configProvider. Any default policy overrides will get logged.
	 */
	public WritePolicy(WritePolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy,
					   String preText) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy, preText);
	}

	/**
	 * Copy write policy from another write policy.
	 */
	public WritePolicy(WritePolicy other) {
		super(other);
		this.recordExistsAction = other.recordExistsAction;
		this.generationPolicy = other.generationPolicy;
		this.commitLevel = other.commitLevel;
		this.generation = other.generation;
		this.expiration = other.expiration;
		this.respondAllOps = other.respondAllOps;
		this.durableDelete = other.durableDelete;
		this.onLockingOnly = other.onLockingOnly;
		this.xdr = other.xdr;
	}

	/**
	 * Copy write policy from another policy.
	 */
	public WritePolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public WritePolicy() {
		// Writes are not retried by default.
		super.maxRetries = 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(commitLevel, durableDelete, expiration, onLockingOnly, generation,
				generationPolicy, recordExistsAction, respondAllOps, xdr);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		WritePolicy other = (WritePolicy) obj;
		return commitLevel == other.commitLevel && durableDelete == other.durableDelete
				&& expiration == other.expiration && onLockingOnly == other.onLockingOnly
				&& generation == other.generation && generationPolicy == other.generationPolicy
				&& recordExistsAction == other.recordExistsAction && respondAllOps == other.respondAllOps
				&& xdr == other.xdr;
	}

	private void updateFromConfig(ConfigurationProvider configProvider, boolean log, String preText) {
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
		DynamicWriteConfig dynWC = dConfig.getDynamicWriteConfig();
		if (dynWC == null) {
			return;
		}
		if (!Objects.equals(preText, "")) {
			preText = " " + preText;
		}
		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynWC.connectTimeout != null && this.connectTimeout != dynWC.connectTimeout.value) {
			this.connectTimeout = dynWC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynWC.failOnFilteredOut != null && this.failOnFilteredOut != dynWC.failOnFilteredOut.value) {
			this.failOnFilteredOut = dynWC.failOnFilteredOut.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.failOnFilteredOut = " + this.failOnFilteredOut);
			}
		}
		if (dynWC.replica != null && this.replica != dynWC.replica) {
			this.replica = dynWC.replica;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.replica = " + this.replica);
			}
		}
		if (dynWC.sendKey != null && this.sendKey != dynWC.sendKey.value) {
			this.sendKey = dynWC.sendKey.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.sendKey = " + this.sendKey);
			}
		}
		if (dynWC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynWC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynWC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynWC.sleepMultiplier != null && this.sleepMultiplier != dynWC.sleepMultiplier.value) {
			this.sleepMultiplier = dynWC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynWC.socketTimeout != null && this.socketTimeout != dynWC.socketTimeout.value) {
			this.socketTimeout = dynWC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynWC.timeoutDelay != null && this.timeoutDelay != dynWC.timeoutDelay.value) {
			this.timeoutDelay = dynWC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynWC.totalTimeout != null && this.totalTimeout != dynWC.totalTimeout.value) {
			this.totalTimeout = dynWC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynWC.maxRetries != null && this.maxRetries != dynWC.maxRetries.value) {
			this.maxRetries = dynWC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynWC.durableDelete != null && this.durableDelete != dynWC.durableDelete.value) {
			this.durableDelete = dynWC.durableDelete.value;
			if (logUpdate) {
				Log.info("Set" + preText + " WritePolicy.durableDelete = " + this.durableDelete);
			}
		}
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setRecordExistsAction(RecordExistsAction recordExistsAction) {
		this.recordExistsAction = recordExistsAction;
	}

	public void setGenerationPolicy(GenerationPolicy generationPolicy) {
		this.generationPolicy = generationPolicy;
	}

	public void setCommitLevel(CommitLevel commitLevel) {
		this.commitLevel = commitLevel;
	}

	public void setGeneration(int generation) {
		this.generation = generation;
	}

	public void setExpiration(int expiration) {
		this.expiration = expiration;
	}

	public void setRespondAllOps(boolean respondAllOps) {
		this.respondAllOps = respondAllOps;
	}

	public void setDurableDelete(boolean durableDelete) {
		this.durableDelete = durableDelete;
	}

	public void setOnLockingOnly(boolean onLockingOnly) {
		this.onLockingOnly = onLockingOnly;
	}

	public void setXdr(boolean xdr) {
		this.xdr = xdr;
	}
}
