/*
 * Copyright 2012-2026 Aerospike, Inc.
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
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchReadConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchWriteConfig;

import java.util.Objects;

/**
 * Batch parent policy.
 */
public class BatchPolicy extends Policy {
	/**
	 * This field is ignored and deprecated. Sync batch node commands are now always issued using
	 * virtual threads in parallel. Async batch node commands always ignored this field. This field
	 * only exists to maintain api compatibility when switching between aerospike-client-jdk21 and
	 * aerospike-client-jdk8 packages.
	 */
	@Deprecated
	public int maxConcurrentThreads = 1;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for in-memory
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * <p>
	 * For batch commands with smaller sized records (&lt;= 1K per record), inline
	 * processing will be significantly faster on in-memory namespaces.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: true
	 */
	public boolean allowInline = true;

	/**
	 * Allow batch to be processed immediately in the server's receiving thread for SSD
	 * namespaces. If false, the batch will always be processed in separate service threads.
	 * Server versions &lt; 6.0 ignore this field.
	 * <p>
	 * Inline processing can introduce the possibility of unfairness because the server
	 * can process the entire batch before moving onto the next command.
	 * <p>
	 * Default: false
	 */
	public boolean allowInlineSSD = false;

	/**
	 * Should all batch keys be attempted regardless of errors. This field is used on both
	 * the client and server. The client handles node specific errors and the server handles
	 * key specific errors.
	 * <p>
	 * If true, every batch key is attempted regardless of previous key specific errors.
	 * Node specific errors such as timeouts stop keys to that node, but keys directed at
	 * other nodes will continue to be processed.
	 * <p>
	 * If false, the server will stop the batch to its node on most key specific errors.
	 * The exceptions are {@link com.aerospike.client.ResultCode#KEY_NOT_FOUND_ERROR} and
	 * {@link com.aerospike.client.ResultCode#FILTERED_OUT} which never stop the batch.
	 * <p>
	 * Server versions &lt; 6.0 do not support this field and treat this value as false
	 * for key specific errors.
	 * <p>
	 * Default: true
	 */
	public boolean respondAllKeys = true;

	/**
	 * This method is deprecated and will eventually be removed.
	 * The set name is now always sent for every distinct namespace/set in the batch.
	 * <p>
	 * Send set name field to server for every key in the batch for batch index protocol.
	 * This is necessary for batch writes and batch reads when authentication is enabled and
	 * security roles are defined on a per set basis.
	 * <p>
	 * Default: false
	 */
	@Deprecated
	public boolean sendSetName;

	/**
	 * Copy batch policy from another batch policy.
	 */
	public BatchPolicy(BatchPolicy other) {
		super(other);
		this.maxConcurrentThreads = other.maxConcurrentThreads;
		this.allowInline = other.allowInline;
		this.allowInlineSSD = other.allowInlineSSD;
		this.respondAllKeys = other.respondAllKeys;
		this.sendSetName = other.sendSetName;
	}

	/**
	 * Copy batch policy from another policy.
	 */
	public BatchPolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public BatchPolicy() {
	}

	/**
	 * Default batch read policy.
	 */
	public static BatchPolicy ReadDefault() {
		return new BatchPolicy();
	}

	/**
	 * Default batch write policy.
	 */
	public static BatchPolicy WriteDefault() {
		BatchPolicy policy = new BatchPolicy();
		policy.maxRetries = 0;
		return policy;
	}

	/**
	 * Merge batch read policy with dynamic configuration. For internal use only.
	 */
	public static BatchPolicy mergeRead(BatchPolicy src, ConfigurationProvider configProvider) {
		return mergeRead(src, configProvider, false, "");
	}

	/**
	 * Merge batch read policy with dynamic configuration. For internal use only.
	 */
	public static BatchPolicy mergeRead(
		BatchPolicy src,
		ConfigurationProvider configProvider,
		boolean log,
		String preText
	) {
		BatchPolicy trg = new BatchPolicy(src);
		boolean logUpdate = false;

		if (configProvider == null) {
			return trg;
		}

		Configuration config = configProvider.fetchConfiguration();

		if (config == null) {
			return trg;
		}

		DynamicConfiguration dConfig = config.getDynamicConfiguration();

		if (dConfig == null) {
			return trg;
		}

		DynamicBatchReadConfig dyn = dConfig.getDynamicBatchReadConfig();

		if (dyn == null) {
			return trg;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}

		if (!Objects.equals(preText, "")) {
			preText = " " + preText;
		}

		if (dyn.readModeAP != null && trg.readModeAP != dyn.readModeAP) {
			trg.readModeAP = dyn.readModeAP;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.readModeAP = " + trg.readModeAP);
			}
		}
		if (dyn.readModeSC != null && src.readModeSC != dyn.readModeSC) {
			trg.readModeSC = dyn.readModeSC;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.readModeSC = " + trg.readModeSC);
			}
		}
		if (dyn.connectTimeout != null && trg.connectTimeout != dyn.connectTimeout.value) {
			trg.connectTimeout = dyn.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.connectTimeout = " + trg.connectTimeout);
			}
		}
		if (dyn.replica != null && trg.replica != dyn.replica) {
			trg.replica = dyn.replica;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.replica = " + trg.replica);
			}
		}
		if (dyn.sleepBetweenRetries != null && trg.sleepBetweenRetries != dyn.sleepBetweenRetries.value) {
			trg.sleepBetweenRetries = dyn.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepBetweenRetries = " + trg.sleepBetweenRetries);
			}
		}
		if (dyn.sleepMultiplier != null && trg.sleepMultiplier != dyn.sleepMultiplier.value) {
			trg.sleepMultiplier = dyn.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepMultiplier = " + trg.sleepMultiplier);
			}
		}
		if (dyn.socketTimeout != null && trg.socketTimeout != dyn.socketTimeout.value) {
			trg.socketTimeout = dyn.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.socketTimeout = " + trg.socketTimeout);
			}
		}
		if (dyn.timeoutDelay != null && trg.timeoutDelay != dyn.timeoutDelay.value) {
			trg.timeoutDelay = dyn.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.timeoutDelay = " + trg.timeoutDelay);
			}
		}
		if (dyn.totalTimeout != null && trg.totalTimeout != dyn.totalTimeout.value) {
			trg.totalTimeout = dyn.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.totalTimeout = " + trg.totalTimeout);
			}
		}
		if (dyn.maxRetries != null && trg.maxRetries != dyn.maxRetries.value) {
			trg.maxRetries = dyn.maxRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxRetries = " + trg.maxRetries);
			}
		}
		if (dyn.maxConcurrentThreads != null && trg.maxConcurrentThreads != dyn.maxConcurrentThreads.value) {
			trg.maxConcurrentThreads = dyn.maxConcurrentThreads.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxConcurrentThreads = " + trg.maxConcurrentThreads);
			}
		}
		if (dyn.allowInline != null && trg.allowInline != dyn.allowInline.value) {
			trg.allowInline = dyn.allowInline.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInline = " + trg.allowInline);
			}
		}
		if (dyn.allowInlineSSD != null && trg.allowInlineSSD != dyn.allowInlineSSD.value) {
			trg.allowInlineSSD = dyn.allowInlineSSD.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInlineSSD = " + trg.allowInlineSSD);
			}
		}
		if (dyn.respondAllKeys != null && trg.respondAllKeys != dyn.respondAllKeys.value) {
			trg.respondAllKeys = dyn.respondAllKeys.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.respondAllKeys = " + trg.respondAllKeys);
			}
		}
		return trg;
	}

	/**
	 * Merge batch write policy with dynamic configuration. For internal use only.
	 */
	public static BatchPolicy mergeWrite(BatchPolicy src, ConfigurationProvider configProvider) {
		return mergeWrite(src, configProvider, false, "");
	}

	/**
	 * Merge batch write policy with dynamic configuration. For internal use only.
	 */
	public static BatchPolicy mergeWrite(
		BatchPolicy src,
		ConfigurationProvider configProvider,
		boolean log,
		String preText
	) {
		BatchPolicy trg = new BatchPolicy(src);
		boolean logUpdate = false;

		if (configProvider == null) {
			return trg;
		}

		Configuration config = configProvider.fetchConfiguration();

		if (config == null) {
			return trg;
		}

		DynamicConfiguration dConfig = config.getDynamicConfiguration();

		if (dConfig == null) {
			return trg;
		}

		DynamicBatchWriteConfig dyn = dConfig.getDynamicBatchWriteConfig();

		if (dyn == null) {
			return trg;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}

		if (!Objects.equals(preText, "")) {
			preText = " " + preText;
		}

		if (dyn.sendKey != null && trg.sendKey != dyn.sendKey.value) {
			trg.sendKey = dyn.sendKey.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sendKey = " + trg.sendKey);
			}
		}
		if (dyn.connectTimeout != null && trg.connectTimeout != dyn.connectTimeout.value) {
			trg.connectTimeout = dyn.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.connectTimeout = " + trg.connectTimeout);
			}
		}
		if (dyn.replica != null && trg.replica != dyn.replica) {
			trg.replica = dyn.replica;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.replica = " + trg.replica);
			}
		}
		if (dyn.sleepBetweenRetries != null && trg.sleepBetweenRetries != dyn.sleepBetweenRetries.value) {
			trg.sleepBetweenRetries = dyn.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepBetweenRetries = " + trg.sleepBetweenRetries);
			}
		}
		if (dyn.sleepMultiplier != null && trg.sleepMultiplier != dyn.sleepMultiplier.value) {
			trg.sleepMultiplier = dyn.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepMultiplier = " + trg.sleepMultiplier);
			}
		}
		if (dyn.socketTimeout != null && trg.socketTimeout != dyn.socketTimeout.value) {
			trg.socketTimeout = dyn.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.socketTimeout = " + trg.socketTimeout);
			}
		}
		if (dyn.timeoutDelay != null && trg.timeoutDelay != dyn.timeoutDelay.value) {
			trg.timeoutDelay = dyn.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.timeoutDelay = " + trg.timeoutDelay);
			}
		}
		if (dyn.totalTimeout != null && trg.totalTimeout != dyn.totalTimeout.value) {
			trg.totalTimeout = dyn.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.totalTimeout = " + trg.totalTimeout);
			}
		}
		if (dyn.maxRetries != null && trg.maxRetries != dyn.maxRetries.value) {
			trg.maxRetries = dyn.maxRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxRetries = " + trg.maxRetries);
			}
		}
		if (dyn.maxConcurrentThreads != null && trg.maxConcurrentThreads != dyn.maxConcurrentThreads.value) {
			trg.maxConcurrentThreads = dyn.maxConcurrentThreads.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxConcurrentThreads = " + trg.maxConcurrentThreads);
			}
		}
		if (dyn.allowInline != null && trg.allowInline != dyn.allowInline.value) {
			trg.allowInline = dyn.allowInline.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInline = " + trg.allowInline);
			}
		}
		if (dyn.allowInlineSSD != null && trg.allowInlineSSD != dyn.allowInlineSSD.value) {
			trg.allowInlineSSD = dyn.allowInlineSSD.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInlineSSD = " + trg.allowInlineSSD);
			}
		}
		if (dyn.respondAllKeys != null && trg.respondAllKeys != dyn.respondAllKeys.value) {
			trg.respondAllKeys = dyn.respondAllKeys.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.respondAllKeys = " + trg.respondAllKeys);
			}
		}
		return trg;
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setMaxConcurrentThreads(int maxConcurrentThreads) {
		this.maxConcurrentThreads = maxConcurrentThreads;
	}

	public void setAllowInline(boolean allowInline) {
		this.allowInline = allowInline;
	}

	public void setAllowInlineSSD(boolean allowInlineSSD) {
		this.allowInlineSSD = allowInlineSSD;
	}

	public void setRespondAllKeys(boolean respondAllKeys) {
		this.respondAllKeys = respondAllKeys;
	}
}
