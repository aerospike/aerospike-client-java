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
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicTxnRollConfig;

/**
 * Transaction policy fields used to batch roll forward/backward records on
 * commit or abort. Used a placeholder for now as there are no additional fields beyond BatchPolicy.
 */
public class TxnRollPolicy extends BatchPolicy {
	/**
	 * Copy policy from another policy.
	 */
	public TxnRollPolicy(TxnRollPolicy other) {
		super(other);
	}

	/**
	 * Copy policy from another policy AND apply config overrides.
	 * Any policy overrides will not get logged.
	 */
	public TxnRollPolicy(TxnRollPolicy other, ConfigurationProvider configProvider) {
		super(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy policy from another policy AND apply config overrides.
	 * Any default policy overrides will get logged.
	 */
	public TxnRollPolicy(TxnRollPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		super(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

	/**
     * Default constructor.
     */
	public TxnRollPolicy() {
		replica = Replica.MASTER;
		maxRetries = 5;
		socketTimeout = 3000;
		totalTimeout = 10000;
		sleepBetweenRetries = 1000;
	}

	@SuppressWarnings("deprecation")
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
		DynamicTxnRollConfig dynTRC = dConfig.getDynamicTxnRollConfig();
		if (dynTRC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynTRC.readModeAP != null & this.readModeAP != dynTRC.readModeAP) {
			this.readModeAP = dynTRC.readModeAP;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.readModeAP = " + this.readModeAP);
			}
		}
		if (dynTRC.readModeSC != null && this.readModeSC != dynTRC.readModeSC) {
			this.readModeSC = dynTRC.readModeSC;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.readModeSC = " + this.readModeSC);
			}
		}
		if (dynTRC.connectTimeout != null && this.connectTimeout != dynTRC.connectTimeout.value) {
			this.connectTimeout = dynTRC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynTRC.replica != null && this.replica != dynTRC.replica) {
			this.replica = dynTRC.replica;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.replica = " + this.replica);
			}
		}
		if (dynTRC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynTRC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynTRC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynTRC.sleepMultiplier != null && this.sleepMultiplier != dynTRC.sleepMultiplier.value) {
			this.sleepMultiplier = dynTRC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynTRC.socketTimeout != null && this.socketTimeout != dynTRC.socketTimeout.value) {
			this.socketTimeout = dynTRC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynTRC.timeoutDelay != null && this.timeoutDelay != dynTRC.timeoutDelay.value) {
			this.timeoutDelay = dynTRC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynTRC.totalTimeout != null && this.totalTimeout != dynTRC.totalTimeout.value) {
			this.totalTimeout = dynTRC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynTRC.maxRetries != null && this.maxRetries != dynTRC.maxRetries.value) {
			this.maxRetries = dynTRC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynTRC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynTRC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynTRC.maxConcurrentThreads.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.maxConcurrentThreads = " + this.maxConcurrentThreads);
			}
		}
		if (dynTRC.allowInline != null && this.allowInline != dynTRC.allowInline.value) {
			this.allowInline = dynTRC.allowInline.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.allowInline = " + this.allowInline);
			}
		}
		if (dynTRC.allowInlineSSD != null && this.allowInlineSSD != dynTRC.allowInlineSSD.value) {
			this.allowInlineSSD = dynTRC.allowInlineSSD.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.allowInlineSSD = " + this.allowInlineSSD);
			}
		}
		if (dynTRC.respondAllKeys != null && this.respondAllKeys != dynTRC.respondAllKeys.value) {
			this.respondAllKeys = dynTRC.respondAllKeys.value;
			if (logUpdate) {
				Log.info("Set TxnRollPolicy.respondAllKeys = " + this.respondAllKeys);
			}
		}
	}
}
