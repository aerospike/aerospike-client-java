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
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicTxnVerifyConfig;

/**
 * Transaction policy fields used to batch verify record versions on commit.
 * Used a placeholder for now as there are no additional fields beyond BatchPolicy.
 */
public class TxnVerifyPolicy extends BatchPolicy {
	/**
	 * Copy policy from another policy.
	 */
	public TxnVerifyPolicy(TxnVerifyPolicy other) {
		super(other);
	}

	/**
	 * Copy policy from another policy AND apply config overrides.
	 * Any policy overrides will not get logged.
	 */
	public TxnVerifyPolicy(TxnVerifyPolicy other, ConfigurationProvider configProvider) {
		super(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy policy from another policy AND apply config overrides.
	 * Any default policy overrides will get logged.
	 */
	public TxnVerifyPolicy(TxnVerifyPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		super(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

	/**
	 * Default constructor.
	 */
	public TxnVerifyPolicy() {
		readModeSC = ReadModeSC.LINEARIZE;
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
		DynamicTxnVerifyConfig dynTVC = dConfig.getDynamicTxnVerifyConfig();
		if (dynTVC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynTVC.readModeAP != null & this.readModeAP != dynTVC.readModeAP) {
			this.readModeAP = dynTVC.readModeAP;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.readModeAP = " + this.readModeAP);
			}
		}
		if (dynTVC.readModeSC != null && this.readModeSC != dynTVC.readModeSC) {
			this.readModeSC = dynTVC.readModeSC;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.readModeSC = " + this.readModeSC);
			}
		}
		if (dynTVC.connectTimeout != null && this.connectTimeout != dynTVC.connectTimeout.value) {
			this.connectTimeout = dynTVC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynTVC.replica != null && this.replica != dynTVC.replica) {
			this.replica = dynTVC.replica;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.replica = " + this.replica);
			}
		}
		if (dynTVC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynTVC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynTVC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynTVC.sleepMultiplier != null && this.sleepMultiplier != dynTVC.sleepMultiplier.value) {
			this.sleepMultiplier = dynTVC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynTVC.socketTimeout != null && this.socketTimeout != dynTVC.socketTimeout.value) {
			this.socketTimeout = dynTVC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynTVC.timeoutDelay != null && this.timeoutDelay != dynTVC.timeoutDelay.value) {
			this.timeoutDelay = dynTVC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynTVC.totalTimeout != null && this.totalTimeout != dynTVC.totalTimeout.value) {
			this.totalTimeout = dynTVC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynTVC.maxRetries != null && this.maxRetries != dynTVC.maxRetries.value) {
			this.maxRetries = dynTVC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynTVC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynTVC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynTVC.maxConcurrentThreads.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.maxConcurrentThreads = " + this.maxConcurrentThreads);
			}
		}
		if (dynTVC.allowInline != null && this.allowInline != dynTVC.allowInline.value) {
			this.allowInline = dynTVC.allowInline.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.allowInline = " + this.allowInline);
			}
		}
		if (dynTVC.allowInlineSSD != null && this.allowInlineSSD != dynTVC.allowInlineSSD.value) {
			this.allowInlineSSD = dynTVC.allowInlineSSD.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.allowInlineSSD = " + this.allowInlineSSD);
			}
		}
		if (dynTVC.respondAllKeys != null && this.respondAllKeys != dynTVC.respondAllKeys.value) {
			this.respondAllKeys = dynTVC.respondAllKeys.value;
			if (logUpdate) {
				Log.info("Set TxnVerifyPolicy.respondAllKeys = " + this.respondAllKeys);
			}
		}
	}
}
