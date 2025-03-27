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

	/**
	 * Override certain policy attributes if they exist in the configProvider
	 */
	public void applyConfigOverrides(ConfigurationProvider configProvider) {
		Configuration config = configProvider.fetchConfiguration();
		DynamicTxnVerifyConfig dynTVC = config.dynamicConfiguration.dynamicTxnVerifyConfig;

		if (dynTVC.readModeAP != null ) this.readModeAP = dynTVC.readModeAP;
		if (dynTVC.readModeSC != null ) this.readModeSC = dynTVC.readModeSC;
		if (dynTVC.connectTimeout != null ) this.connectTimeout = dynTVC.connectTimeout.value;
		if (dynTVC.replica != null ) this.replica = dynTVC.replica;
		if (dynTVC.sleepBetweenRetries != null ) this.sleepBetweenRetries = dynTVC.sleepBetweenRetries.value;
		if (dynTVC.socketTimeout != null ) this.socketTimeout = dynTVC.socketTimeout.value;
		if (dynTVC.timeoutDelay != null ) this.timeoutDelay = dynTVC.timeoutDelay.value;
		if (dynTVC.totalTimeout != null ) this.totalTimeout = dynTVC.totalTimeout.value;
		if (dynTVC.maxRetries != null ) this.maxRetries = dynTVC.maxRetries.value;
		if (dynTVC.maxConcurrentThreads != null ) this.maxConcurrentThreads = dynTVC.maxConcurrentThreads.value;
		if (dynTVC.allowInline != null ) this.allowInline = dynTVC.allowInline.value;
		if (dynTVC.allowInlineSSD != null ) this.allowInlineSSD = dynTVC.allowInlineSSD.value;
		if (dynTVC.respondAllKeys != null ) this.respondAllKeys = dynTVC.respondAllKeys.value;

		Log.debug("TxnVerifyPolicy has been aligned with config properties.");
	}
}
