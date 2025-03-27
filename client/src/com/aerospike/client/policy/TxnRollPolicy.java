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
	 * Default constructor.
	 */
	public TxnRollPolicy() {
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
		DynamicTxnRollConfig dynTRC = config.dynamicConfiguration.dynamicTxnRollConfig;

		if (dynTRC.readModeAP != null ) this.readModeAP = dynTRC.readModeAP;
		if (dynTRC.readModeSC != null ) this.readModeSC = dynTRC.readModeSC;
		if (dynTRC.connectTimeout != null ) this.connectTimeout = dynTRC.connectTimeout.value;
		if (dynTRC.replica != null ) this.replica = dynTRC.replica;
		if (dynTRC.sleepBetweenRetries != null ) this.sleepBetweenRetries = dynTRC.sleepBetweenRetries.value;
		if (dynTRC.socketTimeout != null ) this.socketTimeout = dynTRC.socketTimeout.value;
		if (dynTRC.timeoutDelay != null ) this.timeoutDelay = dynTRC.timeoutDelay.value;
		if (dynTRC.totalTimeout != null ) this.totalTimeout = dynTRC.totalTimeout.value;
		if (dynTRC.maxRetries != null ) this.maxRetries = dynTRC.maxRetries.value;
		if (dynTRC.maxConcurrentThreads != null ) this.maxConcurrentThreads = dynTRC.maxConcurrentThreads.value;
		if (dynTRC.allowInline != null ) this.allowInline = dynTRC.allowInline.value;
		if (dynTRC.allowInlineSSD != null ) this.allowInlineSSD = dynTRC.allowInlineSSD.value;
		if (dynTRC.respondAllKeys != null ) this.respondAllKeys = dynTRC.respondAllKeys.value;

		Log.debug("TxnRollPolicy has been aligned with config properties.");
	}

}
