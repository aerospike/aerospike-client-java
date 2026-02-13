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
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchReadConfig;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicBatchWriteConfig;

import java.util.Objects;

/**
 * Base policy for batch operations (batch get, batch exists, batch write, batch UDF, etc.).
 *
 * <p>Defines concurrency (maxConcurrentThreads), allowInline, and other batch-level options. Subclasses
 * and companion policies (e.g. {@link BatchWritePolicy}, {@link BatchDeletePolicy}) add operation-specific options.
 * Pass to {@link com.aerospike.client.AerospikeClient#get(Policy, List)} and other batch methods.
 *
 * <p>Set maxConcurrentThreads, allowInline and pass to batch get or other batch methods.</p>
 * <pre>{@code
 * BatchPolicy policy = new BatchPolicy();
 * policy.maxConcurrentThreads = 0;   // 0=parallel, 1=sequential (default), >1=cap
 * policy.allowInline = true;         // inline for in-memory (default)
 * policy.allowInlineSSD = false;     // no inline for SSD (default)
 * policy.respondAllKeys = true;      // attempt all keys despite errors (default)
 * policy.totalTimeout = 5000;       // from Policy: total batch timeout
 * policy.socketTimeout = 2000;       // from Policy: per-socket timeout
 * client.get(policy, keys);
 * }</pre>
 *
 * @see Policy
 * @see BatchWritePolicy
 * @see BatchDeletePolicy
 */
public class BatchPolicy extends Policy {
	/**
	 * Maximum number of concurrent synchronous batch node request threads to server nodes.
	 * Asynchronous batch requests ignore this field and always issue all node requests in parallel.
	 * <p>
	 * The batch is split into requests for each node according to the node assignment of each
	 * batch key. The number of batch node requests is always less or equal to the cluster size.
	 * <p>
	 * If there are 16 batch node requests and maxConcurrentThreads is 8, then batch requests
	 * will be made for 8 nodes in parallel threads. When a request completes, a new request will
	 * be issued until all 16 requests are complete. If there are 4 batch node requests and
	 * maxConcurrentThreads is 8, then only 4 batch requests will be made for 4 nodes in parallel
	 * threads.
	 * <p>
	 * Values:
	 * <ul>
	 * <li>
	 * 1 (default): Issue batch node requests sequentially. This mode has a performance advantage
	 * for small batch sizes because requests can be issued in the main transaction thread without
	 * using a thread pool. This mode is not optimal for batch requests spread out over many nodes
	 * in a large cluster.
	 * </li>
	 * <li>
	 * 0: Issue all batch node requests in parallel threads. This mode has a performance advantage
	 * for large batch sizes because each node can process the request immediately. The downside is
	 * extra threads will need to be created (or taken from a thread pool). In extreme cases, the
	 * operating system's thread capacity could be exhausted.
	 * </li>
	 * <li>
	 * > 0: Issue up to maxConcurrentThreads batch node requests in parallel threads. When a request
	 * completes, a new request will be issued until all requests are complete. This mode prevents
	 * too many parallel threads being created for large clusters. The downside is extra threads
	 * will still need to be created (or taken from a thread pool). A typical value is the number
	 * of cpu cores available on the client machine.
	 * </li>
	 * </ul>
	 * <p>
	 * Default: 1
	 */
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
	 * The client will stop the entire batch on node specific errors for sync commands
	 * that are run in sequence (maxConcurrentThreads == 1). The client will not stop
	 * the entire batch for async commands or sync commands run in parallel.
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
	 * Copy batch policy from another batch policy AND override certain policy attributes if they exist in the
	 * configProvider. Any policy overrides will not get logged.
	 */
	public BatchPolicy(BatchPolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false, "");
	}

	/**
	 * Copy batch policy from another batch policy AND override certain policy attributes if they exist in the
	 * configProvider. Any default policy overrides will get logged.
	 */
	public BatchPolicy(BatchPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy, String preText) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy, preText);
	}

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
		DynamicBatchReadConfig dynBRC = dConfig.getDynamicBatchReadConfig();
		if (dynBRC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (!Objects.equals(preText, "")) {
			preText = " " + preText;
		}
		if (dynBRC.readModeAP != null && this.readModeAP != dynBRC.readModeAP) {
			this.readModeAP = dynBRC.readModeAP;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.readModeAP = " + this.readModeAP);
			}
		}
		if (dynBRC.readModeSC != null && this.readModeSC != dynBRC.readModeSC) {
			this.readModeSC = dynBRC.readModeSC;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.readModeSC = " + this.readModeSC);
			}
		}
		if (dynBRC.connectTimeout != null && this.connectTimeout != dynBRC.connectTimeout.value) {
			this.connectTimeout = dynBRC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynBRC.replica != null && this.replica != dynBRC.replica) {
			this.replica = dynBRC.replica;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.replica = " + this.replica);
			}
		}
		if (dynBRC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynBRC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynBRC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}

		if (dynBRC.sleepMultiplier != null && this.sleepMultiplier != dynBRC.sleepMultiplier.value) {
			this.sleepMultiplier = dynBRC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}

		if (dynBRC.socketTimeout != null && this.socketTimeout != dynBRC.socketTimeout.value) {
			this.socketTimeout = dynBRC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynBRC.timeoutDelay != null && this.timeoutDelay != dynBRC.timeoutDelay.value) {
			this.timeoutDelay = dynBRC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynBRC.totalTimeout != null && this.totalTimeout != dynBRC.totalTimeout.value) {
			this.totalTimeout = dynBRC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynBRC.maxRetries != null && this.maxRetries != dynBRC.maxRetries.value) {
			this.maxRetries = dynBRC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynBRC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynBRC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynBRC.maxConcurrentThreads.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.maxConcurrentThreads = " + this.maxConcurrentThreads);
			}
		}
		if (dynBRC.allowInline != null && this.allowInline != dynBRC.allowInline.value) {
			this.allowInline = dynBRC.allowInline.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInline = " + this.allowInline);
			}
		}
		if (dynBRC.allowInlineSSD != null && this.allowInlineSSD != dynBRC.allowInlineSSD.value) {
			this.allowInlineSSD = dynBRC.allowInlineSSD.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.allowInlineSSD = " + this.allowInlineSSD);
			}
		}
		if (dynBRC.respondAllKeys != null && this.respondAllKeys != dynBRC.respondAllKeys.value) {
			this.respondAllKeys = dynBRC.respondAllKeys.value;
			if (logUpdate) {
				Log.info("Set" + preText + " BatchPolicy.respondAllKeys = " + this.respondAllKeys);
			}
		}
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

	/**
	 * Apply batch_write config properties if they exist in the configProvider (BatchWrite).
	 */
	public void graftBatchWriteConfig(ConfigurationProvider configProvider) {
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

		if (dynBWC.connectTimeout != null && this.connectTimeout != dynBWC.connectTimeout.value) {
			this.connectTimeout = dynBWC.connectTimeout.value;
		}
		if (dynBWC.replica != null && this.replica != dynBWC.replica) {
			this.replica = dynBWC.replica;
		}
		if (dynBWC.sendKey != null && 	this.sendKey != dynBWC.sendKey.value) {
			this.sendKey = dynBWC.sendKey.value;
		}
		if (dynBWC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynBWC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynBWC.sleepBetweenRetries.value;
		}
		if (dynBWC.sleepMultiplier != null && this.sleepMultiplier != dynBWC.sleepMultiplier.value) {
			this.sleepMultiplier = dynBWC.sleepMultiplier.value;
		}
		if (dynBWC.socketTimeout != null && this.socketTimeout != dynBWC.socketTimeout.value) {
			this.socketTimeout = dynBWC.socketTimeout.value;
		}
		if (dynBWC.timeoutDelay != null && this.timeoutDelay != dynBWC.timeoutDelay.value) {
			this.timeoutDelay = dynBWC.timeoutDelay.value;
		}
		if (dynBWC.totalTimeout != null && this.totalTimeout != dynBWC.totalTimeout.value) {
			this.totalTimeout = dynBWC.totalTimeout.value;
		}
		if (dynBWC.maxRetries != null && this.maxRetries != dynBWC.maxRetries.value) {
			this.maxRetries = dynBWC.maxRetries.value;
		}
		if (dynBWC.maxConcurrentThreads != null && this.maxConcurrentThreads != dynBWC.maxConcurrentThreads.value) {
			this.maxConcurrentThreads = dynBWC.maxConcurrentThreads.value;
		}
		if (dynBWC.allowInlineSSD != null && this.allowInlineSSD != dynBWC.allowInlineSSD.value) {
			this.allowInlineSSD = dynBWC.allowInlineSSD.value;
		}
		if (dynBWC.respondAllKeys != null && this.respondAllKeys != dynBWC.respondAllKeys.value) {
			this.respondAllKeys = dynBWC.respondAllKeys.value;
		}
	}
}
