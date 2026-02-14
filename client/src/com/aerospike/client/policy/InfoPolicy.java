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
package com.aerospike.client.policy;

/**
 * Policy for info protocol requests to a node; currently supports socket timeout.
 * <p>
 * Pass to {@link com.aerospike.client.Info#request} and related overloads, or use for cluster tend. Default timeout is 1000 ms.
 *
 * <p><b>Example:</b>
 * <p>Use a 2-second socket timeout for info requests to a node (e.g. to fetch "build").</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * InfoPolicy policy = new InfoPolicy();
 * policy.timeout = 2000;
 * String build = Info.request(policy, client.getNodes()[0], "build");
 * }</pre>
 *
 * @see com.aerospike.client.Info#request
 */
public final class InfoPolicy {
	/**
	 * Info command socket timeout in milliseconds.
	 * <p>
	 * Default: 1000
	 */
	public int timeout;

	/**
	 * Copy timeout from other InfoPolicy.
	 */
	public InfoPolicy(InfoPolicy other) {
		this.timeout = other.timeout;
	}

	/**
	 * Copy timeout from generic Policy to InfoPolicy.
	 */
	public InfoPolicy(Policy other) {
		this.timeout = other.socketTimeout;
	}

	/**
	 * Default constructor.  Default is one second timeout.
	 */
	public InfoPolicy() {
		timeout = 1000;
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
}
