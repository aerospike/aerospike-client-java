/*
 * Copyright 2012-2017 Aerospike, Inc.
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
 * Policy attributes used for info commands.
 */
public final class InfoPolicy {
	/**
	 * Info command socket timeout in milliseconds.
	 * Default is one second timeout.
	 */
	public int socketTimeout;
	
	/**
	 * Copy timeout from other InfoPolicy.
	 */
	public InfoPolicy(InfoPolicy other) {
		this.socketTimeout = other.socketTimeout;
	}

	/**
	 * Copy timeout from generic Policy to InfoPolicy.
	 */
	public InfoPolicy(Policy other) {
		this.socketTimeout = other.socketTimeout;
	}
	
	/**
	 * Default constructor.  Default is one second timeout.
	 */
	public InfoPolicy() {
		socketTimeout = 1000;
	}
}
