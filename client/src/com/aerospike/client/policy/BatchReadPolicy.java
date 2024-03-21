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

import com.aerospike.client.exp.Expression;

/**
 * Policy attributes used in batch read commands.
 */
public final class BatchReadPolicy {
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
	 * Read policy for AP (availability) namespaces.
	 * <p>
	 * Default: {@link ReadModeAP#ONE}
	 */
	public ReadModeAP readModeAP = ReadModeAP.ONE;

	/**
	 * Read policy for SC (strong consistency) namespaces.
	 * <p>
	 * Default: {@link ReadModeSC#SESSION}
	 */
	public ReadModeSC readModeSC = ReadModeSC.SESSION;

	/**
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 * <p>
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 * <p>
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 * <li>
	 * <p>
	 * Default: 0
	 */
	public int readTouchTtlPercent;

	/**
	 * Copy constructor.
	 */
	public BatchReadPolicy(BatchReadPolicy other) {
		this.filterExp = other.filterExp;
		this.readModeAP = other.readModeAP;
		this.readModeSC = other.readModeSC;
		this.readTouchTtlPercent = other.readTouchTtlPercent;
	}

	/**
	 * Default constructor.
	 */
	public BatchReadPolicy() {
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setFilterExp(Expression filterExp) {
		this.filterExp = filterExp;
	}

	public void setReadModeAP(ReadModeAP readModeAP) {
		this.readModeAP = readModeAP;
	}

	public void setReadModeSC(ReadModeSC readModeSC) {
		this.readModeSC = readModeSC;
	}

	public void setReadTouchTtlPercent(int readTouchTtlPercent) {
		this.readTouchTtlPercent = readTouchTtlPercent;
	}
}
