/*
 * Copyright 2012-2020 Aerospike, Inc.
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
package com.aerospike.client.operation;

/**
 * Action to take when bitwise add/subtract results in overflow/underflow.
 */
public enum BitOverflowAction {
	/**
	 * Fail operation with error.
	 */
	FAIL(0),

	/**
	 * If add/subtract overflows/underflows, set to max/min value.
	 * Example: MAXINT + 1 = MAXINT
	 */
	SATURATE(2),

	/**
	 * If add/subtract overflows/underflows, wrap the value.
	 * Example: MAXINT + 1 = -1
	 */
	WRAP(4);

	int flags;

	private BitOverflowAction(int flags) {
		this.flags = flags;
	}
}
