/*
 * Copyright 2012-2023 Aerospike, Inc.
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

package com.aerospike.client.proxy;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.BVal;

public class ProxyRecord {
	/**
	 * Optional Key.
	 */
	public final Key key;

	/**
	 * Optional Record result after command has completed.
	 */
	public final Record record;

	/**
	 * Optional bVal.
	 */
	public final BVal bVal;

	/**
	 * The result code from proxy server.
	 */
	public final int resultCode;

	public ProxyRecord(int resultCode, Key key, Record record, BVal bVal) {
		this.resultCode = resultCode;
		this.key = key;
		this.record = record;
		this.bVal = bVal;
	}
}
