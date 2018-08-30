/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.reactor.dto;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.HashMap;
import java.util.Map;

/**
 * Container object for keys identifier and exists flags.
 */
public final class KeysRecords {
	/**
	 * Unique identifiers for records.
	 */
	public final Key[] keys;

	/**
	 * Records headers and bin data.
	 */
	public final Record[] records;


	public KeysRecords(Key[] keys, Record[] records) {
		this.keys = keys;
		this.records = records;
	}

	public Map<Key, Record> asMap(){
		Map<Key, Record> map = new HashMap<>(keys.length);
		for(int i = 0, n = keys.length; i < n; i++){
			map.put(keys[i], records[i]);
		}
		return map;
	}

	@Override
	public String toString() {
		return asMap().toString();
	}
}
