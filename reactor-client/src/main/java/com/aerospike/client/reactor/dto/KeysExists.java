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

import java.util.HashMap;
import java.util.Map;

/**
 * Container object for keys identifier and exists flags.
 */
public final class KeysExists {
	/**
	 * Unique identifier for record.
	 */
	public final Key[] keys;

	public final boolean[] exists;

	public KeysExists(Key[] keys, boolean[] exists) {
		this.keys = keys;
		this.exists = exists;
	}

	public Map<Key, Boolean> asMap(){
		Map<Key, Boolean> map = new HashMap<>(keys.length);
		for(int i = 0, n = keys.length; i < n; i++){
			map.put(keys[i], exists[i]);
		}
		return map;
	}

	@Override
	public String toString() {
		return asMap().toString();
	}

}
