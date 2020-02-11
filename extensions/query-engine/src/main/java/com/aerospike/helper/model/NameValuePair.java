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
package com.aerospike.helper.model;

public class NameValuePair {
	public String name;
	public Object value;

	public NameValuePair(String name, Object value) {
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return name + "|" + value.toString();
	}

	public void clear() {
		if (this.value != null && (this.value instanceof Long)) {
			this.value = 0L;
		} else if (this.value != null && (this.value instanceof String)) {
			this.value = "";
		} else if (this.value != null && (this.value instanceof Integer)) {
			this.value = 0;
		} else if (this.value != null && (this.value instanceof Float)) {
			this.value = 0.0;
		}
	}
}
