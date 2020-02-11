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
package com.aerospike.helper.query;

import com.aerospike.client.Key;
import com.aerospike.client.Value;

/**
 * Qualifier used to query by primary key
 *
 * @author peter
 */
public class KeyQualifier extends Qualifier {
	private static final long serialVersionUID = 2430949321378171078L;

	boolean hasDigest = false;

	public KeyQualifier(Value value) {
		super(QueryEngine.Meta.KEY.toString(), FilterOperation.EQ, value);
	}

	public KeyQualifier(byte[] digest) {
		super(QueryEngine.Meta.KEY.toString(), FilterOperation.EQ, null);
		this.internalMap.put("digest", digest);
		this.hasDigest = true;
	}

	@Override
	protected String luaFieldString(String field) {
		return "digest";
	}

	public byte[] getDigest() {
		return (byte[]) this.internalMap.get("digest");
	}

	public Key makeKey(String namespace, String set) {
		if (hasDigest) {
			byte[] digest = getDigest();
			return new Key(namespace, digest, set, null);
		} else {
			return new Key(namespace, set, getValue1());
		}
	}
}
