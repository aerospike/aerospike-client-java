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

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;

/**
 * Qualifier used to query by expiry epoch
 *
 * @author peter
 */
public class ExpiryQualifier extends Qualifier {
	private static final long serialVersionUID = 13172814137477042L;

	public ExpiryQualifier(FilterOperation op, Value value) {
		super(QueryEngine.Meta.EXPIRATION.toString(), op, value);
		if (value.getType() != ParticleType.INTEGER) {
			throw new QualifierException("ExpiryQualifier value must be an integer or long");
		}
	}

	@Override
	protected String luaFieldString(String field) {
		return "expiry";
	}
}
