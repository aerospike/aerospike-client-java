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
import com.aerospike.client.query.PredExp;

/**
 * Qualifier used to query by latest update time
 *
 * @author eluppol
 */
public class LatestUpdateQualifier extends Qualifier {
	private static final long serialVersionUID = -8767573059309320133L;

	public LatestUpdateQualifier(FilterOperation op, Value value) {
		super("latest_update_time", op, value); // the field should never be used as here we use a bit different logic
		if (value.getType() != ParticleType.INTEGER) {
			throw new QualifierException("LatestUpdateQualifer value must be an integer or long");
		}
	}

	@Override
	public PredExp getFieldExpr(int valType)
	{
		return PredExp.recLastUpdate();
	}
}
