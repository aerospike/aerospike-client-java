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
package com.aerospike.client.exp;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;

/**
 * Expression operations.
 */
public final class ExpOperation {
	/**
	 * Create operation that performs an expression that writes to a record bin.
	 * Requires server version 5.6.0+.
	 *
	 * @param binName	name of bin to store expression result
	 * @param exp		expression to evaluate
	 * @param flags		expression write flags.  See {@link com.aerospike.client.exp.ExpWriteFlags}
	 */
	public static Operation write(String binName, Expression exp, int flags) {
		return createOperation(Operation.Type.EXP_MODIFY, binName, exp, flags);
	}

	/**
	 * Create operation that performs a read expression.
	 * Requires server version 5.6.0+.
	 *
	 * @param name		variable name of read expression result. This name can be used as the
	 * 					bin name when retrieving bin results from the record.
	 * @param exp		expression to evaluate
	 * @param flags		expression read flags.  See {@link com.aerospike.client.exp.ExpReadFlags}
	 */
	public static Operation read(String name, Expression exp, int flags) {
		return createOperation(Operation.Type.EXP_READ, name, exp, flags);
	}
	public static Operation read(String name, byte[] b, int flags) {
		return createOperation(Operation.Type.EXP_READ, name, b, flags);
	}

	private static Operation createOperation(Operation.Type type, String name, Expression exp, int flags) {
		byte[] packedBytes = packOperation(type, name, exp.getBytes(), flags);
		return new Operation(type, name, Value.get(packedBytes));
	}

	private static Operation createOperation(Operation.Type type, String name, byte[] b, int flags) {
		byte[] packedBytes = packOperation(type, name, b, flags);
		return new Operation(type, name, Value.get(packedBytes));
	}

	// return new Operation(type, name, Value.get(packer.getBuffer()));
	private static byte[] packOperation(Operation.Type type, String name, byte[] b, int flags) {
		Packer packer = new Packer();

		packer.packArrayBegin(2);
		packer.packByteArray(b, 0, b.length);
		packer.packInt(flags);

		packer.createBuffer();

		packer.packArrayBegin(2);
		packer.packByteArray(b, 0, b.length);
		packer.packInt(flags);

		return packer.getBuffer();
	}
}
