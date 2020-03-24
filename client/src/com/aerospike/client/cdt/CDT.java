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
package com.aerospike.client.cdt;

import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;

public abstract class CDT {

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 0);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 1);
		packer.packInt(v1);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, int v2) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 2);
		packer.packInt(v1);
		packer.packInt(v2);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, int v2, int v3) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 3);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, Value v2) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 2);
		packer.packInt(v1);
		v2.pack(packer);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, Value v2, int v3) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 3);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, Value v2, int v3, int v4) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 4);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, int v1, List<Value> v2) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 2);
		packer.packInt(v1);
		packer.packValueList(v2);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createOperation(int command, Operation.Type type, String binName, CTX[] ctx, Value v1, Value v2, int v3) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, command, 3);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static Operation createRangeOperation(int command, Operation.Type type, String binName, CTX[] ctx, Value begin, Value end, int returnType) {
		Packer packer = new Packer();

		if (begin == null) {
			begin = Value.getAsNull();
		}

		if (end == null) {
			CDT.init(packer, ctx, command, 2);
			packer.packInt(returnType);
			begin.pack(packer);
		}
		else {
			CDT.init(packer, ctx, command, 3);
			packer.packInt(returnType);
			begin.pack(packer);
			end.pack(packer);
		}
		return new Operation(type, binName, Value.get(packer.toByteArray()));
	}

	protected static void init(Packer packer, CTX[] ctx, int command, int count) {
		if (ctx != null && ctx.length > 0) {
			packer.packArrayBegin(3);
			packer.packInt(0xff);
			packer.packArrayBegin(ctx.length * 2);

			for (CTX c : ctx) {
				packer.packInt(c.id);
				c.value.pack(packer);
			}
			packer.packArrayBegin(count + 1);
			packer.packInt(command);
		}
		else {
			packer.packRawShort(command);

			if (count > 0) {
				packer.packArrayBegin(count);
			}
		}
	}

	protected static void init(Packer packer, CTX[] ctx, int command, int count, int flag) {
		packer.packArrayBegin(3);
		packer.packInt(0xff);
		packer.packArrayBegin(ctx.length * 2);

		CTX c;
		int last = ctx.length - 1;

		for (int i = 0; i < last; i++) {
			c = ctx[i];
			packer.packInt(c.id);
			c.value.pack(packer);
		}

		c = ctx[last];
		packer.packInt(c.id | flag);
		c.value.pack(packer);

		packer.packArrayBegin(count + 1);
		packer.packInt(command);
	}
}
