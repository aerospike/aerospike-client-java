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
package com.aerospike.client.util;

import java.util.List;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;

public final class Pack {
	public static byte[] pack(int command, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(1);
		packer.packInt(command);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(1);
		packer.packInt(command);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		packer.packInt(v1);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		packer.packInt(v1);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, int v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, long v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packLong(v3);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packLong(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, boolean v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packBoolean(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packBoolean(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, int v2, byte[] v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packParticleBytes(v3);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packParticleBytes(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, byte[] v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packParticleBytes(v2);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packParticleBytes(v2);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Value v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Value v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Value v2, int v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, List<Value> list, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packValueList(list);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packValueList(list);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, List<Value> v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packValueList(v2);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		packer.packValueList(v2);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Value value, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		value.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		value.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Value value, int v1, int v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		value.pack(packer);
		packer.packInt(v1);
		packer.packInt(v2);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		value.pack(packer);
		packer.packInt(v1);
		packer.packInt(v2);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Value v1, Value v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Value v1, Value v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Value v1, Value v2, int v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		Pack.init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		packer.createBuffer();

		Pack.init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, List<?> list, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		packer.packList(list);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		packer.packList(list);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, List<?> list, int v1, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packList(list);
		packer.packInt(v1);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packList(list);
		packer.packInt(v1);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, List<Value> list, int v1, int v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packValueList(list);
		packer.packInt(v1);
		packer.packInt(v2);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packValueList(list);
		packer.packInt(v1);
		packer.packInt(v2);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, List<Value> list, int v1, int v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packValueList(list);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packValueList(list);
		packer.packInt(v1);
		packer.packInt(v2);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Exp v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Exp v2, Exp v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		v3.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		v3.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, int v1, Exp v2, Exp v3, Exp v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		v3.pack(packer);
		v4.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		packer.packInt(v1);
		v2.pack(packer);
		v3.pack(packer);
		v4.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1) {
		Packer packer = new Packer();

		packer.packArrayBegin(2);
		packer.packInt(command);
		v1.pack(packer);

		packer.createBuffer();

		packer.packArrayBegin(2);
		packer.packInt(command);
		v1.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		v1.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(2);
		packer.packInt(command);
		v1.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, int v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		packer.packInt(v2);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		packer.packInt(v2);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, int v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		packer.packInt(v2);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		packer.packInt(v2);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, Exp v2, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(3);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, Exp v2, int v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, Exp v2, int v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		packer.packInt(v3);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, Exp v2, Exp v3, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		v3.pack(packer);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(4);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		v3.pack(packer);

		return packer.getBuffer();
	}

	public static byte[] pack(int command, Exp v1, Exp v2, Exp v3, int v4, CTX... ctx) {
		Packer packer = new Packer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		v3.pack(packer);
		packer.packInt(v4);

		packer.createBuffer();

		init(packer, ctx);
		packer.packArrayBegin(5);
		packer.packInt(command);
		v1.pack(packer);
		v2.pack(packer);
		v3.pack(packer);
		packer.packInt(v4);

		return packer.getBuffer();
	}

	public static void init(Packer packer, CTX[] ctx) {
		if (ctx != null && ctx.length > 0) {
			packer.packArrayBegin(3);
			packer.packInt(0xff);
			packer.packArrayBegin(ctx.length * 2);

			for (CTX c : ctx) {
				packer.packInt(c.id);
				c.value.pack(packer);
			}
		}
	}

	public static byte[] pack(CTX[] ctx) {
		Packer packer = new Packer();

		packer.packArrayBegin(ctx.length * 2);

		for (CTX c : ctx) {
			packer.packInt(c.id);
			c.value.pack(packer);
		}

		packer.createBuffer();

		packer.packArrayBegin(ctx.length * 2);

		for (CTX c : ctx) {
			packer.packInt(c.id);
			c.value.pack(packer);
		}

		return packer.getBuffer();
	}
}
