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
package com.aerospike.client.cdt;

import com.aerospike.client.Value;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

public abstract class CDT {

    enum Type {
        /**
         * Opcode used encode a CDT select and apply operation.
         * If flag bit 2 is clear, the operation will be interpreted as a select
         * operation; otherwise, as an apply operation.
         */
        SELECT(0xfe),

		/**
 		 * Opcode used to encode the calling of a virtual operation.
		 */
		CONTEXT_EVAL(0xff);

        final int value;

        Type(int value) {
            this.value = value;
        }
    }

	protected static byte[] packRangeOperation(int command, int returnType, Value begin, Value end, CTX[] ctx) {
		Packer packer = new Packer();

		// First pass calculates buffer size.
		// Second pass writes to buffer.
		for (int i = 0; i < 2; i++) {
			Pack.init(packer, ctx);
			packer.packArrayBegin((end != null) ? 4 : 3);
			packer.packInt(command);
			packer.packInt(returnType);

			if (begin != null) {
				begin.pack(packer);
			} else {
				packer.packNil();
			}

			if (end != null) {
				end.pack(packer);
			}

			if (i == 0) {
				packer.createBuffer();
			}
		}
		return packer.getBuffer();
	}

	protected static void init(Packer packer, CTX[] ctx, int command, int count, int flag) {
		packer.packArrayBegin(3);
		packer.packInt(Type.CONTEXT_EVAL.value);
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
