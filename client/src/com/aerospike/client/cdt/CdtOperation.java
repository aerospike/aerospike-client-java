/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;
import com.aerospike.client.exp.Expression;

public class CdtOperation {	
    /**
     * Create CDT select operation with context.
     *
     * @param binName		bin name
     * @param flags			select flags
     * @param ctx			optional path to nested CDT
     */
    public static Operation selectByPath(String binName, int flags, CTX... ctx) {
        byte[] packedBytes;
        if (binName == null || binName.isEmpty() || binName.length() > Bin.MAX_BIN_NAME_LENGTH) {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "binName cannot be null, empty, or exceed " + Bin.MAX_BIN_NAME_LENGTH + " characters");
        }
        if (ctx == null || ctx.length == 0) { 
            packedBytes = Pack.pack(CDT.Type.SELECT.value, flags);
        } else {
            packedBytes = packCdtSelect(flags, CDT.Type.SELECT, ctx);
        }

        return new Operation(Operation.Type.CDT_READ, binName, Value.get(packedBytes, ParticleType.BLOB));
    }

    /**
     * Create CDT apply operation with context and modify expression.
     *
     * @param binName		bin name
     * @param flags			select flags
     * @param modifyExp		modify expression
     * @param ctx			optional path to nested CDT
     */
    public static Operation modifyByPath(String binName, int flags, Expression modifyExp, CTX... ctx) {
        byte[] packedBytes;
        if (binName == null || binName.isEmpty() || binName.length() > Bin.MAX_BIN_NAME_LENGTH) {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR, "binName cannot be null, empty, or exceed " + Bin.MAX_BIN_NAME_LENGTH + " characters");
        }
        if (ctx == null || ctx.length == 0) { 
            packedBytes = Pack.pack(CDT.Type.SELECT.value, flags, modifyExp);
        } else {
            packedBytes = packCdtModify(flags, CDT.Type.SELECT, modifyExp, ctx);
        }

        return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packedBytes, ParticleType.BLOB));
    }
	
	private static byte[] packCdtSelect(int flags, CDT.Type typeSelect, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(3);
            packer.packInt(typeSelect.value);
            packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
                    packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

 	        // Ensure the apply flag is cleared, since no expression is provided.
	        // This avoids problems if the caller accidentally sets bit 2 in the flags field.
            packer.packInt(flags & ~4);           

            if (i == 0) {
                packer.createBuffer();
            }
        }

        return packer.getBuffer();
	}

	private static byte[] packCdtModify(int modifyFlag, CDT.Type type, Expression modifyExp, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(4);
            packer.packInt(type.value);
            packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
                    packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

            packer.packInt(modifyFlag | 4);
            packer.packByteArray(modifyExp.getBytes(), 0, modifyExp.getBytes().length);

            if (i == 0) {
                packer.createBuffer();
            }
        }

        return packer.getBuffer();
	}
}
