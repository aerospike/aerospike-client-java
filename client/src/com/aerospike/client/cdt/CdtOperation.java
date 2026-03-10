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
	 * Create a CDT select operation that traverses a nested CDT structure and returns
	 * matching values.
	 * <p>
	 * The result type depends on the select flags:
	 * <ul>
	 * <li>{@link SelectFlags#VALUE} - returns a list of leaf values found at the path.</li>
	 * <li>{@link SelectFlags#MATCHING_TREE} - returns the original nested structure.</li>
	 * <li>{@link SelectFlags#MAP_KEY} - returns a list of map keys.</li>
	 * <li>{@link SelectFlags#MAP_KEY_VALUE} - returns a list of map key-value pairs.</li>
	 * </ul>
	 * <p>
	 * Valid {@code flags} are defined in {@link SelectFlags} and can be combined with bitwise OR
	 * (e.g. {@code SelectFlags.VALUE | SelectFlags.NO_FAIL}).
	 * <p>
	 * The context path ({@code ctx}) uses {@link CTX} methods such as {@link CTX#mapKey(Value)},
	 * {@link CTX#allChildren()}, and {@link CTX#allChildrenWithFilter(com.aerospike.client.exp.Exp)}.
	 * May be null or empty to operate on the top-level bin value.
	 *
	 * <pre>{@code
	 * // Select all book titles where price <= 10.0.
	 * CTX ctx1 = CTX.mapKey(Value.get("book"));
	 * CTX ctx2 = CTX.allChildrenWithFilter(
	 *     Exp.le(
	 *         MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
	 *             Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
	 *         Exp.val(10.0)
	 *     )
	 * );
	 * CTX ctx3 = CTX.allChildrenWithFilter(
	 *     Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
	 * );
	 *
	 * Operation selectOp = CdtOperation.selectByPath("myBin", SelectFlags.VALUE, ctx1, ctx2, ctx3);
	 * Record result = client.operate(null, key, selectOp);
	 * List<?> titles = result.getList("myBin");
	 * }</pre>
	 *
	 * @param binName	bin name
	 * @param flags		select flags from {@link SelectFlags}
	 * @param ctx		context path using {@link CTX} methods
	 * @return			a CDT read {@link Operation}
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
	 * Create a CDT modify operation that traverses a nested CDT structure and applies
	 * a modification expression at each matching location. The operation writes the
	 * modified CDT structure back to the bin.
	 * <p>
	 * Valid {@code flags} are defined in {@link ModifyFlags} and can be combined with bitwise OR
	 * (e.g. {@code ModifyFlags.APPLY | ModifyFlags.NO_FAIL}).
	 * <p>
	 * The {@code modifyExp} is a compiled expression built with
	 * {@link com.aerospike.client.exp.Exp#build(com.aerospike.client.exp.Exp)}.
	 * The expression can reference the current value via loop variable expressions such as
	 * {@link com.aerospike.client.exp.Exp#floatLoopVar(com.aerospike.client.exp.LoopVarPart)}.
	 * <p>
	 * The context path ({@code ctx}) uses {@link CTX} methods such as {@link CTX#mapKey(Value)},
	 * {@link CTX#allChildren()}, and {@link CTX#allChildrenWithFilter(com.aerospike.client.exp.Exp)}.
	 * May be null or empty to operate on the top-level bin value.
	 *
	 * <pre>{@code
	 * // Increase all book prices by 10%.
	 * CTX bookKey = CTX.mapKey(Value.get("book"));
	 * CTX allChildren = CTX.allChildren();
	 * CTX priceKey = CTX.mapKey(Value.get("price"));
	 *
	 * Expression modifyExp = Exp.build(
	 *     Exp.mul(
	 *         Exp.floatLoopVar(LoopVarPart.VALUE),
	 *         Exp.val(1.10)
	 *     )
	 * );
	 *
	 * Operation applyOp = CdtOperation.modifyByPath("myBin", ModifyFlags.DEFAULT, modifyExp,
	 *     bookKey, allChildren, priceKey);
	 * Record result = client.operate(null, key, applyOp);
	 * }</pre>
	 *
	 * @param binName		bin name
	 * @param flags			modify flags from {@link ModifyFlags}
	 * @param modifyExp		compiled modify {@link Expression}
	 * @param ctx			context path using {@link CTX} methods
	 * @return				a CDT modify {@link Operation}
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
