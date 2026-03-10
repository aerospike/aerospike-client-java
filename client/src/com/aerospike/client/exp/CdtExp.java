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
package com.aerospike.client.exp;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ModifyFlags;
import com.aerospike.client.cdt.SelectFlags;
import com.aerospike.client.util.Packer;

public class CdtExp {
    /**
     * The module identifier for CDT expressions.
     */
	private static final int MODULE = 0;

    /**
     * The modify flag for CDT expressions.
     */
	public static final int MODIFY = 0x40;

    /**
     * The type of CDT expression.
     */
    enum Type {
        /**
         * The identifier for SELECT CDT expressions.
         */
        SELECT(0xfe);

        int value;

        Type(int value) {
            this.value = value;
        }
    }

	/**
	 * Create a CDT select expression that traverses a nested CDT structure and returns
	 * matching values. The expression evaluates to the type specified by {@code returnType}.
	 * <p>
	 * The {@code returnType} should match the expected result shape:
	 * <ul>
	 * <li>{@link Exp.Type#LIST} when using {@link SelectFlags#VALUE}, {@link SelectFlags#MAP_KEY},
	 *     or {@link SelectFlags#MAP_KEY_VALUE}.</li>
	 * <li>{@link Exp.Type#MAP} when using {@link SelectFlags#MATCHING_TREE} on a map bin.</li>
	 * </ul>
	 * <p>
	 * Valid {@code flags} are defined in {@link SelectFlags} and can be combined with bitwise OR
	 * (e.g. {@code SelectFlags.VALUE | SelectFlags.NO_FAIL}).
	 * <p>
	 * The context path ({@code ctx}) uses {@link CTX} methods such as {@link CTX#mapKey(Value)},
	 * {@link CTX#allChildren()}, and {@link CTX#allChildrenWithFilter(Exp)}.
	 *
	 * <pre>{@code
	 * // Select all prices from a list of book maps stored under a "book" key.
	 * CTX bookKey = CTX.mapKey(Value.get("book"));
	 * CTX allChildren = CTX.allChildren();
	 * CTX priceKey = CTX.mapKey(Value.get("price"));
	 *
	 * Expression selectExp = Exp.build(
	 *     CdtExp.selectByPath(
	 *         Exp.Type.LIST,
	 *         SelectFlags.VALUE,
	 *         Exp.mapBin("myBin"),
	 *         bookKey, allChildren, priceKey
	 *     )
	 * );
	 * }</pre>
	 *
	 * @param returnType	expected result type ({@link Exp.Type#LIST} or {@link Exp.Type#MAP})
	 * @param flags			select flags from {@link SelectFlags}
	 * @param bin			source bin expression (e.g. {@link Exp#mapBin(String)})
	 * @param ctx			context path using {@link CTX} methods
	 * @return				an {@link Exp} that evaluates to the type specified by {@code returnType}
	 */
    public static Exp selectByPath(Exp.Type returnType, int flags, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtSelect(Type.SELECT, flags, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE);
    }

	/**
	 * Create a CDT modify expression that traverses a nested CDT structure and applies
	 * a modification expression at each matching location. Returns the entire modified
	 * CDT structure as the type specified by {@code returnType}.
	 * <p>
	 * The {@code returnType} should typically be {@link Exp.Type#MAP} or {@link Exp.Type#LIST}
	 * to match the top-level type of the bin being modified.
	 * <p>
	 * Valid {@code modifyFlag} values are defined in {@link ModifyFlags} and can be combined
	 * with bitwise OR (e.g. {@code ModifyFlags.APPLY | ModifyFlags.NO_FAIL}).
	 * <p>
	 * The {@code modifyExp} can reference the current value via loop variable expressions
	 * such as {@link Exp#floatLoopVar(LoopVarPart)}.
	 * <p>
	 * The context path ({@code ctx}) uses {@link CTX} methods such as {@link CTX#mapKey(Value)},
	 * {@link CTX#allChildren()}, and {@link CTX#allChildrenWithFilter(Exp)}.
	 *
	 * <pre>{@code
	 * // Multiply all book prices by 1.50.
	 * CTX bookKey = CTX.mapKey(Value.get("book"));
	 * CTX allChildren = CTX.allChildren();
	 * CTX priceKey = CTX.mapKey(Value.get("price"));
	 *
	 * Exp modifyExp = Exp.mul(
	 *     Exp.floatLoopVar(LoopVarPart.VALUE),
	 *     Exp.val(1.50)
	 * );
	 *
	 * Expression applyExp = Exp.build(
	 *     CdtExp.modifyByPath(
	 *         Exp.Type.MAP,
	 *         ModifyFlags.DEFAULT,
	 *         modifyExp,
	 *         Exp.mapBin("myBin"),
	 *         bookKey, allChildren, priceKey
	 *     )
	 * );
	 * }</pre>
	 *
	 * @param returnType	expected result type ({@link Exp.Type#MAP} or {@link Exp.Type#LIST})
	 * @param modifyFlag	modify flags from {@link ModifyFlags}
	 * @param modifyExp		expression to apply at each matched location
	 * @param bin			source bin expression (e.g. {@link Exp#mapBin(String)})
	 * @param ctx			context path using {@link CTX} methods
	 * @return				an {@link Exp} containing the entire modified CDT structure
	 */
    public static Exp modifyByPath(Exp.Type returnType, int modifyFlag, Exp modifyExp, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtModify(Type.SELECT, modifyFlag, modifyExp, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE | MODIFY);
    }

	private static byte[] packCdtModify(Type type, int modifyFlag, Exp modifyExp, CTX... ctx) {
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
            modifyExp.pack(packer);

            if (i == 0) {
                packer.createBuffer();
            }

        }

        return packer.getBuffer();
	}

	private static byte[] packCdtSelect(Type type, int selectFlag, CTX... ctx) {
        Packer packer = new Packer();

        for (int i = 0; i < 2; i++) {
            packer.packArrayBegin(3);
		    packer.packInt(type.value);
		    packer.packArrayBegin(ctx.length * 2);

            for (CTX c : ctx) {
                packer.packInt(c.id);
                if (c.value != null)
                    c.value.pack(packer);
                else 
		            packer.packByteArray(c.exp.getBytes(), 0, c.exp.getBytes().length);
            }

            packer.packInt(selectFlag);

            if (i == 0) {
                packer.createBuffer();
            }

        }

        return packer.getBuffer();
	}
}
