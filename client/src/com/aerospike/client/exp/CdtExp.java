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

import com.aerospike.client.cdt.CTX;
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
     * Create a CDT select expression that selects values at the given context path.
     * Use with {@link Exp#build} and filter/expression policies or secondary index expressions.
     * <p>Select list element at index 0 from bin "tags" for use in a filter or policy.</p>
     * <pre>{@code
     * // Select list element at index 0 from bin "tags"
     * Exp exp = CdtExp.selectByPath(Exp.Type.LIST, Exp.SELECT_LIST_VALUE, Exp.bin("tags", Exp.Type.LIST), CTX.listIndex(0));
     * Expression filter = Exp.build(exp);
     * }</pre>
     *
     * @param returnType	type of the selected value (e.g. {@link Exp.Type#INT}, {@link Exp.Type#LIST})
     * @param flags		select flags (e.g. {@link Exp#SELECT_LIST_VALUE}, {@link Exp#SELECT_MAP_KEY_VALUE})
     * @param bin		bin expression (e.g. {@link Exp#bin}); must not be null
     * @param ctx		context path into nested CDT (e.g. {@link CTX#listIndex}); may be empty for top-level
     * @return		expression that selects by path
     */
    public static Exp selectByPath(Exp.Type returnType, int flags, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtSelect(Type.SELECT, flags, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE);
    }

    /**
     * Create a CDT modify expression that modifies values at the given context path.
     * Use with {@link Exp#build} in write/expression policies.
     * <p>Increment integer at map key "count" in bin "stats" with a modify expression.</p>
     * <pre>{@code
     * // Increment integer at map key "count" in bin "stats"
     * Exp modExp = CdtExp.modifyByPath(Exp.Type.INT, CdtExp.MODIFY, Exp.add(Exp.val(1)), Exp.bin("stats", Exp.Type.MAP), CTX.mapKey(com.aerospike.client.Value.get("count")));
     * Expression exp = Exp.build(modExp);
     * }</pre>
     *
     * @param returnType	type of the modified value
     * @param modifyFlag	modify flag (e.g. {@link #MODIFY}, {@link Exp#MODIFY_APPLY})
     * @param modifyExp	expression to apply at the path (e.g. {@link Exp#add}); must not be null
     * @param bin		bin expression (e.g. {@link Exp#bin}); must not be null
     * @param ctx		context path into nested CDT; may be empty for top-level
     * @return		expression that modifies by path
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
