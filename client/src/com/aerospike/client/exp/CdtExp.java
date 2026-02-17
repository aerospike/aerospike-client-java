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
     * Create a CDT select expression.
     * @param returnType
     * @param flags
     * @param bin
     * @param ctx
     * @return
     */
    public static Exp selectByPath(Exp.Type returnType, int flags, Exp bin, CTX... ctx) {
        byte[] bytes = packCdtSelect(Type.SELECT, flags, ctx);

        return new Exp.Module(bin, bytes, returnType.code, MODULE);
    }

    /**
     * Create a CDT modify expression.
     * @param returnType
     * @param flags
     * @param modifyExp
     * @param bin
     * @param ctx
     * @return
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
