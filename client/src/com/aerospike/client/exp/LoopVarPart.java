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

/**
 * Which part of a loop variable to use in iteration expressions (e.g. in {@link com.aerospike.client.cdt.CTX#allChildrenWithFilter(Exp)} and CDT apply).
 * <p>
 * Use with {@link Exp#mapLoopVar}, {@link Exp#stringLoopVar}, {@link Exp#intLoopVar}, {@link Exp#floatLoopVar} when building filter/apply expressions over map or list elements.
 * <p>Filter nested children by loop variable key or value.</p>
 * <pre>{@code
 * CTX ctx = CTX.allChildrenWithFilter(Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")));
 * CTX ctx2 = CTX.allChildrenWithFilter(Exp.le(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT, Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)), Exp.val(10.0)));
 * }</pre>
 *
 * @see Exp#mapLoopVar
 * @see Exp#stringLoopVar
 * @see com.aerospike.client.cdt.CTX#allChildrenWithFilter(Exp)
 * @see com.aerospike.client.cdt.CdtOperation#modifyByPath
 */
public enum LoopVarPart {
	/**
	 * Access the key part of the loop variable.
	 * For maps, this refers to the map key.
	 * For lists, this refers to the list index.
	 */
	MAP_KEY(0),

	/**
	 * Access the value part of the loop variable.
	 * For maps, this refers to the map value.
	 * For lists, this refers to the list item value.
	 */
	VALUE(1),

	/**
	 * Returns a list of indexes.
	 */
	INDEX(2);
	
	
	public final int id;
	
	private LoopVarPart(int id) {
		this.id = id;
	}
}
