/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.lua;

import java.util.Iterator;
import java.util.Map.Entry;

import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

public class LuaValueMapIterator extends LuaMapIterator<LuaValue,LuaValue> {

	public LuaValueMapIterator(Iterator<Entry<LuaValue,LuaValue>> iter) {
		super(iter);
	}

	public Varargs next() {
		Entry<LuaValue,LuaValue> entry = iter.next();
		return LuaValue.varargsOf(new LuaValue[] {entry.getKey(), entry.getValue()});
	}
}
