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

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

public class LuaMapIterator<K,V> extends LuaUserdata implements LuaIterator {

	protected final Iterator<Entry<K,V>> iter;

	public LuaMapIterator(Iterator<Entry<K,V>> iter) {
		super(iter);
		this.iter = iter;
	}

	public final boolean hasNext() {
		return iter.hasNext();
	}

	public Varargs next() {
		Entry<K,V> entry = (Entry<K,V>)iter.next();
		LuaValue key = (entry.getKey() instanceof LuaValue)? (LuaValue)entry.getKey() : new LuaUserdata(entry.getKey());
		LuaValue value = (entry.getValue() instanceof LuaValue)? (LuaValue)entry.getValue() : new LuaUserdata(entry.getValue());
		return LuaValue.varargsOf(new LuaValue[] {key, value});
	}
}
