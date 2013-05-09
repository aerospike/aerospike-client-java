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

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public class LuaListIterator<T> extends LuaUserdata implements LuaIterator {

	protected final Iterator<T> iter;

	public LuaListIterator(Iterator<T> iter) {
		super(iter);
		this.iter = iter;
	}

	@Override
	public final boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public LuaValue next() {
		Object object = iter.next();
		return (object instanceof LuaValue)? (LuaValue)object : new LuaUserdata(object);
	}
}
