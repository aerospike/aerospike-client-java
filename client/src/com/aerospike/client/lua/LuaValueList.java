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

import java.util.List;

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaValueList extends LuaList<LuaValue> {

	public LuaValueList(List<LuaValue> list) {
		super(list);
	}
	
	@Override
	public LuaValue getValue(LuaValue index) {
		return list.get(index.toint() - 1);
	}

	@Override
	public void setValue(LuaValue index, LuaValue value) {
		list.set(index.toint() - 1, value);
	}

	@Override
	public void append(LuaValue value) {
		list.add(value);
	}
	
	@Override
	public void append(int index, LuaValue value) {
		list.add(index, value);
	}

	@Override
	public LuaList<LuaValue> subList(int begin, int end) {		
		return new LuaValueList(list.subList(begin, end));
	}

	@Override
	public LuaUserdata iterator() {
		return new LuaValueListIterator(list.iterator());
	}
}
