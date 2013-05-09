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

import java.util.Map;

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaValueMap extends LuaMap<LuaValue,LuaValue> {

	public LuaValueMap(Map<LuaValue,LuaValue> map) {
		super(map);
	}
	
	@Override
	public LuaValue getValue(LuaValue key) {
		return map.get(getObject(key));
	}

	@Override
	public LuaValue keys() {
		return new LuaValueListIterator(map.keySet().iterator());
	}

	@Override
	public LuaValue values() {
		return new LuaValueListIterator(map.values().iterator());
	}

	@Override
	public LuaUserdata iterator() {
		return new LuaValueMapIterator(map.entrySet().iterator());
	}
}
