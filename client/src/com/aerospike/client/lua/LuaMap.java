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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaMap extends LuaUserdata implements LuaData {

	private final Map<LuaValue,LuaValue> map;

	public LuaMap(LuaInstance instance, Map<LuaValue,LuaValue> map) {
		super(map);
		this.map = map;
		setmetatable(instance.getPackage("Map"));
	}

	public void put(LuaValue key, LuaValue value) {
		map.put(key, value);
	}
	
	public LuaValue get(LuaValue key) {
		return map.get(key);
	}

	public Iterator<Entry<LuaValue,LuaValue>> entrySetIterator() {
		return map.entrySet().iterator();
	}
	
	public Iterator<LuaValue> keySetIterator() {
		return map.keySet().iterator();
	}

	public Iterator<LuaValue> valuesIterator() {
		return map.values().iterator();
	}

	public LuaInteger size() {
		return LuaInteger.valueOf(map.size());
	}

	public LuaString toLuaString() {
		return LuaString.valueOf(map.toString());
	}
	
	public Object luaToObject() {
		Map<Object,Object> target = new HashMap<Object,Object>(map.size());
		
		for (Entry<LuaValue,LuaValue> entry : map.entrySet()) {
			Object key = LuaUtil.luaToObject(entry.getKey());
			Object value = LuaUtil.luaToObject(entry.getValue());
			target.put(key, value);
		}
		return target;
	}
}
