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

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public class LuaMap<K,V> extends LuaUserdata {

	protected final Map<K,V> map;

	public LuaMap(Map<K,V> map) {
		super(map);
		this.map = map;
	}
	
	public LuaInteger size() {
		return LuaInteger.valueOf(map.size());
	}
	
	public LuaString toLuaString() {
		return LuaString.valueOf(map.toString());
	}

	public LuaValue getValue(LuaValue key) {
		Object object = map.get(getObject(key));
		return (object instanceof LuaValue)? (LuaValue)object : new LuaUserdata(object);
	}

	@SuppressWarnings("unchecked")
	public void setValue(LuaValue key, LuaValue value) {
		map.put((K)getObject(key), (V)value);
	}

	public LuaValue keys() {
		return new LuaListIterator<K>(map.keySet().iterator());
	}

	public LuaValue values() {
		return new LuaListIterator<V>(map.values().iterator());
	}

	public LuaUserdata iterator() {
		return new LuaMapIterator<K,V>(map.entrySet().iterator());
	}
	
	public static Object getObject(LuaValue val) {
		switch (val.type()) {
		case LuaValue.TBOOLEAN:
			return val.toboolean();
			
		case LuaValue.TNUMBER:
			return val.tolong();
			
		case LuaValue.TSTRING:
			return val.tostring();
			
		case LuaValue.TINT:
			return val.toint();
		
		case LuaValue.TNIL:
		default:
			return null;
		}
	}
}
