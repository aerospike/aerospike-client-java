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

public class LuaMap<K,V> extends LuaUserdata {

	protected final Map<K,V> map;

	public LuaMap(Map<K,V> map) {
		super(map);
		this.map = map;
	}
	
	public LuaValue getValue(LuaValue key) {
		Object object = map.get(getKeyObject(key));
		return (object instanceof LuaValue)? (LuaValue)object : new LuaUserdata(object);
	}

	@SuppressWarnings("unchecked")
	public final void setValue(LuaValue key, LuaValue value) {
		map.put((K)getKeyObject(key), (V)value);
	}
	
	public static final Object getKeyObject(LuaValue val) {
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
	
	public static final class LuaValueMap extends LuaMap<LuaValue,LuaValue> {

		public LuaValueMap(Map<LuaValue,LuaValue> map) {
			super(map);
		}
		
		@Override
		public LuaValue getValue(LuaValue key) {
			return map.get(getKeyObject(key));
		}
	}
}
