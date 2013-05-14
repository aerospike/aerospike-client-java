package com.aerospike.client.lua;

import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaUtil {
	
	@SuppressWarnings("unchecked")
	public static LuaValue objectToLua(Object obj) {
		if (obj instanceof LuaValue) {
			return (LuaValue)obj;
		}

		if (obj == null) {
			return LuaValue.NIL;
		}
		
		if (obj instanceof String) {
			return LuaString.valueOf((String)obj);
		}
		
		if (obj instanceof byte[]) {
			return LuaValue.valueOf((byte[])obj);
		}
		
		if (obj instanceof Integer) {
			return LuaInteger.valueOf((Integer)obj);
		}
		
		if (obj instanceof Long) {
			return LuaInteger.valueOf((Long)obj);
		}

		if (obj instanceof com.aerospike.client.Value) {
			com.aerospike.client.Value value = (com.aerospike.client.Value) obj;
			return value.getLuaValue();
		}
		
		if (obj instanceof List<?>) {
			return new LuaList<Object>((List<Object>) obj);
		}
		
		if (obj instanceof Map<?,?>) {
			return new LuaMap<Object,Object>((Map<Object,Object>) obj);
		}
		return new LuaUserdata(obj);
	}
}
