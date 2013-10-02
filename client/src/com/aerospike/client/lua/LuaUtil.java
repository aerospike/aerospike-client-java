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

import org.luaj.vm2.LuaValue;

import com.aerospike.client.Log;
import com.aerospike.client.query.ResultSet;

public final class LuaUtil {
	
	public static Object luaToObject(LuaValue source) {
		switch (source.type()) {
		case LuaValue.TNUMBER:
			return source.tolong();
		
		case LuaValue.TBOOLEAN:
			return source.toboolean();
			
		case LuaValue.TSTRING:
			return source.tojstring();
			
		case LuaValue.TUSERDATA:
			LuaData data = (LuaData)source;
			return data.luaToObject();
			
		default:
			if (Log.warnEnabled()) {
				Log.warn("Invalid lua type: " + source.type());
			}
			return ResultSet.END;
		}
	}	
}
