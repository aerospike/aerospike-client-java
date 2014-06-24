/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.lua;

import org.luaj.vm2.LuaValue;

import com.aerospike.client.Log;

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
		
		case LuaValue.TNIL:
			return null;
			
		default:
			if (Log.warnEnabled()) {
				Log.warn("Invalid lua type: " + source.type());
			}
			return null;
		}
	}	
}
