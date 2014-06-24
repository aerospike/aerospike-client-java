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

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;

import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;

public final class LuaAerospikeLib extends OneArgFunction {

	private final LuaInstance instance;
	
	public LuaAerospikeLib(LuaInstance instance) {
		this.instance = instance;
	}

	public LuaValue call(LuaValue env) {		
		LuaTable meta = new LuaTable();
		meta.set("__index", new index());
		
		LuaTable table = new LuaTable();		
		table.setmetatable(meta);
		
		instance.registerPackage("aerospike", table);		
		return table;
	}

	public static final class index extends TwoArgFunction {
		private ThreeArgFunction func = new log();
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			return func;
		}
	}

	public static final class log extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			String message = arg3.toString();
			
			switch(arg2.toint()) {
			case 1:
				Log.log(Level.WARN, message);
				break;
			case 2:
				Log.log(Level.INFO, message);
				break;
			case 3:
			case 4:
				Log.log(Level.DEBUG, message);
				break;
			}
			
			return NIL;
		}
	}
}
