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
