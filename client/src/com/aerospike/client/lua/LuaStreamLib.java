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

import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.TwoArgFunction;

public final class LuaStreamLib extends OneArgFunction {
	
	public LuaStreamLib() {
	}
	
	private LuaTable init() {
		LuaTable meta = new LuaTable();
		bind(meta, Meta1.class, new String[] {"__tostring"});
		
		LuaTable table = new LuaTable();
		table.setmetatable(meta);
		bind(table, Arg1.class, new String[] {"read", "readable", "writable"});
		bind(table, Arg2.class, new String[] {"write"});
		
		String packageName = "stream";
		env.set(packageName, table);
		PackageLib.instance.LOADED.set(packageName, table);
		return table;
	}

	@Override
	public LuaValue call(LuaValue arg) {
		switch (opcode) {
		case 0: // init library
			return init();
		}
		return NIL;
	}

	public static final class Meta1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			
			switch (opcode) {
			case 0:  // __tostring
				return stream.toLuaString();
			}
			return NIL;
		}
	}

	public static final class Arg1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			
			switch (opcode) {
			case 0:  // read
				return stream.read();
				
			case 1:  // readable
				return LuaBoolean.valueOf(stream.readable());
				
			case 2:  // writable
				return LuaBoolean.valueOf(stream.writeable());
			}
			return NIL;
		}
	}

	public static final class Arg2 extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaStream stream = (LuaStream)arg1;
			
			switch (opcode) {
			case 0:  // write
				stream.write(arg2);
				return NIL;
			}
			return NIL;
		}
	}
}
