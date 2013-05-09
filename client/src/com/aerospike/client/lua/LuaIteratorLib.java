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
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

public final class LuaIteratorLib extends OneArgFunction {

	public LuaIteratorLib() {
	}

	private LuaTable init() {
		LuaTable table = new LuaTable();
		bind(table, Arg1.class, new String[] {"has_next"});
		bind(table, ArgV.class, new String[] {"next"});
		
		String packageName = "iterator";
		env.set(packageName, table);
		PackageLib.instance.LOADED.set(packageName, table);
		return table;
	}
	
	public LuaValue call(LuaValue arg) {
		switch (opcode) {
		case 0: // init library
			return init();
		}
		return NIL;
	}

	public static final class Arg1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaIterator iter = (LuaIterator)arg;
			
			switch (opcode) {
			case 0:  // has_next
				return LuaBoolean.valueOf(iter.hasNext());
			}
			return NIL;
		}
	}
	
	public static final class ArgV extends VarArgFunction {
		@Override
		public Varargs invoke(Varargs args) {
			LuaIterator iter = (LuaIterator)arg(1);
			
			switch (opcode) {
			case 0: // next
				return iter.next();
			}
			return NONE;
		}
	}
}
