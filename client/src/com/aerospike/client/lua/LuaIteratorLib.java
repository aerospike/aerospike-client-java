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
import org.luaj.vm2.lib.VarArgFunction;

public final class LuaIteratorLib extends OneArgFunction {

	private final LuaInstance instance;

	public LuaIteratorLib(LuaInstance instance) {
		this.instance = instance;
	}

	public LuaValue call(LuaValue env) {
		LuaTable table = new LuaTable(0,5);
		table.set("has_next", new hasnext());
		table.set("next", new next());
		
		instance.registerPackage("iterator", table);
		return table;
	}

	public static final class hasnext extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaIterator iter = (LuaIterator)arg;
			return LuaBoolean.valueOf(iter.hasNext());
		}
	}
	
	public static final class next extends VarArgFunction {
		@Override
		public Varargs invoke(Varargs args) {
			LuaIterator iter = (LuaIterator)arg(1);
			return iter.next();
		}
	}
}
