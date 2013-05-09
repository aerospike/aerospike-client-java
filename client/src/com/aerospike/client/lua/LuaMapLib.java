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

import java.util.HashMap;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;

public final class LuaMapLib extends OneArgFunction {

	public LuaMapLib() {
	}

	private LuaTable init() {
		LuaTable meta = new LuaTable();
		bind(meta, Meta0.class, new String[] { "__call"});
		bind(meta, Meta1.class, new String[] { "__len", "__tostring"});
		bind(meta, Meta2.class, new String[] { "__index"});
		bind(meta, Meta3.class, new String[] { "__newindex"});
		
		LuaTable table = new LuaTable();
		table.setmetatable(meta);
		bind(table, Arg1.class, new String[] {"size", "pairs", "iterator", "keys", "values"});
		
		String packageName = "map";
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

	public static final class Meta0 extends ZeroArgFunction {
		@Override
		public LuaValue call() {
			switch (opcode) {
			case 0:  // __call
				return new LuaValueMap(new HashMap<LuaValue,LuaValue>());
			}
			return NIL;
		}
	}

	public static final class Meta1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			
			switch (opcode) {
			case 0:  // __len
				return map.size();
				
			case 1:  // __tostring
				return map.toLuaString();
			}
			return NIL;
		}
	}

	public static final class Meta2 extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg1;
						
			switch (opcode) {
			case 0:  // __index
				return map.getValue(arg2);
			}
			return NIL;
		}
	}

	public static final class Meta3 extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			if (arg1 instanceof LuaMap<?,?>) {
				LuaMap<?,?> map = (LuaMap<?,?>)arg1;
				
				switch (opcode) {
				case 0:  // __newindex
					map.setValue(arg2, arg3);
					return NIL;
				}
			}
			return NIL;
		}
	}

	public static final class Arg1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			
			switch (opcode) {
			case 0:  // size
				return map.size();
				
			case 1:  // pairs
			case 2:  // iterator
				return map.iterator();
			
			case 3: // keys
				return map.keys();
				
			case 4: // values
				return map.values();
			}
			return NIL;
		}
	}	
}
