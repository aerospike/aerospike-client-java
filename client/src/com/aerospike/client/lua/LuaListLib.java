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

import java.util.ArrayList;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;

public final class LuaListLib extends OneArgFunction {

	public LuaListLib() {
	}

	private LuaTable init() {
		LuaTable meta = new LuaTable();
		bind(meta, Meta0.class, new String[] { "__call"});
		bind(meta, Meta1.class, new String[] { "__len", "__tostring"});
		bind(meta, Meta2.class, new String[] { "__index"});
		bind(meta, Meta3.class, new String[] { "__newindex"});
		
		LuaTable table = new LuaTable();
		table.setmetatable(meta);
		
		bind(table, Arg1.class, new String[] {"size", "iterator"});
		bind(table, Arg2.class, new String[] {"append", "prepend", "take", "drop"});
		
		String packageName = "list";
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
			case 0: // __call
				return new LuaValueList(new ArrayList<LuaValue>());
			}
			return NIL;
		}
	}

	public static final class Meta1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaList<?> list = (LuaList<?>)arg;
			
			switch (opcode) {
			case 0:  // __len
				return list.size();
				
			case 1:  // __tostring
				return list.toLuaString();
			}
			return NIL;
		}
	}

	public static final class Meta2 extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
						
			switch (opcode) {
			case 0:  // __index
				return list.getValue(arg2);
			}
			return NIL;
		}
	}

	public static final class Meta3 extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			if (arg1 instanceof LuaList<?>) {
				LuaList<?> list = (LuaList<?>)arg1;
				
				switch (opcode) {
				case 0:  // __newindex
					list.setValue(arg2, arg3);
					return NIL;
				}
			}
			return NIL;
		}
	}

	public static final class Arg1 extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaList<?> list = (LuaList<?>)arg;
			
			switch (opcode) {
			case 0:  // size
				return list.size();
				
			case 1:  // iterator
				return list.iterator();				
			}
			return NIL;
		}
	}
	
	public static final class Arg2 extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
						
			switch (opcode) {
			case 0:  // append
				list.append(arg2);
				return NIL;
				
			case 1:  // prepend
				list.append(0, arg2);
				return NIL;
				
			case 2:  // take
				return list.take(arg2);

			case 3:  // drop
				return list.drop(arg2);
			}
			return NIL;
		}
	}
}
