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
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

public final class LuaMapLib extends OneArgFunction {

	private final LuaInstance instance;
	
	public LuaMapLib(LuaInstance instance) {
		this.instance = instance;
		instance.load(new MetaLib(instance));
	}

	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,1);
		meta.set("__call", new create(instance));
		
		LuaTable table = new LuaTable(0,10);
		table.setmetatable(meta);
		table.set("size", new size());
		iterator iter = new iterator();
		table.set("iterator", iter);
		table.set("pairs", iter);
		table.set("keys", new keys());
		table.set("values", new values());
		
		instance.registerPackage("map", table);
		return table;
	}

	public static final class create extends VarArgFunction {
		private final LuaValue meta;
		
		public create(LuaInstance instance) {
			this.meta = instance.getPackage("Map");
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			LuaValueMap map = new LuaValueMap(new HashMap<LuaValue,LuaValue>());
			map.setmetatable(meta);
			
			if (args.istable(2)) {
				LuaTable table = args.checktable(2);
				LuaValue k = LuaValue.NIL;
				
				while (true) {
					 Varargs n = table.next(k);
					 
					 if ((k = n.arg1()).isnil())
						 break;

					 LuaValue v = n.arg(2);
					 map.setValue(k, v);
				 }
			}				
			return LuaValue.varargsOf(new LuaValue[] {map});
		}
	}

	public static final class size extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			return map.size();
		}
	}

	public static final class iterator extends OneArgFunction {		
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			return map.iterator();
		}
	}

	public static final class keys extends OneArgFunction {		
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			return map.keys();
		}
	}

	public static final class values extends OneArgFunction {		
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap<?,?> map = (LuaMap<?,?>)arg;
			return map.values();
		}
	}

	public static final class MetaLib extends OneArgFunction {

		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable();
			meta.set("__len", new len());
			meta.set("__tostring", new tostring());
			meta.set("__index", new index());
			meta.set("__newindex", new newindex());
			
			instance.registerPackage("Map", meta);
			return meta;
		}
			
		public static final class len extends OneArgFunction {
			@Override
			public LuaValue call(LuaValue arg) {
				LuaMap<?,?> map = (LuaMap<?,?>)arg;
				return map.size();
			}
		}

		public static final class tostring extends OneArgFunction {
			@Override
			public LuaValue call(LuaValue arg) {
				LuaMap<?,?> map = (LuaMap<?,?>)arg;
				return map.toLuaString();
			}
		}

		public static final class index extends TwoArgFunction {
			@Override
			public LuaValue call(LuaValue arg1, LuaValue arg2) {
				LuaMap<?,?> map = (LuaMap<?,?>)arg1;
				return map.getValue(arg2);
			}
		}

		public static final class newindex extends ThreeArgFunction {
			@Override
			public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
				LuaMap<?,?> map = (LuaMap<?,?>)arg1;
				map.setValue(arg2, arg3);
				return NIL;
			}
		}
	}
}
