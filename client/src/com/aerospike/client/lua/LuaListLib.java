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
import java.util.Iterator;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;

import com.aerospike.client.lua.LuaList.LuaValueList;

public final class LuaListLib extends OneArgFunction {

	private final LuaInstance instance;
	
	public LuaListLib(LuaInstance instance) {
		this.instance = instance;
		instance.load(new MetaLib(instance));
	}

	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,2);
		meta.set("__call", new create(instance));
		
		LuaTable table = new LuaTable(0,8);
		table.setmetatable(meta);
		table.set("size", new len());
		table.set("iterator", new iterator());
		table.set("append", new append());
		table.set("prepend", new prepend());
		table.set("take", new take());
		table.set("drop", new drop());
		table.set("tostring", new tostring());
		
		instance.registerPackage("list", table);
		return table;
	}

	public static final class MetaLib extends OneArgFunction {

		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable(0, 5);
			meta.set("__len", new len());
			meta.set("__tostring", new tostring());
			meta.set("__index", new index());
			meta.set("__newindex", new newindex());
			
			instance.registerPackage("List", meta);
			return meta;
		}
	}

	public static final class create extends VarArgFunction {
		private final LuaValue meta;
		
		public create(LuaInstance instance) {
			this.meta = instance.getPackage("List");
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			LuaValueList list = new LuaValueList(new ArrayList<LuaValue>());
			list.setmetatable(meta);
			
			if (args.istable(2)) {
				LuaTable table = args.checktable(2);
				LuaValue k = LuaValue.NIL;
				
				while (true) {
					 Varargs n = table.next(k);
					 
					 if ((k = n.arg1()).isnil())
						 break;

					 LuaValue v = n.arg(2);
					 list.append(v);
				 }
			}				
			return LuaValue.varargsOf(new LuaValue[] {list});
		}
	}

	public static final class iterator extends OneArgFunction {		
		@SuppressWarnings("unchecked")
		@Override
		public LuaValue call(LuaValue arg) {
			LuaList<?> list = (LuaList<?>)arg;
			
			if (list instanceof LuaValueList) {
				return new nextLuaValue((Iterator<LuaValue>)list.list.iterator());
			}
			else {
				return new nextObject(list.list.iterator());				
			}
		}
	}
	
	public static final class nextLuaValue extends ZeroArgFunction {
		private final Iterator<LuaValue> iter;
		
		public nextLuaValue(Iterator<LuaValue> iter) {
			this.iter = iter;
		}
		
		@Override
		public LuaValue call() {
			return (iter.hasNext())? iter.next() : LuaValue.NIL;
		}
	}

	public static final class nextObject extends ZeroArgFunction {
		private final Iterator<?> iter;
		
		public nextObject(Iterator<?> iter) {
			this.iter = iter;
		}
		
		@Override
		public LuaValue call() {
			if (iter.hasNext()) {
				Object obj = iter.next();
				return LuaUtil.objectToLua(obj);				
			}
			return NIL;
		}
	}

	public static final class append extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
			list.append(arg2);
			return NIL;
		}
	}

	public static final class prepend extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
			list.append(0, arg2);
			return NIL;
		}
	}
	
	public static final class take extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
			return list.take(arg2);
		}
	}
	
	public static final class drop extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
			return list.drop(arg2);
		}
	}
	
	public static final class len extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaList<?> list = (LuaList<?>)arg;
			return LuaInteger.valueOf(list.list.size());
		}
	}

	public static final class tostring extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaList<?> list = (LuaList<?>)arg;
			return LuaString.valueOf(list.list.toString());
		}
	}

	public static final class index extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaList<?> list = (LuaList<?>)arg1;
			return list.getValue(arg2);
		}
	}

	public static final class newindex extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			LuaList<?> list = (LuaList<?>)arg1;
			int index = arg2.toint();
			list.ensureSize(index);
			list.setValue(index - 1, arg3);
			return NIL;
		}
	}
}
