/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.lua;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

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
		LuaTable meta = new LuaTable(0,2);
		meta.set("__call", new create(instance));
		
		LuaTable table = new LuaTable(0,8);
		table.setmetatable(meta);
		table.set("size", new len());
		table.set("pairs", new iterator());
		table.set("keys", new keys());
		table.set("values", new values());
		// map.clone and map.merge are defined in as.lua.
		//table.set("clone", new clone());
		//table.set("merge", new merge());
		
		instance.registerPackage("map", table);
		return table;
	}

	public static final class MetaLib extends OneArgFunction {
		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable(0,5);
			meta.set("__len", new len());
			meta.set("__tostring", new tostring());
			meta.set("__index", new index());
			meta.set("__newindex", new newindex());
			
			instance.registerPackage("Map", meta);
			return meta;
		}			
	}

	public static final class create extends VarArgFunction {
		private final LuaInstance instance;
		
		public create(LuaInstance instance) {
			this.instance = instance;
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			LuaMap map = new LuaMap(instance, new HashMap<LuaValue,LuaValue>());
			
			if (args.istable(2)) {
				LuaTable table = args.checktable(2);
				LuaValue k = LuaValue.NIL;
				
				while (true) {
					 Varargs n = table.next(k);
					 
					 if ((k = n.arg1()).isnil())
						 break;

					 LuaValue v = n.arg(2);
					 map.put(k, v);
				 }
			}				
			return LuaValue.varargsOf(new LuaValue[] {map});
		}
	}

	public static final class iterator extends OneArgFunction {		
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap map = (LuaMap)arg;
			return new nextLuaValue(map.entrySetIterator());
		}
	}

	public static final class nextLuaValue extends VarArgFunction {
		private final Iterator<Entry<LuaValue,LuaValue>> iter;
		
		public nextLuaValue(Iterator<Entry<LuaValue,LuaValue>> iter) {
			this.iter = iter;
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			if (iter.hasNext()) {
				Entry<LuaValue,LuaValue> entry = iter.next();
				return LuaValue.varargsOf(new LuaValue[] {entry.getKey(), entry.getValue()});
			}
			return NONE;
		}
	}

	public static final class keys extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue m) {
			LuaMap map = (LuaMap)m;
			return new LuaListLib.nextLuaValue(map.keySetIterator());
		}
	}

	public static final class values extends OneArgFunction {	
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap map = (LuaMap)arg;
			return new LuaListLib.nextLuaValue(map.valuesIterator());
		}
	}

	public static final class len extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaMap map = (LuaMap)arg;
			return map.size();
		}
	}

	public static final class tostring extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue m) {
			LuaMap map = (LuaMap)m;
			return map.toLuaString();
		}
	}

	public static final class index extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue m, LuaValue key) {
			LuaMap map = (LuaMap)m;
			return map.get(key);
		}
	}

	public static final class newindex extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue m, LuaValue key, LuaValue value) {
			LuaMap map = (LuaMap)m;
			map.put(key, value);
			return NIL;
		}
	}
}
