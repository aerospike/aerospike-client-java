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

import java.util.ArrayList;
import java.util.Iterator;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;

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
		// list.clone is defined in as.lua
		// table.set("clone", new clone());
		
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
		private final LuaInstance instance;
		
		public create(LuaInstance instance) {
			this.instance = instance;
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			LuaList list = new LuaList(instance, new ArrayList<LuaValue>());
			
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
		@Override
		public LuaValue call(LuaValue l) {
			LuaList list = (LuaList)l;
			return new nextLuaValue(list.iterator());
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

	public static final class append extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue value) {
			LuaList list = (LuaList)l;
			list.append(value);
			return NIL;
		}
	}

	public static final class prepend extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue value) {
			LuaList list = (LuaList)l;
			list.prepend(value);
			return NIL;
		}
	}
	
	public static final class take extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue count) {
			LuaList list = (LuaList)l;
			return list.take(count);
		}
	}
	
	public static final class drop extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue count) {
			LuaList list = (LuaList)l;
			return list.drop(count);
		}
	}
	
	public static final class len extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue l) {
			LuaList list = (LuaList)l;
			return list.size();
		}
	}

	public static final class tostring extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue l) {
			LuaList list = (LuaList)l;
			return list.toLuaString();
		}
	}

	public static final class index extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue index) {
			LuaList list = (LuaList)l;
			return list.get(index);
		}
	}

	public static final class newindex extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue l, LuaValue index, LuaValue value) {
			LuaList list = (LuaList)l;
			list.set(index, value);
			return NIL;
		}
	}
}
