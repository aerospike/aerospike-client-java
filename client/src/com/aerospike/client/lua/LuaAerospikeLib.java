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
