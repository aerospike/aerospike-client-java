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

import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;

public final class LuaStreamLib extends OneArgFunction {
	
	private final LuaInstance instance;

	public LuaStreamLib(LuaInstance instance) {
		this.instance = instance;
	}
	
	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,1);
		meta.set("__tostring", new tostring());
		
		LuaTable table = new LuaTable(0,10);
		table.setmetatable(meta);
		table.set("read", new read());
		table.set("readable", new readable());
		table.set("writeable", new writeable());
		table.set("write", new write());
		
		instance.registerPackage("stream", table);
		return table;
	}

	public static final class tostring extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			return stream.toLuaString();
		}
	}

	public static final class read extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			return stream.read();
		}
	}

	public static final class readable extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			return LuaBoolean.valueOf(stream.readable());
		}
	}

	public static final class writeable extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaStream stream = (LuaStream)arg;
			return LuaBoolean.valueOf(stream.writeable());
		}
	}

	public static final class write extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaStream stream = (LuaStream)arg1;
			stream.write(arg2);
			return NIL;
		}
	}
}
