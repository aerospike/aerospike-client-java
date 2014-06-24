/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
