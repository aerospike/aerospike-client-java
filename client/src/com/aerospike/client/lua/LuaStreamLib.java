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
