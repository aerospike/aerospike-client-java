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

import java.util.Arrays;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

import com.aerospike.client.command.Buffer;

public final class LuaBytesLib extends OneArgFunction {

	private final LuaInstance instance;
	
	public LuaBytesLib(LuaInstance instance) {
		this.instance = instance;
		instance.load(new MetaLib(instance));
	}

	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,2);
		meta.set("__call", new create(instance));
		
		LuaTable table = new LuaTable(0,10);
		table.setmetatable(meta);
		table.set("size", new len());
		table.set("set_size", new set_size());
		
		new getint(table, 0, "get_int16");
		new getint(table, 1, "get_int32");
		new getint(table, 2, "get_int64");

		new setint(table, 0, "set_int16");
		new setint(table, 1, "set_int32");
		new setint(table, 2, "set_int64");
		
		table.set("set_string", new set_string());
		table.set("set_bytes", new set_bytes());

		table.set("get_type", new get_type());
		table.set("set_type", new set_type());

		instance.registerPackage("bytes", table);
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
			
			instance.registerPackage("Bytes", meta);
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
			int size = args.toint(2);
			LuaBytes bytes = new LuaBytes(instance, size);		
			return LuaValue.varargsOf(new LuaValue[] {bytes});
		}
	}

	public static final class get_type extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaBytes bytes = (LuaBytes)arg;
			return LuaInteger.valueOf(bytes.type);
		}
	}

	public static final class len extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaBytes bytes = (LuaBytes)arg;
			return LuaInteger.valueOf(bytes.bytes.length);
		}
	}

	public static final class tostring extends OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			LuaBytes bytes = (LuaBytes)arg;
			return LuaString.valueOf(Arrays.toString(bytes.bytes));
		}
	}

	public static final class index extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			int offset = arg2.toint() - 1;
			return LuaInteger.valueOf(bytes.bytes[offset]);
		}
	}

	public static final class newindex extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			LuaBytes bytes = (LuaBytes)arg1;
			int offset = arg2.toint() - 1;
			bytes.bytes[offset] = arg3.tobyte();
			return NIL;
		}
	}
	
	public static final class set_type extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			bytes.type = arg2.toint();
			return NIL;
		}
	}

	public static final class set_string extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			byte[] bytes = ((LuaBytes)arg1).bytes;
			int offset = arg2.toint() - 1;
			String val = arg3.toString();
			Buffer.stringToUtf8(val, bytes, offset);
			return NIL;
		}
	}

	public static final class set_bytes extends VarArgFunction {
		@Override
		public Varargs invoke(Varargs args) {
			byte[] dest = ((LuaBytes)args.arg(1)).bytes;
			int destPos = args.arg(2).toint() - 1;
			byte[] src = ((LuaBytes)args.arg(3)).bytes;
			int length = args.arg(4).toint();
			
			if (length == 0) {
				length = src.length;
			}
			else if (length > src.length) {
				return NIL;
			}
			
			if (destPos < 0 || (destPos + length) > dest.length) {
				return NIL;
			}
			System.arraycopy(src, 0, dest, destPos, length);
			return NIL;
		}
	}

	public static final class setint extends ThreeArgFunction {
		public setint(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			byte[] bytes = ((LuaBytes)arg1).bytes;
			int offset = arg2.toint() - 1;
			int val = arg3.toint();
			
			switch (opcode) {
			case 0: // put_int16
				Buffer.shortToBytes(val, bytes, offset);
				break;
				
			case 1: // put_int32
				Buffer.intToBytes(val, bytes, offset);
				break;

			case 2: // put_int64
				Buffer.longToBytes(val, bytes, offset);
				break;
			}
			return NIL;
		}
	}
	
	public static final class getint extends TwoArgFunction {
		public getint(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			byte[] bytes = ((LuaBytes)arg1).bytes;
			int offset = arg2.toint() - 1;
			
			switch (opcode) {
			case 0: // get_int16
				return LuaInteger.valueOf(Buffer.bytesToShort(bytes, offset));
				
			case 1: // get_int32
				return LuaInteger.valueOf(Buffer.bytesToInt(bytes, offset));

			case 2: // get_int64
				return LuaInteger.valueOf(Buffer.bytesToLong(bytes, offset));
			}
			return NIL;
		}		
	}
	
	public static final class get_string extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			byte[] bytes = ((LuaBytes)arg1).bytes;
			int offset = arg2.toint() - 1;
			int length = arg3.toint();
			String val = Buffer.utf8ToString(bytes, offset, length);
			return LuaString.valueOf(val);
		}
	}
	
	public static final class set_size extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			int size = arg2.toint();
			bytes.setLength(size);
			return NIL;
		}
	}
}
