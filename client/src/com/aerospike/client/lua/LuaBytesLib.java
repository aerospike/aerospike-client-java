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
		
		LuaTable table = new LuaTable(0,8);
		table.setmetatable(meta);
		table.set("size", new len());
		table.set("tostring", new tostring());
		
		new putint(table, 0, "put_int16");
		new putint(table, 1, "put_int32");
		new putint(table, 2, "put_int64");
		
		table.set("put_string", new put_string());
		table.set("put_bytes", new put_bytes());

		new putint(table, 0, "set_int16");
		new putint(table, 1, "set_int32");
		new putint(table, 2, "set_int64");

		table.set("set_string", new put_string());
		table.set("set_bytes", new put_bytes());

		new getint(table, 0, "get_int16");
		new getint(table, 1, "get_int32");
		new getint(table, 2, "get_int64");

		table.set("get_string", new get_string());
		table.set("get_bytes", new get_bytes());
		table.set("set_len", new set_len());

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
		private final LuaValue meta;
		
		public create(LuaInstance instance) {
			this.meta = instance.getPackage("Bytes");
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			int size = args.toint(2);
			LuaBytes bytes = new LuaBytes(size);		
			bytes.setmetatable(meta);			
			return LuaValue.varargsOf(new LuaValue[] {bytes});
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
	
	public static final class put_string extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			byte[] bytes = ((LuaBytes)arg1).bytes;
			int offset = arg2.toint() - 1;
			String val = arg3.toString();
			Buffer.stringToUtf8(val, bytes, offset);
			return NIL;
		}
	}

	public static final class put_bytes extends VarArgFunction {
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
			
			if (destPos < 0 || (destPos + length) > src.length) {
				return NIL;
			}
			System.arraycopy(src, 0, dest, destPos, length);
			return NIL;
		}
	}

	public static final class putint extends ThreeArgFunction {
		public putint(LuaTable table, int id, String name) {
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

	public static final class get_bytes extends ThreeArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			byte[] src = ((LuaBytes)arg1).bytes;
			int srcPos = arg2.toint() - 1;
			int length = arg3.toint();
			
			if (srcPos > src.length) {
				return NIL;
			}
			
			int remaining = src.length - srcPos;
			
			if (length > remaining) {
				length = remaining;
			}
				
			byte[] dest = new byte[length];
			System.arraycopy(src, srcPos, dest, 0, length);
			return new LuaBytes(dest);
		}
	}
	
	public static final class set_len extends TwoArgFunction {
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			int length = arg2.toint() - 1;
			bytes.setLength(length);
			return bytes;
		}
	}
}
