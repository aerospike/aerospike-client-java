/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNumber;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

public final class LuaBytesLib extends OneArgFunction {

	private final LuaInstance instance;

	public LuaBytesLib(LuaInstance instance) {
		this.instance = instance;
		instance.load(new MetaLib(instance));
	}

	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,2);
		meta.set("__call", new create(instance));

		LuaTable table = new LuaTable(0,50);
		table.setmetatable(meta);

		new bytescode(table, 0, "size");
		new bytescode(table, 2, "get_byte");
		new bytescode(table, 3, "get_type");
		new bytescode(table, 4, "get_string");
		new bytescode(table, 5, "get_bytes");
		new bytescode(table, 6, "get_int16");
		new bytescode(table, 6, "get_int16_be");
		new bytescode(table, 7, "get_int16_le");
		new bytescode(table, 8, "get_int32");
		new bytescode(table, 8, "get_int32_be");
		new bytescode(table, 9, "get_int32_le");
		new bytescode(table, 10, "get_int64");
		new bytescode(table, 10, "get_int64_be");
		new bytescode(table, 11, "get_int64_le");
		new bytescode(table, 12, "get_var_int");
		new bytescode(table, 13, "set_var_int");
		new bytescode(table, 14, "append_var_int");

		new bytesbool(table, 0, "set_byte");
		new bytesbool(table, 1, "set_size");
		new bytesbool(table, 2, "set_type");
		new bytesbool(table, 3, "set_string");
		new bytesbool(table, 4, "set_bytes");
		new bytesbool(table, 5, "set_int16");
		new bytesbool(table, 5, "set_int16_be");
		new bytesbool(table, 6, "set_int16_le");
		new bytesbool(table, 7, "set_int32");
		new bytesbool(table, 7, "set_int32_be");
		new bytesbool(table, 8, "set_int32_le");
		new bytesbool(table, 9, "set_int64");
		new bytesbool(table, 9, "set_int64_be");
		new bytesbool(table, 10, "set_int64_le");
		new bytesbool(table, 11, "append_string");
		new bytesbool(table, 12, "append_bytes");
		new bytesbool(table, 13, "append_byte");
		new bytesbool(table, 14, "append_int16");
		new bytesbool(table, 14, "append_int16_be");
		new bytesbool(table, 15, "append_int16_le");
		new bytesbool(table, 16, "append_int32");
		new bytesbool(table, 16, "append_int32_be");
		new bytesbool(table, 17, "append_int32_le");
		new bytesbool(table, 18, "append_int64");
		new bytesbool(table, 18, "append_int64_be");
		new bytesbool(table, 19, "append_int64_le");

		instance.registerPackage("bytes", table);
		return table;
	}

	private static final class MetaLib extends OneArgFunction {

		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable(0, 5);
			new bytescode(meta, 0, "__len");
			new bytescode(meta, 1, "__tostring");
			new bytescode(meta, 2, "__index");
			new bytesbool(meta, 0, "__newindex");
			instance.registerPackage("Bytes", meta);
			return meta;
		}
	}

	private static final class create extends VarArgFunction {
		private final LuaInstance instance;

		public create(LuaInstance instance) {
			this.instance = instance;
		}

		@Override
		public Varargs invoke(Varargs args) {
			int size = args.toint(2);  // size is optional, defaults to zero.
			return new LuaBytes(instance, size);
		}
	}

	private static final class bytescode extends VarArgFunction {
		public bytescode(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public Varargs invoke(Varargs args) {
			LuaBytes bytes = (LuaBytes)args.arg(1);

			switch (opcode) {
			case 0: // __len, size
				return LuaInteger.valueOf(bytes.size());

			case 1: // __tostring
				return LuaString.valueOf(bytes.toString());

			case 2: // __index, get_byte
				return LuaInteger.valueOf(bytes.getByte(args.arg(2).toint() - 1));

			case 3: // get_type
				return LuaInteger.valueOf(bytes.getType());

			case 4: // get_string
				return LuaString.valueOf(bytes.getString(args.arg(2).toint() - 1, args.arg(3).toint()));

			case 5: // get_bytes
				return bytes.getBytes(args.arg(2).toint() - 1, args.arg(3).toint());

			case 6: // get_int16, get_int16_be
				return LuaInteger.valueOf(bytes.getBigInt16(args.arg(2).toint() - 1));

			case 7: // get_int16_le
				return LuaInteger.valueOf(bytes.getLittleInt16(args.arg(2).toint() - 1));

			case 8: // get_int32, get_int32_be
				return LuaInteger.valueOf(bytes.getBigInt32(args.arg(2).toint() - 1));

			case 9: // get_int32_le
				return LuaInteger.valueOf(bytes.getLittleInt32(args.arg(2).toint() - 1));

			case 10: // get_int64, get_int64_be
				return LuaNumber.valueOf(bytes.getBigInt64(args.arg(2).toint() - 1));

			case 11: // get_int64_le
				return LuaNumber.valueOf(bytes.getLittleInt64(args.arg(2).toint() - 1));

			case 12: // get_var_int
				int[] results = bytes.getVarInt(args.arg(2).toint() - 1);
				return LuaValue.varargsOf(new LuaValue[] {LuaInteger.valueOf(results[0]), LuaInteger.valueOf(results[1])});

			case 13: // set_var_int
				return LuaInteger.valueOf(bytes.setVarInt(args.arg(2).toint() - 1, args.arg(3).toint()));

			case 14: // append_var_int
				return LuaInteger.valueOf(bytes.appendVarInt(args.arg(2).toint()));

			default:
				return NIL;
			}
		}
	}

	private static final class bytesbool extends VarArgFunction {
		public bytesbool(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public Varargs invoke(Varargs args) {
			LuaBytes bytes = (LuaBytes)args.arg(1);

			try {
				switch (opcode) {
				case 0: // __newindex, set_byte
					bytes.setByte(args.arg(2).toint() - 1, args.arg(3).tobyte());
					break;

				case 1: // set_size
					bytes.setCapacity(args.arg(2).toint() - 1);
					break;

				case 2: // set_type
					bytes.setType(args.arg(2).toint());
					break;

				case 3: // set_string
					bytes.setString(args.arg(2).toint() - 1, args.arg(3).tojstring());
					break;

				case 4: // set_bytes
					bytes.setBytes(args.arg(2).toint() - 1, (LuaBytes)args.arg(3), args.arg(4).toint());
					break;

				case 5: // set_int16 and set_int16_be
					bytes.setBigInt16(args.arg(2).toint() - 1, args.arg(3).toint());
					break;

				case 6: // set_int16_le
					bytes.setLittleInt16(args.arg(2).toint() - 1, args.arg(3).toint());
					break;

				case 7: // set_int32 and set_int32_be
					bytes.setBigInt32(args.arg(2).toint() - 1, args.arg(3).toint());
					break;

				case 8: // set_int32_le
					bytes.setLittleInt32(args.arg(2).toint() - 1, args.arg(3).toint());
					break;

				case 9: // set_int64 and set_int64_be
					bytes.setBigInt64(args.arg(2).toint() - 1, args.arg(3).tolong());
					break;

				case 10: // set_int64_le
					bytes.setLittleInt64(args.arg(2).toint() - 1, args.arg(3).tolong());
					break;

				case 11: // append_string
					bytes.appendString(args.arg(2).tojstring());
					break;

				case 12: // append_bytes
					bytes.appendBytes((LuaBytes)args.arg(2), args.arg(3).toint());
					break;

				case 13: // append_bytes
					bytes.appendBytes((LuaBytes)args.arg(2), args.arg(3).toint());
					break;

				case 14: // append_byte
					bytes.appendByte(args.arg(2).tobyte());
					break;

				case 15: // append_int16_le
					bytes.appendLittleInt16(args.arg(2).toint());
					break;

				case 16: // append_int32 and append_int32_be
					bytes.appendBigInt32(args.arg(2).toint());
					break;

				case 17: // append_int32_le
					bytes.appendLittleInt32(args.arg(2).toint());
					break;

				case 18: // append_int64 and append_int64_be
					bytes.appendBigInt64(args.arg(2).tolong());
					break;

				case 19: // append_int64_le
					bytes.appendLittleInt64(args.arg(2).tolong());
					break;

				default:
					return LuaValue.valueOf(false);
				}
				return LuaValue.valueOf(true);
			}
			catch (Exception e) {
				return LuaValue.valueOf(false);
			}
		}
	}
}
