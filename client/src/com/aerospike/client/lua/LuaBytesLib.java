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

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
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
		
		new one_arg(table, 0, "size");
		new one_arg(table, 1, "get_type");

		new two_arg(table, 0, "set_size");
		new two_arg(table, 1, "set_type");

		new var_args(table, 0, "get_var_int");
		new var_args(table, 1, "set_bytes");
		new var_args(table, 2, "append_bytes");

		new get_block(instance, table, 0, "get_string");
		new get_block(instance, table, 1, "get_bytes");

		new get_int(table, 0, "get_int16");
		new get_int(table, 0, "get_int16_be");
		new get_int(table, 1, "get_int16_le");
		new get_int(table, 2, "get_int32");
		new get_int(table, 2, "get_int32_be");
		new get_int(table, 3, "get_int32_le");
		new get_int(table, 4, "get_int64");
		new get_int(table, 4, "get_int64_be");
		new get_int(table, 5, "get_int64_le");
		new get_int(table, 6, "get_byte");

		new set_int(table, 0, "set_int16");
		new set_int(table, 0, "set_int16_be");
		new set_int(table, 1, "set_int16_le");
		new set_int(table, 2, "set_int32");
		new set_int(table, 2, "set_int32_be");
		new set_int(table, 3, "set_int32_le");
		new set_int(table, 4, "set_int64");
		new set_int(table, 4, "set_int64_be");
		new set_int(table, 5, "set_int64_le");
		new set_int(table, 6, "set_var_int");
		new set_int(table, 7, "set_string");
		new set_int(table, 8, "set_byte");
				
		new append_int(table, 0, "append_int16");
		new append_int(table, 0, "append_int16_be");
		new append_int(table, 1, "append_int16_le");
		new append_int(table, 2, "append_int32");
		new append_int(table, 2, "append_int32_be");
		new append_int(table, 3, "append_int32_le");
		new append_int(table, 4, "append_int64");
		new append_int(table, 4, "append_int64_be");
		new append_int(table, 5, "append_int64_le");
		new append_int(table, 6, "append_var_int");
		new append_int(table, 7, "append_string");
		new append_int(table, 8, "append_byte");
		
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
			new one_arg(meta, 0, "__len");  // size
			new one_arg(meta, 2, "__tostring");
			new get_int(meta, 6, "__index");
			new set_int(meta, 8, "__newindex");
			
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
	
	private static final class one_arg extends OneArgFunction {
		public one_arg(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg) {
			LuaBytes bytes = (LuaBytes)arg;
			
			switch (opcode) {
			case 0: // len
				return LuaInteger.valueOf(bytes.size());
				
			case 1: // get_type
				return LuaInteger.valueOf(bytes.getType());

			case 2: // to_string
				return LuaString.valueOf(bytes.toString());
				
			default:
				return LuaValue.valueOf(false);					
			}
		}
	}

	private static final class two_arg extends TwoArgFunction {
		public two_arg(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			
			switch (opcode) {
			case 0: // set_size
				int size = arg2.toint();
				bytes.setCapacity(size);
				return LuaValue.valueOf(true);					
				
			case 1: // set_type
				bytes.setType(arg2.toint());
				return LuaValue.valueOf(true);
		
			default:
				return LuaValue.valueOf(false);					
			}
		}
	}
	
	private static final class var_args extends VarArgFunction {
		public var_args(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public Varargs invoke(Varargs args) {
			LuaBytes bytes = (LuaBytes)args.arg(1);
						
			try {
				switch (opcode) {
				case 0: { // get_var_int
					int offset = args.arg(2).toint() - 1;
					int[] results = bytes.getVarInt(offset);
					return LuaValue.varargsOf(new LuaValue[] {LuaInteger.valueOf(results[0]), LuaInteger.valueOf(results[1])});			
				}
				case 1: { // set_bytes
					int offset = args.arg(2).toint() - 1;
					LuaBytes value = (LuaBytes)args.arg(3);
					int length = args.arg(4).toint();
					bytes.setBytes(value, offset, length);
					break;
				}
				case 2: { // append_bytes
					LuaBytes value = (LuaBytes)args.arg(2);
					int length = args.arg(3).toint();
					bytes.appendBytes(value, length);
					break;
				}
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

	private static final class get_block extends ThreeArgFunction {
		private final LuaInstance instance;
		
		public get_block(LuaInstance instance, LuaTable table, int id, String name) {
			this.instance = instance;
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			LuaBytes bytes = (LuaBytes)arg1;
			int offset = arg2.toint() - 1;
			int length = arg3.toint();
			
			try {
				switch (opcode) {
				case 0: // get_string
					return LuaString.valueOf(bytes.getString(offset, length));
					
				case 1: // get_bytes
					byte[] target = bytes.getBytes(offset, length);
					return new LuaBytes(instance, target);
					
				default:
					return LuaValue.valueOf(false);					
				}			
			}
			catch (Exception e) {
				return LuaValue.valueOf(false);
			}
		}
	}

	private static final class get_int extends TwoArgFunction {
		public get_int(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			int offset = arg2.toint() - 1;
			
			try {
				switch (opcode) {
				case 0: // get_int16 and get_int16_be
					return LuaInteger.valueOf(bytes.getBigInt16(offset));
					
				case 1: // get_int16_le
					return LuaInteger.valueOf(bytes.getLittleInt16(offset));
	
				case 2: // get_int32 and get_int32_be
					return LuaInteger.valueOf(bytes.getBigInt32(offset));
	
				case 3: // get_int32_le
					return LuaInteger.valueOf(bytes.getLittleInt32(offset));
	
				case 4: // get_int64 and get_int64_be
					return LuaInteger.valueOf(bytes.getBigInt64(offset));
					
				case 5: // get_int64_le
					return LuaInteger.valueOf(bytes.getLittleInt64(offset));
					
				case 6: // get_byte
					return LuaInteger.valueOf(bytes.getByte(offset));
					
				default:
					return LuaValue.valueOf(false);					
				}
			}
			catch (Exception e) {
				return LuaValue.valueOf(false);
			}
		}		
	}
	
	private static final class set_int extends ThreeArgFunction {
		public set_int(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
			LuaBytes bytes = (LuaBytes)arg1;
			int offset = arg2.toint() - 1;
			
			try {
				switch (opcode) {
				case 0: // set_int16 and set_int16_be
					bytes.setBigInt16(arg3.toshort(), offset);
					break;
	
				case 1: // set_int16_le
					bytes.setLittleInt16(arg3.toshort(), offset);
					break;
					
				case 2: // set_int32 and set_int32_be
					bytes.setBigInt32(arg3.toint(), offset);
					break;
					
				case 3: // set_int32_le
					bytes.setLittleInt32(arg3.toint(), offset);
					break;
					
				case 4: // set_int64 and set_int64_be
					bytes.setBigInt64(arg3.tolong(), offset);
					break;
					
				case 5: // set_int64_le
					bytes.setLittleInt64(arg3.tolong(), offset);
					break;
					
				case 6: // set_var_int
					return LuaInteger.valueOf(bytes.setVarInt(arg3.toint(), offset));
					
				case 7: // set_string
					bytes.setString(arg3.toString(), offset);
					break;

				case 8: // set_byte
					bytes.setByte(arg3.tobyte(), offset);
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
	
	private static final class append_int extends TwoArgFunction {
		public append_int(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}
		
		@Override
		public LuaValue call(LuaValue arg1, LuaValue arg2) {
			LuaBytes bytes = (LuaBytes)arg1;
			
			try {
				switch (opcode) {
				case 0: // append_int16 and append_int16_be
					bytes.appendBigInt16(arg2.toshort());
					break;
	
				case 1: // append_int16_le
					bytes.appendLittleInt16(arg2.toshort());
					break;
					
				case 2: // append_int32 and append_int32_be
					bytes.appendBigInt32(arg2.toint());
					break;
					
				case 3: // append_int32_le
					bytes.appendLittleInt32(arg2.toint());
					break;
					
				case 4: // append_int64 and append_int64_be
					bytes.appendBigInt64(arg2.tolong());
					break;
					
				case 5: // append_int64_le
					bytes.appendLittleInt64(arg2.tolong());
					break;
					
				case 6: // append_var_int
					return LuaInteger.valueOf(bytes.appendVarInt(arg2.toint()));
					
				case 7: // append_string
					bytes.appendString(arg2.toString());
					break;
					
				case 8: // append_byte
					bytes.appendByte(arg2.tobyte());
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
