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

import java.util.ArrayList;
import java.util.Iterator;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
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

		LuaTable table = new LuaTable(0,15);
		table.setmetatable(meta);
		table.set("create", new create(instance));

		new listcode(table, 0, "size");
		new listcode(table, 4, "iterator");
		new listcode(table, 5, "insert");
		new listcode(table, 6, "append");
		new listcode(table, 7, "prepend");
		new listcode(table, 8, "take");
		new listcode(table, 9, "remove");
		new listcode(table, 10, "drop");
		new listcode(table, 11, "trim");
		new listcode(table, 12, "clone");
		new listcode(table, 13, "concat");
		new listcode(table, 14, "merge");

		instance.registerPackage("list", table);
		return table;
	}

	private static final class MetaLib extends OneArgFunction {
		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable(0, 5);
			new listcode(meta, 0, "__len");
			new listcode(meta, 1, "__tostring");
			new listcode(meta, 2, "__index");
			new listcode(meta, 3, "__newindex");
			instance.registerPackage("List", meta);
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
			int capacity = 32;

			if (args.isnumber(1)) {
				capacity = args.toint(1);
			}

			LuaList list = new LuaList(instance, new ArrayList<LuaValue>(capacity));

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
			return list;
		}
	}

	private static final class listcode extends VarArgFunction {
		public listcode(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public Varargs invoke(Varargs args) {
			LuaList list = (LuaList)args.arg(1);

			switch (opcode) {
			case 0: // __len, size
				return list.size();

			case 1: // __tostring
				return list.toLuaString();

			case 2: // __index
				return list.get(args.arg(2));

			case 3: // __newindex
				list.set(args.arg(2), args.arg(3));
				return NIL;

			case 4: // iterator
				return new nextLuaValue(list.iterator());

			case 5: // insert
				list.insert(args.arg(2), args.arg(3));
				return NIL;

			case 6: // append
				list.append(args.arg(2));
				return NIL;

			case 7: // prepend
				list.prepend(args.arg(2));
				return NIL;

			case 8: // take
				return list.take(args.arg(2));

			case 9: // remove
				list.remove(args.arg(2));
				return NIL;

			case 10: // drop
				return list.drop(args.arg(2));

			case 11: // trim
				list.trim(args.arg(2));
				return NIL;

			case 12: // clone
				return list.clone();

			case 13: // concat
				list.concat((LuaList)args.arg(2));
				return NIL;

			case 14: // merge
				return list.merge((LuaList)args.arg(2));

			default:
				return NIL;
			}
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
}
