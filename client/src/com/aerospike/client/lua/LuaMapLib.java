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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

public final class LuaMapLib extends OneArgFunction {

	private final LuaInstance instance;

	public LuaMapLib(LuaInstance instance) {
		this.instance = instance;
		instance.load(new MetaLib(instance));
	}

	public LuaValue call(LuaValue env) {
		LuaTable meta = new LuaTable(0,2);
		meta.set("__call", new create(instance));

		LuaTable table = new LuaTable(0,11);
		table.setmetatable(meta);
		table.set("create", new create(instance));

		new mapcode(table, 0, "size");
		new mapcode(table, 4, "pairs");
		new mapcode(table, 5, "keys");
		new mapcode(table, 6, "values");
		new mapcode(table, 7, "remove");
		new mapcode(table, 8, "clone");
		new mapcode(table, 9, "merge");
		new mapcode(table, 10, "diff");

		instance.registerPackage("map", table);
		return table;
	}

	private static final class MetaLib extends OneArgFunction {
		private final LuaInstance instance;

		public MetaLib(LuaInstance instance) {
			this.instance = instance;
		}

		public LuaValue call(LuaValue env) {
			LuaTable meta = new LuaTable(0,5);
			new mapcode(meta, 0, "__len");
			new mapcode(meta, 1, "__tostring");
			new mapcode(meta, 2, "__index");
			new mapcode(meta, 3, "__newindex");
			instance.registerPackage("Map", meta);
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

			LuaMap map = new LuaMap(instance, new HashMap<LuaValue,LuaValue>(capacity));

			if (args.istable(2)) {
				LuaTable table = args.checktable(2);
				LuaValue k = LuaValue.NIL;

				while (true) {
					 Varargs n = table.next(k);

					 if ((k = n.arg1()).isnil())
						 break;

					 LuaValue v = n.arg(2);
					 map.put(k, v);
				 }
			}
			return map;
		}
	}

	private static final class mapcode extends VarArgFunction {
		public mapcode(LuaTable table, int id, String name) {
			super.opcode = id;
			super.name = name;
			table.set(name, this);
		}

		@Override
		public Varargs invoke(Varargs args) {
			LuaMap map = (LuaMap)args.arg(1);

			switch (opcode) {
			case 0: // __len, size
				return map.size();

			case 1: // __tostring
				return map.toLuaString();

			case 2: // __index
				return map.get(args.arg(2));

			case 3: // __newindex
				map.put(args.arg(2), args.arg(3));
				return NIL;

			case 4: // pairs
				return new nextLuaValue(map.entrySetIterator());

			case 5: // keys
				return new LuaListLib.nextLuaValue(map.keySetIterator());

			case 6: // values
				return new LuaListLib.nextLuaValue(map.valuesIterator());

			case 7: // remove
				map.remove(args.arg(2));
				return NIL;

			case 8: // clone
				return map.clone();

			case 9: // merge
				return map.merge((LuaMap)args.arg(2), (LuaFunction)args.arg(3));

			case 10: // diff
				return map.diff((LuaMap)args.arg(2));

			default:
				return NIL;
			}
		}
	}

	private static final class nextLuaValue extends VarArgFunction {
		private final Iterator<Entry<LuaValue,LuaValue>> iter;

		public nextLuaValue(Iterator<Entry<LuaValue,LuaValue>> iter) {
			this.iter = iter;
		}

		@Override
		public Varargs invoke(Varargs args) {
			if (iter.hasNext()) {
				Entry<LuaValue,LuaValue> entry = iter.next();
				return LuaValue.varargsOf(new LuaValue[] {entry.getKey(), entry.getValue()});
			}
			return NONE;
		}
	}
}
