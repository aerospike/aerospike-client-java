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
import java.util.Map;
import java.util.Map.Entry;

import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

public final class LuaMap extends LuaUserdata implements LuaData {

	private final LuaInstance instance;
	private final Map<LuaValue,LuaValue> map;

	public LuaMap(LuaInstance instance, Map<LuaValue,LuaValue> map) {
		super(map);
		this.instance = instance;
		this.map = map;
		setmetatable(instance.getPackage("Map"));
	}

	public LuaInteger size() {
		return LuaInteger.valueOf(map.size());
	}

	public LuaString toLuaString() {
		return LuaString.valueOf(map.toString());
	}

	public LuaValue get(LuaValue key) {
		LuaValue val = map.get(key);
		return val != null ? val : NIL;
	}

	public void put(LuaValue key, LuaValue value) {
		map.put(key, value);
	}

	public Iterator<Entry<LuaValue,LuaValue>> entrySetIterator() {
		return map.entrySet().iterator();
	}

	public Iterator<LuaValue> keySetIterator() {
		return map.keySet().iterator();
	}

	public Iterator<LuaValue> valuesIterator() {
		return map.values().iterator();
	}

	public void remove(LuaValue key) {
		map.remove(key);
	}

	public LuaMap clone() {
		return new LuaMap(instance, new HashMap<LuaValue,LuaValue>(map));
	}

	public LuaMap merge(LuaMap map2, LuaFunction func) {
		HashMap<LuaValue,LuaValue> target = new HashMap<LuaValue,LuaValue>(map.size() + map2.map.size());
		target.putAll(map);

		boolean hasFunc = !(func == null || func.isnil());

		for (Entry<LuaValue,LuaValue> entry : map2.map.entrySet()) {
			if (hasFunc) {
				LuaValue value = map.get(entry.getKey());

				if (value != null) {
					Varargs ret = func.invoke(value, entry.getValue());
					target.put(entry.getKey(), (LuaValue)ret);
					continue;
				}
			}
			target.put(entry.getKey(), entry.getValue());
		}
		return new LuaMap(instance, target);
	}

	public LuaMap diff(LuaMap map2) {
		HashMap<LuaValue,LuaValue> target = new HashMap<LuaValue,LuaValue>(map.size() + map2.map.size());

		for (Entry<LuaValue,LuaValue> entry : map.entrySet()) {
			if (!map2.map.containsKey(entry.getKey())) {
				target.put(entry.getKey(), entry.getValue());
			}
		}

		for (Entry<LuaValue,LuaValue> entry : map2.map.entrySet()) {
			if (!map.containsKey(entry.getKey())) {
				target.put(entry.getKey(), entry.getValue());
			}
		}
		return new LuaMap(instance, target);
	}

	public Object luaToObject() {
		Map<Object,Object> target = new HashMap<Object,Object>(map.size());

		for (Entry<LuaValue,LuaValue> entry : map.entrySet()) {
			Object key = LuaUtil.luaToObject(entry.getKey());
			Object value = LuaUtil.luaToObject(entry.getValue());
			target.put(key, value);
		}
		return target;
	}
}
