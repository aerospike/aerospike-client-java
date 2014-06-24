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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaMap extends LuaUserdata implements LuaData {

	private final Map<LuaValue,LuaValue> map;

	public LuaMap(LuaInstance instance, Map<LuaValue,LuaValue> map) {
		super(map);
		this.map = map;
		setmetatable(instance.getPackage("Map"));
	}

	public void put(LuaValue key, LuaValue value) {
		map.put(key, value);
	}
	
	public LuaValue get(LuaValue key) {
		return map.get(key);
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

	public LuaInteger size() {
		return LuaInteger.valueOf(map.size());
	}

	public LuaString toLuaString() {
		return LuaString.valueOf(map.toString());
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
