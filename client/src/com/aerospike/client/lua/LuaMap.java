/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
