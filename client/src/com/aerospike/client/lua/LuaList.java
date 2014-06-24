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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaList extends LuaUserdata implements LuaData {

	private final LuaInstance instance;
	private final List<LuaValue> list;

	public LuaList(LuaInstance instance, List<LuaValue> list) {
		super(list);
		this.instance = instance;
		this.list = list;
		setmetatable(instance.getPackage("List"));
	}

	public LuaValue get(LuaValue index) {
		return list.get(index.toint() - 1);
	}
	
	public void set(LuaValue index, LuaValue value) {		
		int i = index.toint();
		ensureSize(i);
		list.set(i - 1, value);
	}
	
	private void ensureSize(int size) {		
		if (size > list.size()) {
			for (int i = list.size(); i < size; i++) {
				list.add(LuaValue.NIL);
			}
		}
	}

	public void append(LuaValue value) {
		list.add(value);
	}
	
	public void prepend(LuaValue value) {
		list.add(0, value);
	}

	public final LuaList take(LuaValue items) {
		int max = items.toint();
		
		if (max > list.size()) {
			max = list.size();
		}
		return subList(0, max);
	}
	
	public final LuaList drop(LuaValue count) {
		int min = count.toint();
		
		if (min > list.size()) {
			min = list.size();
		}
		return subList(min, list.size());
	}

	public LuaList subList(int begin, int end) {		
		return new LuaList(instance, list.subList(begin, end));
	}
	
	public Iterator<LuaValue> iterator() {
		return list.iterator();		
	}
	
	public LuaString toLuaString() {
		return LuaString.valueOf(list.toString());
	}

	public LuaInteger size() {
		return LuaInteger.valueOf(list.size());
	}
	
	public Object luaToObject() {
		ArrayList<Object> target = new ArrayList<Object>(list.size());
		
		for (LuaValue luaValue : list) {
			Object obj = LuaUtil.luaToObject(luaValue);
			target.add(obj);
		}
		return target;
	}
}
