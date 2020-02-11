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

	public LuaInteger size() {
		return LuaInteger.valueOf(list.size());
	}

	public LuaString toLuaString() {
		return LuaString.valueOf(list.toString());
	}

	public LuaValue get(LuaValue index) {
		return list.get(index.toint() - 1);
	}

	public void set(LuaValue index, LuaValue value) {
		int i = index.toint();
		ensureSize(i);
		list.set(i - 1, value);
	}

	public Iterator<LuaValue> iterator() {
		return list.iterator();
	}

	public void insert(LuaValue index, LuaValue value) {
		int i = index.toint();
		ensureSize(i);
		list.add(i - 1, value);
	}

	public void append(LuaValue value) {
		list.add(value);
	}

	public void prepend(LuaValue value) {
		list.add(0, value);
	}

	public LuaList take(LuaValue items) {
		int max = items.toint();

		if (max > list.size()) {
			max = list.size();
		}
		return subList(0, max);
	}

	public void remove(LuaValue index) {
		list.remove(index.toint() - 1);
	}

	public LuaList drop(LuaValue count) {
		int c = count.toint();

		if (c >= list.size()) {
			return new LuaList(instance, new ArrayList<LuaValue>(0));
		}
		return subList(c, list.size());
	}

	public void trim(LuaValue count) {
		int min = count.toint() - 1;

		for (int i = list.size() - 1; i >= min; i--) {
			list.remove(i);
		}
	}

	public LuaList clone() {
		return new LuaList(instance, new ArrayList<LuaValue>(list));
	}

	public void concat(LuaList list2) {
		this.list.addAll(list2.list);
	}

	public LuaList merge(LuaList list2) {
		List<LuaValue> target = new ArrayList<LuaValue>(this.list);
		target.addAll(list2.list);
		return new LuaList(instance, target);
	}

	public Object luaToObject() {
		ArrayList<Object> target = new ArrayList<Object>(list.size());

		for (LuaValue luaValue : list) {
			Object obj = LuaUtil.luaToObject(luaValue);
			target.add(obj);
		}
		return target;
	}

	private void ensureSize(int size) {
		if (size > list.size()) {
			for (int i = list.size(); i < size; i++) {
				list.add(LuaValue.NIL);
			}
		}
	}

	private LuaList subList(int begin, int end) {
		return new LuaList(instance, new ArrayList<LuaValue>(list.subList(begin, end)));
	}
}
