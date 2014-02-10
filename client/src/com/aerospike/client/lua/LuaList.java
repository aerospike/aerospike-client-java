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
