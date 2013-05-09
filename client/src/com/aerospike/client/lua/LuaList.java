/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.lua;

import java.util.List;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public class LuaList<T> extends LuaUserdata {

	protected final List<T> list;

	public LuaList(List<T> list) {
		super(list);
		this.list = list;
	}
	
	public LuaInteger size() {
		return LuaInteger.valueOf(list.size());
	}
	
	public LuaString toLuaString() {
		return LuaString.valueOf(list.toString());
	}

	public LuaValue getValue(LuaValue index) {
		Object object = list.get(index.toint() - 1);
		return (object instanceof LuaValue)? (LuaValue)object : new LuaUserdata(object);
	}

	@SuppressWarnings("unchecked")
	public void setValue(LuaValue index, LuaValue value) {
		list.set(index.toint() - 1, (T)value);
	}

	@SuppressWarnings("unchecked")
	public void append(LuaValue value) {
		list.add((T)value);
	}
	
	@SuppressWarnings("unchecked")
	public void append(int index, LuaValue value) {
		list.add(index, (T)value);
	}

	public LuaList<T> take(LuaValue items) {
		int max = items.toint();
		
		if (max > list.size()) {
			max = list.size();
		}
		return subList(0, max);
	}
	
	public LuaList<T> drop(LuaValue items) {
		int min = items.toint();
		
		if (min > list.size()) {
			min = list.size();
		}
		return subList(min, list.size());
	}

	public LuaList<T> subList(int begin, int end) {		
		return new LuaList<T>(list.subList(begin, end));
	}
	
	public LuaUserdata iterator() {
		return new LuaListIterator<T>(list.iterator());
	}
}
