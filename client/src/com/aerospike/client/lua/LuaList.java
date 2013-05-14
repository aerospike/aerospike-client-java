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

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public class LuaList<T> extends LuaUserdata {

	protected List<T> list;

	public LuaList(List<T> list) {
		super(list);
		this.list = list;
	}

	public LuaValue getValue(LuaValue index) {
		Object object = list.get(index.toint() - 1);
		return (object instanceof LuaValue)? (LuaValue)object : new LuaUserdata(object);
	}

	@SuppressWarnings("unchecked")
	public void setValue(int index, LuaValue value) {
		list.set(index, (T)value);
	}

	@SuppressWarnings("unchecked")
	public void append(LuaValue value) {
		list.add((T)value);
	}
	
	@SuppressWarnings("unchecked")
	public void append(int index, LuaValue value) {
		list.add(index, (T)value);
	}

	public final LuaList<T> take(LuaValue items) {
		int max = items.toint();
		
		if (max > list.size()) {
			max = list.size();
		}
		return subList(0, max);
	}
	
	public final LuaList<T> drop(LuaValue items) {
		int min = items.toint();
		
		if (min > list.size()) {
			min = list.size();
		}
		return subList(min, list.size());
	}

	public LuaList<T> subList(int begin, int end) {		
		list = list.subList(begin, end);
		return new LuaList<T>(list);
	}
	
	@SuppressWarnings("unchecked")
	public final void ensureSize(int size) {		
		if (size > list.size()) {
			for (int i = list.size(); i < size; i++) {
				list.add((T)LuaValue.NIL);
			}
		}
	}
	
	public static final class LuaValueList extends LuaList<LuaValue> {

		public LuaValueList(List<LuaValue> list) {
			super(list);
		}
		
		@Override
		public LuaValue getValue(LuaValue index) {
			return list.get(index.toint() - 1);
		}

		@Override
		public void setValue(int index, LuaValue value) {
			list.set(index, value);
		}

		@Override
		public void append(LuaValue value) {
			list.add(value);
		}
		
		@Override
		public void append(int index, LuaValue value) {
			list.add(index, value);
		}

		@Override
		public LuaList<LuaValue> subList(int begin, int end) {		
			list = list.subList(begin, end);
			return new LuaValueList(list);
		}
	}
}
