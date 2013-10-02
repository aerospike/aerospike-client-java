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
import java.util.Map;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNumber;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.util.MsgUnpacker;

public class LuaUnpacker extends MsgUnpacker<LuaValue> {		
	private LuaInstance instance;
	
	public LuaUnpacker(LuaInstance instance) {
		this.instance = instance;
	}
			
	@Override
	protected LuaMap getMap(Map<LuaValue,LuaValue> value) {
		return new LuaMap(instance, value);
	}

	@Override
	protected LuaList getList(List<LuaValue> value) {
		return new LuaList(instance, value);
	}

	@Override
	protected LuaValue getJavaBlob(Object value) {
		return new LuaJavaBlob(value);
	}

	@Override
	protected LuaBytes getBlob(byte[] value) {
		return new LuaBytes(value);
	}

	@Override
	protected LuaString getString(String value) {
		return LuaString.valueOf(value);
	}
	
	@Override
	protected LuaNumber getLong(long value) {
		return LuaInteger.valueOf(value);
	}
}
