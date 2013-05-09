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

import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.Log;
import com.aerospike.client.query.ResultSet;

public final class LuaOutputStream extends LuaUserdata implements LuaStream {
	
	private final ResultSet resultSet;
	
	public LuaOutputStream(ResultSet resultSet) {
		super(resultSet);
		this.resultSet = resultSet;
	}
	
	@Override
	public LuaValue read() {
		throw new RuntimeException("LuaOutputStream is not readable.");
	}

	@Override
	public void write(LuaValue source) {
		Object target;
		
		switch (source.type()) {
		case TNUMBER:
			if (source.isint()) {
				target = source.toint();
			}
			else {
				target = source.tolong();
			}
			break;
		
		case TBOOLEAN:
			target = source.toboolean();
			break;
			
		case TSTRING:
			target = source.toString();
			break;
			
		case TUSERDATA:
			LuaUserdata data = (LuaUserdata) source;
			target = data.m_instance;
			break;
			
		default:
			if (Log.warnEnabled()) {
				Log.warn("Invalid lua type: " + source.type());
			}
			target = ResultSet.END;
			break;
		}
		resultSet.put(target);
	}

	@Override
	public boolean readable() {
		return false;
	}

	@Override
	public boolean writeable() {
		return true;
	}

	@Override
	public LuaValue toLuaString() {
		return LuaString.valueOf(LuaOutputStream.class.getName());
	}
}
