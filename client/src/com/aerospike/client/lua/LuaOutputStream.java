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

import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

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
		Object target = LuaUtil.luaToObject(source);
		
		if (target != null) {
			resultSet.put(target);
		}
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
