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

import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaBoolean;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNumber;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.util.Unpacker;

public class LuaUnpacker extends Unpacker<LuaValue> {		
	private LuaInstance instance;
	
	public LuaUnpacker(LuaInstance instance, byte[] buffer, int offset, int length) {
		super(buffer, offset, length);
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
		return new LuaBytes(instance, value);
	}

	@Override
	protected LuaString getString(String value) {
		return LuaString.valueOf(value);
	}
	
	@Override
	protected LuaNumber getLong(long value) {
		return LuaInteger.valueOf(value);
	}
	
	@Override
	protected LuaNumber getDouble(double value) {
		return LuaDouble.valueOf(value);
	}
	
	@Override
	protected LuaBoolean getBoolean(boolean value) {
		return LuaBoolean.valueOf(value);
	}
}
