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

import org.luaj.vm2.LuaUserdata;

public class LuaBytes extends LuaUserdata implements LuaData {

	protected byte[] bytes;
	protected int type;

	public LuaBytes(LuaInstance instance, int size) {
		super(new byte[size]);
		bytes = (byte[])super.m_instance;
		setmetatable(instance.getPackage("Bytes"));
	}
	
	public LuaBytes(LuaInstance instance, byte[] bytes) {
		super(bytes);
		this.bytes = bytes;
		setmetatable(instance.getPackage("Bytes"));
	}
	
	public void setLength(int length) {
		if (bytes.length == length) {
			return;
		}
		
		byte[] target = new byte[length];
		
		if (length > bytes.length) {
			length = bytes.length;
		}
		System.arraycopy(bytes, 0, target, 0, length);
		bytes = target;
	}
	
	public Object luaToObject() {
		return bytes;
	}
}
