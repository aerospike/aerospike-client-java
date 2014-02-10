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
