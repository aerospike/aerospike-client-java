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
