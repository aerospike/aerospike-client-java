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
