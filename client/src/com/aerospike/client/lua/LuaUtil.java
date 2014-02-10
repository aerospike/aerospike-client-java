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

import org.luaj.vm2.LuaValue;

import com.aerospike.client.Log;
import com.aerospike.client.query.ResultSet;

public final class LuaUtil {
	
	public static Object luaToObject(LuaValue source) {
		switch (source.type()) {
		case LuaValue.TNUMBER:
			return source.tolong();
		
		case LuaValue.TBOOLEAN:
			return source.toboolean();
			
		case LuaValue.TSTRING:
			return source.tojstring();
			
		case LuaValue.TUSERDATA:
			LuaData data = (LuaData)source;
			return data.luaToObject();
			
		default:
			if (Log.warnEnabled()) {
				Log.warn("Invalid lua type: " + source.type());
			}
			return ResultSet.END;
		}
	}	
}
