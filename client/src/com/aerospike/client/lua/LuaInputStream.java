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

import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

public final class LuaInputStream extends LuaUserdata implements LuaStream {
	
	private final BlockingQueue<LuaValue> queue;

	public LuaInputStream(BlockingQueue<LuaValue> queue) {
		super(queue);
		this.queue = queue;
	}

	public LuaValue read() {
		try {
			return queue.take();
		}
		catch (InterruptedException ie) {
			return LuaValue.NIL;
		}
	}

	@Override
	public void write(LuaValue value) {
		throw new RuntimeException("LuaInputStream is not writeable.");
	}

	@Override
	public boolean readable() {
		return true;
	}

	@Override
	public boolean writeable() {
		return false;
	}

	@Override
	public LuaValue toLuaString() {
		return LuaString.valueOf(LuaInputStream.class.getName());
	}
}
