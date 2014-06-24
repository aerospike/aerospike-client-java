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
