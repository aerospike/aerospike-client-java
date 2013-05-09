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
