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

import org.luaj.vm2.LuaValue;

public interface LuaStream {
	public LuaValue read();
	public void write(LuaValue value);
	public boolean readable();
	public boolean writeable();
	public LuaValue toLuaString();
}
