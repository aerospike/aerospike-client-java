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

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.Log;
import com.aerospike.client.Value;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Util;

public final class LuaExecutor implements Runnable {
	private final Statement statement;
	private final BlockingQueue<LuaValue> inputQueue;
	private final ResultSet resultSet;
	
	public LuaExecutor(Statement statement, BlockingQueue<LuaValue> inputQueue, ResultSet resultSet) {
		this.statement = statement;
		this.inputQueue = inputQueue;
		this.resultSet = resultSet;
	}
	
	public void run() {
		try {
			LuaInstance lua = LuaCache.getInstance();
			
			try {
				lua.load(statement.getPackageName());
				
				LuaValue[] args = new LuaValue[4 + statement.getFunctionArgs().length];
				args[0] = lua.getFunction(statement.getFunctionName());
				args[1] = LuaInteger.valueOf(2);
				args[2] = new LuaInputStream(inputQueue);
				args[3] = new LuaOutputStream(resultSet);
				int count = 4;
				
				for (Value value : statement.getFunctionArgs()) {
					args[count++] = value.getLuaValue();
				}
				lua.call("apply_stream", args);
			}
			finally {
				LuaCache.putInstance(lua);
			}
		}
		catch (Exception e) {
			if (Log.infoEnabled()) {
				Log.info("Lua error: " + Util.getErrorMessage(e));
			}
		}
	}
}
