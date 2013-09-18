/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaValue;

import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.lua.LuaExecutor;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryAggregateExecutor extends QueryExecutor {
	
	private final BlockingQueue<LuaValue> inputQueue;
	private final ResultSet resultSet;
	private final Thread luaThread;
	
	public QueryAggregateExecutor(
		QueryPolicy policy, 
		Statement statement, 
		Node[] nodes, 
		String packageName, 
		String functionName, 
		Value[] functionArgs
	) {
		super(policy, statement);
		statement.setAggregateFunction(packageName, functionName, functionArgs, true);
		inputQueue = new ArrayBlockingQueue<LuaValue>(500);
		resultSet = new ResultSet(this, policy.recordQueueSize);
		LuaExecutor luaExecutor = new LuaExecutor(statement, inputQueue, resultSet);
		
		// Work around luaj LuaInteger static initialization bug.
		// Calling LuaInteger.valueOf(long) is required because LuaValue.valueOf() does not have
		// a method that takes in a long parameter.  The problem is directly calling
		// LuaInteger.valueOf(long) results in a static initialization error.
		//
		// If LuaValue.valueOf() is called before any luaj calls, then the static initializer in
		// LuaInteger will be initialized properly.  		
		LuaValue.valueOf(0);
		
		// Start Lua thread which reads from a queue, applies aggregate function and 
		// writes to a result set. 
		luaThread = new Thread(luaExecutor);
		luaThread.start();
		
		// Start thread queries to each node.
		startThreads(nodes);
	}
	
	@Override
	protected QueryCommand createCommand(Node node) {
		return new QueryAggregateCommand(node, inputQueue);
	}
	
	@Override
	protected void sendCompleted() {
		try {
			// Send end command to lua thread.
			inputQueue.put(LuaValue.NIL);
			
			// Ensure lua thread completes before sending end command to result set.
			luaThread.join(1000);
		}
		catch (InterruptedException ie) {
		}

		// Send end command to user's result set.
		resultSet.put(ResultSet.END);
	}

	public ResultSet getResultSet() {
		return resultSet;
	}
}
