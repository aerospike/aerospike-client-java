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

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.lua.LuaCache;
import com.aerospike.client.lua.LuaInputStream;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.lua.LuaOutputStream;
import com.aerospike.client.policy.QueryPolicy;

public final class QueryAggregateExecutor extends QueryExecutor implements Runnable {
	
	private final BlockingQueue<LuaValue> inputQueue;
	private final ResultSet resultSet;
	private final Thread luaThread;
	private LuaInstance lua;
	
	public QueryAggregateExecutor(
		Cluster cluster,
		QueryPolicy policy, 
		Statement statement, 
		String packageName, 
		String functionName, 
		Value[] functionArgs
	) throws AerospikeException {
		super(cluster, policy, statement);
		inputQueue = new ArrayBlockingQueue<LuaValue>(500);
		resultSet = new ResultSet(this, policy.recordQueueSize);
		statement.setAggregateFunction(packageName, functionName, functionArgs, true);

		// Work around luaj LuaInteger static initialization bug.
		// Calling LuaInteger.valueOf(long) is required because LuaValue.valueOf() does not have
		// a method that takes in a long parameter.  The problem is directly calling
		// LuaInteger.valueOf(long) results in a static initialization error.
		//
		// If LuaValue.valueOf() is called before any luaj calls, then the static initializer in
		// LuaInteger will be initialized properly.  		
		LuaValue.valueOf(0);
		
		// Initialize lua thread, but don't start it yet.
		luaThread = new Thread(this);
	}
	
	public void execute() {		
		// Start Lua thread which reads from a queue, applies aggregate function and 
		// writes to a result set. 
		luaThread.start();
	}
	
	public void run() {
		try {
			runThreads();
		}
		catch (Exception e) {
			super.stopThreads(e);
		}
	}

	public void runThreads() throws AerospikeException {		
		lua = LuaCache.getInstance();
		
		// Start thread queries to each node.
		startThreads();		

		try {
			lua.load(statement.getPackageName(), false);
			
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
	
	@Override
	protected QueryCommand createCommand(Node node) {
		return new QueryAggregateCommand(node, policy, statement, lua, inputQueue);
	}
	
	@Override
	protected void sendCompleted() {
		try {
			// Send end command to lua thread.
			inputQueue.put(LuaValue.NIL);
			
			// Ensure lua thread completes before sending end command to result set.
			if (exception == null) {
				luaThread.join(1000);
			}
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
