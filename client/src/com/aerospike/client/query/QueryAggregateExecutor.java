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
package com.aerospike.client.query;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
	private LuaInstance lua;
	private Future<?> future;
	
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
	}
	
	public void execute() {		
		// Start Lua thread which reads from a queue, applies aggregate function and 
		// writes to a result set. 
		future = threadPool.submit(this);
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
		}
		catch (InterruptedException ie) {
		}
			
		if (exception == null) {
			try {
				// Ensure lua thread completes before sending end command to result set.
				future.get(1000, TimeUnit.MILLISECONDS);
			}
			catch (Exception e) {
			}
		}

		// Send end command to user's result set.
		resultSet.put(ResultSet.END);
	}

	public ResultSet getResultSet() {
		return resultSet;
	}
}
