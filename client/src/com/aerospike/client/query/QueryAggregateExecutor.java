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
package com.aerospike.client.query;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
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
	
	public QueryAggregateExecutor(
		Cluster cluster,
		QueryPolicy policy, 
		Statement statement, 
		String packageName, 
		String functionName, 
		Value[] functionArgs
	) throws AerospikeException {
		super(cluster, policy, statement, null);
		inputQueue = new ArrayBlockingQueue<LuaValue>(500);
		resultSet = new ResultSet(this, policy.recordQueueSize);
		statement.setAggregateFunction(packageName, functionName, functionArgs, true);
		statement.prepare();

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
		threadPool.execute(this);
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
		
		try {
			// Start thread queries to each node.
			startThreads();		

			lua.load(statement.getPackageName(), false);
			
			LuaValue[] args = new LuaValue[4 + statement.getFunctionArgs().length];
			args[0] = lua.getFunction(statement.getFunctionName());
			args[1] = LuaInteger.valueOf(2);
			args[2] = new LuaInputStream(inputQueue);
			args[3] = new LuaOutputStream(resultSet);
			int count = 4;
			
			for (Value value : statement.getFunctionArgs()) {
				args[count++] = value.getLuaValue(lua);
			}
			lua.call("apply_stream", args);
		}
		finally {
			// Send end command to user's result set.
			// If query was already cancelled, this put will be ignored.
			resultSet.put(ResultSet.END);
			LuaCache.putInstance(lua);
		}
	}
	
	@Override
	protected QueryCommand createCommand(Node node) {
		return new QueryAggregateCommand(node, policy, statement, lua, inputQueue);
	}
	
	@Override
	protected void sendCancel() {
		// Clear lua input queue to ensure cancel is accepted.
		inputQueue.clear();
		resultSet.abort();
		
		// Send end command to lua input queue.
		// It's critical that the end offer succeeds.
		while (! inputQueue.offer(LuaValue.NIL)) {
			// Queue must be full. Remove one item to make room.
			if (inputQueue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				if (Log.debugEnabled()) {
					Log.debug("Lua input queue " + statement.taskId + " both offer and poll failed on abort");
				}
				break;				
			}
		}
	}

	@Override
	protected void sendCompleted() {
		// Send end command to lua thread.
		// It's critical that the end put succeeds.
		// Loop through all interrupts.
		while (true) {
			try {
				inputQueue.put(LuaValue.NIL);
				break;
			}
			catch (InterruptedException ie) {
				if (Log.debugEnabled()) {
					Log.debug("Lua input queue " + statement.taskId + " put interrupted");
				}
			}
		}
	}

	public ResultSet getResultSet() {
		return resultSet;
	}
}
