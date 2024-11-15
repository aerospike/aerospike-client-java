/*
 * Copyright 2012-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
package com.aerospike.client.proxy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.Command;
import com.aerospike.client.lua.LuaCache;
import com.aerospike.client.lua.LuaInputStream;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.lua.LuaOutputStream;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.proxy.grpc.GrpcCallExecutor;
import com.aerospike.client.proxy.grpc.GrpcConversions;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.proxy.client.Kvs;
import com.aerospike.proxy.client.QueryGrpc;

/**
 * Query aggregation command for the proxy.
 */
public final class QueryAggregateCommandProxy extends MultiCommandProxy implements Runnable {
	private final BlockingQueue<LuaValue> inputQueue;
	private final ResultSetProxy resultSet;
	private final LuaInstance lua;
	private final Statement statement;
	private final AtomicBoolean done;
	private final long taskId;
	private volatile Exception exception;

	public QueryAggregateCommandProxy(
		GrpcCallExecutor executor,
		ThreadFactory threadFactory,
		QueryPolicy queryPolicy,
		Statement statement,
		long taskId
	) {
		super(QueryGrpc.getQueryStreamingMethod(), executor, queryPolicy);
		this.statement = statement;
		this.taskId = taskId;
		this.inputQueue = new ArrayBlockingQueue<>(500);
		this.resultSet = new ResultSetProxy(this, queryPolicy.recordQueueSize);
		this.done = new AtomicBoolean();

		// Work around luaj LuaInteger static initialization bug.
		// Calling LuaInteger.valueOf(long) is required because LuaValue.valueOf() does not have
		// a method that takes in a long parameter.  The problem is directly calling
		// LuaInteger.valueOf(long) results in a static initialization error.
		//
		// If LuaValue.valueOf() is called before any luaj calls, then the static initializer in
		// LuaInteger will be initialized properly.
		LuaValue.valueOf(0);

		// Retrieve lua instance from cache.
		lua = LuaCache.getInstance();

		try {
			// Start Lua virtual thread which reads from a queue, applies aggregate function and
			// writes to a result set.
			threadFactory.newThread(this).start();
		}
		catch (RuntimeException re) {
			// Put the lua instance back if thread creation fails.
			LuaCache.putInstance(lua);
			throw re;
		}
	}

	@Override
	void writeCommand(Command command) {
		// Nothing to do since there is no Aerospike payload.
	}

	@Override
	void parseResult(Parser parser) {
		int resultCode = parser.parseHeader();
		parser.skipKey();

		if (resultCode != 0) {
			// Aggregation scans (with null query filter) will return KEY_NOT_FOUND_ERROR
			// when the set does not exist on the target node.
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				// Non-fatal error.
				return;
			}
			throw new AerospikeException(resultCode);
		}

		if (! super.hasNext) {
			sendCompleted();
			return;
		}

		if (parser.opCount != 1) {
			throw new AerospikeException("Query aggregate expected exactly " +
				"one bin.  Received " + parser.opCount);
		}

		LuaValue aggregateValue = parser.getLuaAggregateValue(lua);

		if (done.get()) {
			throw new AerospikeException.QueryTerminated();
		}

		if (aggregateValue != null) {
			try {
				inputQueue.put(aggregateValue);
			}
			catch (InterruptedException ie) {
				// Ignore
			}
		}
	}

	@Override
	void onFailure(AerospikeException ae) {
		stop(ae);
	}

	@Override
	Kvs.AerospikeRequestPayload.Builder getRequestBuilder() {
		// Set the query parameters in the Aerospike request payload.
		Kvs.AerospikeRequestPayload.Builder builder = Kvs.AerospikeRequestPayload.newBuilder();
		Kvs.QueryRequest.Builder queryRequestBuilder =
			Kvs.QueryRequest.newBuilder();

		queryRequestBuilder.setQueryPolicy(GrpcConversions.toGrpc((QueryPolicy)policy));
		queryRequestBuilder.setStatement(GrpcConversions.toGrpc(statement, taskId, 0));
		builder.setQueryRequest(queryRequestBuilder.build());
		return builder;
	}

	public void stop(Exception cause) {
		// There is no need to stop threads if all threads have already completed.
		if (done.compareAndSet(false, true)) {
			exception = cause;
			sendCancel();
		}
	}

	private void sendCompleted() {
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
					Log.debug("Lua input queue " + taskId + " put " +
						"interrupted");
				}
			}
		}
	}

	private void sendCancel() {
		// Clear lua input queue to ensure cancel is accepted.
		inputQueue.clear();
		resultSet.abort();

		// Send end command to lua input queue.
		// It's critical that the end offer succeeds.
		while (!inputQueue.offer(LuaValue.NIL)) {
			// Queue must be full. Remove one item to make room.
			if (inputQueue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				if (Log.debugEnabled()) {
					Log.debug("Lua input queue " + taskId + " both " +
						"offer and poll failed on abort");
				}
				break;
			}
		}
	}

	public void checkForException() {
		// Throw an exception if an error occurred.
		if (exception != null) {
			if (exception instanceof AerospikeException) {
				throw (AerospikeException)exception;
			}
			else {
				throw new AerospikeException(exception);
			}
		}
	}

	public void run() {
		try {
			lua.loadPackage(statement);

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
		catch (Exception e) {
			stop(e);
		}
		finally {
			// Send end command to user's result set.
			// If query was already cancelled, this put will be ignored.
			resultSet.put(ResultSet.END);
			LuaCache.putInstance(lua);
		}
	}

	long getTaskId() {
		return taskId;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}
}
