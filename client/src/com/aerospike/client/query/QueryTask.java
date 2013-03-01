/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Priority;

/**
 * Asynchronous task handle for database requests that return data.
 */
public class QueryTask extends FutureTask<Void> {	
	/**
	 * Asynchronous return status callback definition. 
	 */
	public interface Callback {
		public void queryNext(QueryTask task, RecordSet rs);
		public void queryComplete(QueryTask task);
	}
	
	/**
	 * Asynchronous return status callback. 
	 */
	public final Callback callback;
	
	/**
	 * Task ID.
	 */
	public final int id;
	
	/**
	 * Percent done per node
	 */
	private Map<Node,Double> nodeStatusMap;  

	/**
	 * Initialize asynchronous task. 
	 */
	public QueryTask(int id, Callable<Void> callable, Callback callback) {
		super(callable);
		this.id = id;
		this.callback = callback;
	}

	/**
	 * Run task at different priority. Not implemented.
	 */
	public void reprioritize(Priority priority) {
	}
			
	/**
	 * Get completion percent of request on a per node basis.
	 */
	public Map<Node,Double> getNodeStatusMap() {
		return nodeStatusMap;
	}
}
