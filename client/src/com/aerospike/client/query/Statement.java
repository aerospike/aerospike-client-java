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

import com.aerospike.client.Value;

/**
 * Query statement parameters.
 */
public final class Statement {	
	String namespace;
	String setName;
	String indexName;
	String[] binNames;
	Filter[] filters;
	String packageName;
	String functionName;
	Value[] functionArgs;
	int taskId;
	boolean returnData;
	
	/**
	 * Set query namespace.
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	/**
	 * Set optional query setname.
	 */
	public void setSetName(String setName) {
		this.setName = setName;
	}

	/**
	 * Set optional query index name.  If not set, the server
	 * will determine the index from the filter's bin name.
	 */
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * Set query bin names.
	 */
	public void setBinNames(String... binNames) {
		this.binNames = binNames;
	}
	
	/**
	 * Set optional query filters.
	 */
	public void setFilters(Filter... filters) {
		this.filters = filters;
	}

	/**
	 * Set optional query task id.
	 */
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	/**
	 * Set Lua aggregation function parameters.  This function will be called on both the server 
	 * and client for each selected item.
	 * 
	 * @param packageName			server package where user defined function resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function name, if any
	 */
	void setAggregateFunction(String packageName, String functionName, Value[] functionArgs, boolean returnData) {
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
		this.returnData = returnData;
	}
	
	/**
	 * Return if full namespace/set scan is specified.
	 */
	public boolean isScan() {
		return filters == null;
	}

	/**
	 * Return query filters.
	 */
	public Filter[] getFilters() {
		return filters;
	}

	/**
	 * Return aggregation file name.
	 */
	public String getPackageName() {
		return packageName;
	}

	/**
	 * Return aggregation function name.
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * Return aggregation function arguments.
	 */
	public Value[] getFunctionArgs() {
		return functionArgs;
	}
	
	/**
	 * Return task ID.
	 */
	public int getTaskId() {
		return taskId;
	}
}
