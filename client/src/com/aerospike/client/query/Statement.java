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
	 * Currently, only one filter is allowed by the server on a secondary index lookup.
	 * If multiple filters are necessary, see QueryFilter example for a workaround.
	 * QueryFilter demonstrates how to add additional filters in an user-defined 
	 * aggregation function. 
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
