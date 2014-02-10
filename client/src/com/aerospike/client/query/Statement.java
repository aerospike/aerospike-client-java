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
