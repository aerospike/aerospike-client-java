/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.query;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.RandomShift;

/**
 * Query statement parameters.
 */
public final class Statement {
	String namespace;
	String setName;
	String indexName;
	String[] binNames;
	Filter filter;
	ClassLoader resourceLoader;
	String resourcePath;
	String packageName;
	String functionName;
	Value[] functionArgs;
	Operation[] operations;
	long taskId;
	long maxRecords;
	int recordsPerSecond;
	boolean returnData;

	/**
	 * Set query namespace.
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * Get query namespace.
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Set optional query setname.
	 */
	public void setSetName(String setName) {
		this.setName = setName;
	}

	/**
	 * Get optional query setname.
	 */
	public String getSetName() {
		return setName;
	}

	/**
	 * Set optional query index name.  If not set, the server
	 * will determine the index from the filter's bin name.
	 * Note, the call is only applicable to pre-6.0 server versions,
	 * and is ignored by server versions 6.0 and later.
	 */
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * Get optional query index name.
	 */
	public String getIndexName() {
		return indexName;
	}

	/**
	 * Set query bin names.
	 */
	public void setBinNames(String... binNames) {
		this.binNames = binNames;
	}

	/**
	 * Get query bin names.
	 */
	public String[] getBinNames() {
		return binNames;
	}

	/**
	 * Set optional query index filter.  This filter is applied to the secondary index on query.
	 * Query index filters must reference a bin which has a secondary index defined.
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	/**
	 * Return query index filter.
	 */
	public Filter getFilter() {
		return filter;
	}

	/**
	 * Set optional task id.
	 */
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	/**
	 * Return optional task id.
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Set maximum number of records returned (for foreground query) or processed
	 * (for background execute query). This number is divided by the number of nodes
	 * involved in the query. The actual number of records returned may be less than
	 * maxRecords if node record counts are small and unbalanced across nodes.
	 */
	public void setMaxRecords(long maxRecords) {
		this.maxRecords = maxRecords;
	}

	/**
	 * Return maximum number of records.
	 */
	public long getMaxRecords() {
		return maxRecords;
	}

	/**
	 * Limit returned records per second (rps) rate for each server.
	 * Do not apply rps limit if recordsPerSecond is zero (default).
	 * <p>
	 * recordsPerSecond is supported in all primary and secondary index
	 * queries in server versions 6.0+. For background queries, recordsPerSecond
	 * is bounded by the server config background-query-max-rps.
	 */
	public void setRecordsPerSecond(int recordsPerSecond) {
		this.recordsPerSecond = recordsPerSecond;
	}

	/**
	 * Return records per second.
	 */
	public int getRecordsPerSecond() {
		return recordsPerSecond;
	}

	/**
	 * Set Lua aggregation function parameters for a Lua package located on the filesystem.
	 * This function will be called on both the server and client for each selected item.
	 *
	 * @param packageName			server package where user defined function resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function name, if any
	 */
	public void setAggregateFunction(String packageName, String functionName, Value... functionArgs) {
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
	}

	/**
	 * Set Lua aggregation function parameters for a Lua package located in a resource file.
	 * This function will be called on both the server and client for each selected item.
	 *
	 * @param resourceLoader		class loader where resource is located.  Example: MyClass.class.getClassLoader() or Thread.currentThread().getContextClassLoader() for webapps
	 * @param resourcePath			class path where Lua resource is located
	 * @param packageName			server package where user defined function resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function name, if any
	 */
	public void setAggregateFunction(ClassLoader resourceLoader, String resourcePath, String packageName, String functionName, Value... functionArgs) {
		this.resourceLoader = resourceLoader;
		this.resourcePath = resourcePath;
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
	}

	/**
	 * Return resource class loader.
	 */
	public ClassLoader getResourceLoader() {
		return resourceLoader;
	}

	/**
	 * Return resource path.
	 */
	public String getResourcePath() {
		return resourcePath;
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
	 * Set operations to be performed on a background query
	 * {@link com.aerospike.client.AerospikeClient#execute(com.aerospike.client.policy.WritePolicy, Statement, Operation...)}
	 * A foreground query that returns records to the client will silently ignore these operations.
	 */
	public void setOperations(Operation[] operations) {
		this.operations = operations;
	}

	/**
	 * Return operations to be performed on a background query.
	 */
	public Operation[] getOperations() {
		return this.operations;
	}

	/**
	 * Not used anymore.
	 */
	@Deprecated
	public void setReturnData(boolean returnData) {
		this.returnData = returnData;
	}

	/**
	 * Not used anymore.
	 */
	@Deprecated
	public boolean returnData() {
		return returnData;
	}

	/**
	 * Return taskId if set by user. Otherwise return a new taskId.
	 */
	public long prepareTaskId() {
		if (taskId != 0) {
			return taskId;
		}

		RandomShift random = new RandomShift();
		return random.nextLong();
	}

	/**
	 * Return if full namespace/set scan is specified.
	 */
	public boolean isScan() {
		return filter == null;
	}
}
