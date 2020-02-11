/*
 * Copyright 2012-2020 Aerospike, Inc.
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
	PredExp[] predExp;
	ClassLoader resourceLoader;
	String resourcePath;
	String packageName;
	String functionName;
	Value[] functionArgs;
	Operation[] operations;
	long taskId;
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
	 * Set optional predicate expression filters in postfix notation.
	 * Predicate expression filters are applied on the query results on the server.
	 * Predicate expression filters may occur on any bin in the record.
	 * Requires Aerospike Server versions >= 3.12
	 * <p>
	 * This method is redundant because PredExp can now be set in the base Policy for
	 * any transaction (including queries).
	 * <p>
	 * Postfix notation is described here:
	 * <a href="http://wiki.c2.com/?PostfixNotation">http://wiki.c2.com/?PostfixNotation</a>
	 * <p>
	 * Example:
	 * <pre>
	 * // (c >= 11 and c <= 20) or (d > 3 and (d < 5)
     * stmt.setPredExp(
     *   PredExp.integerBin("c"),
     *   PredExp.integerValue(11),
     *   PredExp.integerGreaterEq(),
     *   PredExp.integerBin("c"),
     *   PredExp.integerValue(20),
     *   PredExp.integerLessEq(),
     *   PredExp.and(2),
     *   PredExp.integerBin("d"),
     *   PredExp.integerValue(3),
     *   PredExp.integerGreater(),
     *   PredExp.integerBin("d"),
     *   PredExp.integerValue(5),
     *   PredExp.integerLess(),
     *   PredExp.and(2),
     *   PredExp.or(2)
     * );
     *
	 * // Record last update time > 2017-01-15
	 * stmt.setPredExp(
	 *   PredExp.recLastUpdate(),
	 *   PredExp.integerValue(new GregorianCalendar(2017, 0, 15)),
	 *   PredExp.integerGreater()
	 * );
     * </pre>
	 */
	public void setPredExp(PredExp... predExp) {
		this.predExp = predExp;
	}

	/**
	 * Return predicate expression filters.
	 */
	public PredExp[] getPredExp() {
		return predExp;
	}

	/**
	 * Set optional query task id.
	 */
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	/**
	 * Return task ID.
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Limit returned records per second (rps) rate for each server.
	 * Do not apply rps limit if recordsPerSecond is zero (default).
	 * Currently only applicable to a query without a defined filter.
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
	 * @param resourcePath          class path where Lua resource is located
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
	 * Set operations to be performed on query/execute.
	 */
	public void setOperations(Operation[] operations) {
		this.operations = operations;
	}

	/**
	 * Return operations to be performed on query/execute.
	 */
	public Operation[] getOperations() {
		return this.operations;
	}

	/**
	 * Set whether command returns data.
	 */
	public void setReturnData(boolean returnData) {
		this.returnData = returnData;
	}

	/**
	 * Does command return data.
	 */
	public boolean returnData() {
		return returnData;
	}

	/**
	 * Prepare statement just prior to execution.  For internal use.
	 */
	public void prepare(boolean returnData) {
		this.returnData = returnData;

		if (taskId == 0) {
			taskId = RandomShift.instance().nextLong();
		}
	}

	/**
	 * Return if full namespace/set scan is specified.
	 */
	public boolean isScan() {
		return filter == null;
	}
}
