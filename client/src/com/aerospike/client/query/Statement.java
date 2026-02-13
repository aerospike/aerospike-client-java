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
 * Holds parameters for secondary index queries and scans executed via
 * {@link com.aerospike.client.AerospikeClient#query} (RecordSet or listener overload).
 *
 * <p>Use this class to specify namespace, set, optional filter (on an indexed bin), bin names to return,
 * optional aggregation function, and limits such as {@link #setMaxRecords(long)} and
 * {@link #setRecordsPerSecond(int)}.
 *
 * <p>Run a secondary index query with filter and iterate RecordSet; use IAerospikeClient and close RecordSet when done.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("users");
 * stmt.setFilter(Filter.equal("status", "active"));
 * stmt.setBinNames("name", "email");
 * stmt.setMaxRecords(1000);
 * RecordSet rs = client.query(queryPolicy, stmt);
 * try {
 *     for (KeyRecord kr : rs) {
 *         // process kr.record
 *     }
 * } finally {
 *     rs.close();
 * }
 * }</pre>
 *
 * <p>Run a full namespace/set scan with no filter and optional recordsPerSecond limit.</p>
 * <pre>{@code
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("orders");
 * stmt.setRecordsPerSecond(500);
 * RecordSet rs = client.query(queryPolicy, stmt);
 * }</pre>
 *
 * <p>Run an aggregation query with filter and UDF; result type depends on the UDF.</p>
 * <pre>{@code
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("events");
 * stmt.setFilter(Filter.range("age", 18, 65));
 * stmt.setAggregateFunction("myudfs", "countGroup", Value.get("state"));
 * Object result = client.queryAggregate(queryPolicy, stmt);
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#query
 * @see Filter
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
	 * Sets the namespace to query or scan.
	 *
	 * @param namespace the namespace name; must not be {@code null}
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * Returns the namespace set for this statement.
	 *
	 * @return the namespace name, or {@code null} if not set
	 */
	public String getNamespace() {
		return namespace;
	}

	/**
	 * Sets the optional set name to restrict the query or scan to a single set.
	 *
	 * @param setName the set name, or {@code null} to query/scan all sets in the namespace
	 */
	public void setSetName(String setName) {
		this.setName = setName;
	}

	/**
	 * Returns the optional set name for this statement.
	 *
	 * @return the set name, or {@code null} if not set (all sets in namespace)
	 */
	public String getSetName() {
		return setName;
	}

	/**
	 * Sets the optional secondary index name to use for the query.
	 *
	 * <p>If not set, the server determines the index from the filter's bin name. This method
	 * is only applicable to server versions before 6.0; server versions 6.0 and later ignore it.
	 *
	 * @param indexName the index name, or {@code null} to let the server choose from the filter bin
	 */
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * Returns the optional index name set for this statement.
	 *
	 * @return the index name, or {@code null} if not set
	 */
	public String getIndexName() {
		return indexName;
	}

	/**
	 * Sets which bins to return for each record; empty or {@code null} means all bins.
	 *
	 * @param binNames the bin names to return, or {@code null} / empty for all bins
	 */
	public void setBinNames(String... binNames) {
		this.binNames = binNames;
	}

	/**
	 * Returns the bin names to return for each record.
	 *
	 * @return the bin names, or {@code null} if not set (all bins)
	 */
	public String[] getBinNames() {
		return binNames;
	}

	/**
	 * Sets the optional filter applied to the secondary index for the query.
	 *
	 * <p>The filter must reference a bin that has a secondary index defined. If {@code null},
	 * the statement represents a full namespace/set scan (see {@link #isScan()}).
	 *
	 * @param filter the index filter, or {@code null} for a full scan
	 * @see Filter
	 * @see #isScan()
	 */
	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	/**
	 * Returns the index filter for this statement.
	 *
	 * @return the filter, or {@code null} if not set (full scan)
	 */
	public Filter getFilter() {
		return filter;
	}

	/**
	 * Sets the optional task ID for background query/scan execution.
	 *
	 * @param taskId the task ID; use 0 to let the client generate one via {@link #prepareTaskId()}
	 */
	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	/**
	 * Returns the task ID set for this statement.
	 *
	 * @return the task ID, or 0 if not set
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Sets the maximum number of records returned (foreground query) or processed (background
	 * execute query).
	 *
	 * <p>The limit is divided across the nodes involved in the query. The actual number of
	 * records returned may be less than {@code maxRecords} if node record counts are small or
	 * unbalanced.
	 *
	 * @param maxRecords maximum number of records; 0 means no limit
	 */
	public void setMaxRecords(long maxRecords) {
		this.maxRecords = maxRecords;
	}

	/**
	 * Returns the maximum number of records set for this statement.
	 *
	 * @return the maximum record count, or 0 if not set (no limit)
	 */
	public long getMaxRecords() {
		return maxRecords;
	}

	/**
	 * Limits the number of records returned or processed per second (rps) per server.
	 *
	 * <p>If {@code recordsPerSecond} is zero (default), no rps limit is applied. This option
	 * is supported for primary and secondary index queries on server versions 6.0 and later.
	 * For background queries, the effective rps is also bounded by the server config
	 * {@code background-query-max-rps}.
	 *
	 * @param recordsPerSecond maximum records per second per server; 0 for no limit
	 */
	public void setRecordsPerSecond(int recordsPerSecond) {
		this.recordsPerSecond = recordsPerSecond;
	}

	/**
	 * Returns the records-per-second limit set for this statement.
	 *
	 * @return the rps limit, or 0 if not set (no limit)
	 */
	public int getRecordsPerSecond() {
		return recordsPerSecond;
	}

	/**
	 * Sets the Lua aggregation function for a package located on the server filesystem.
	 *
	 * <p>The function is invoked on the server and optionally on the client for each selected
	 * record. Use the other overload {@link #setAggregateFunction(ClassLoader, String, String, String, Value...)}
	 * when the Lua module is loaded from the classpath (e.g. a resource).
	 *
	 * <p><b>Example:</b>
	 * <pre>{@code
	 * Statement stmt = new Statement();
	 * stmt.setNamespace("test");
	 * stmt.setSetName("demo");
	 * stmt.setAggregateFunction("myudfs", "myAggregate", Value.get("arg1"));
	 * }</pre>
	 *
	 * @param packageName server package (module) where the UDF resides; must not be {@code null}
	 * @param functionName name of the aggregation function; must not be {@code null}
	 * @param functionArgs optional arguments to pass to the function; may be empty
	 * @see #setAggregateFunction(ClassLoader, String, String, String, Value...)
	 */
	public void setAggregateFunction(String packageName, String functionName, Value... functionArgs) {
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
	}

	/**
	 * Sets the Lua aggregation function for a package loaded from a classpath resource.
	 *
	 * <p>The function is invoked on the server and optionally on the client for each selected
	 * record. Use this overload when the Lua module is bundled as a resource (e.g. in a JAR).
	 *
	 * <p><b>Example:</b>
	 * <pre>{@code
	 * Statement stmt = new Statement();
	 * stmt.setNamespace("test");
	 * stmt.setSetName("demo");
	 * stmt.setAggregateFunction(MyClass.class.getClassLoader(), "udf/myagg.lua", "myagg", "reduce", Value.get(0));
	 * }</pre>
	 *
	 * @param resourceLoader class loader that can load the resource; e.g. {@code MyClass.class.getClassLoader()} or {@code Thread.currentThread().getContextClassLoader()} for web apps
	 * @param resourcePath classpath path to the Lua resource file; must not be {@code null}
	 * @param packageName server package (module) name where the UDF resides; must not be {@code null}
	 * @param functionName name of the aggregation function; must not be {@code null}
	 * @param functionArgs optional arguments to pass to the function; may be empty
	 * @see #setAggregateFunction(String, String, Value...)
	 */
	public void setAggregateFunction(ClassLoader resourceLoader, String resourcePath, String packageName, String functionName, Value... functionArgs) {
		this.resourceLoader = resourceLoader;
		this.resourcePath = resourcePath;
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
	}

	/**
	 * Returns the class loader used to load the aggregation Lua resource.
	 *
	 * @return the resource class loader, or {@code null} if aggregation uses filesystem package
	 */
	public ClassLoader getResourceLoader() {
		return resourceLoader;
	}

	/**
	 * Returns the classpath path to the aggregation Lua resource.
	 *
	 * @return the resource path, or {@code null} if aggregation uses filesystem package
	 */
	public String getResourcePath() {
		return resourcePath;
	}

	/**
	 * Returns the server package (module) name for the aggregation function.
	 *
	 * @return the package name, or {@code null} if no aggregation was set
	 */
	public String getPackageName() {
		return packageName;
	}

	/**
	 * Returns the aggregation function name.
	 *
	 * @return the function name, or {@code null} if no aggregation was set
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * Returns the arguments passed to the aggregation function.
	 *
	 * @return the function arguments, or {@code null} if no aggregation was set
	 */
	public Value[] getFunctionArgs() {
		return functionArgs;
	}

	/**
	 * Sets the operations to run on the server for each record in a background query.
	 *
	 * <p>Used with {@link com.aerospike.client.AerospikeClient#execute(com.aerospike.client.policy.WritePolicy, Statement, Operation...)}.
	 * Foreground queries that return records to the client ignore these operations.
	 *
	 * @param operations the operations to apply per record; may be {@code null}
	 * @see com.aerospike.client.AerospikeClient#execute(com.aerospike.client.policy.WritePolicy, Statement, Operation...)
	 */
	public void setOperations(Operation[] operations) {
		this.operations = operations;
	}

	/**
	 * Returns the operations to be performed on each record in a background query.
	 *
	 * @return the operations, or {@code null} if not set
	 */
	public Operation[] getOperations() {
		return this.operations;
	}

	/**
	 * No longer used; retained for backward compatibility only.
	 *
	 * @param returnData ignored
	 * @deprecated This method has no effect
	 */
	@Deprecated
	public void setReturnData(boolean returnData) {
		this.returnData = returnData;
	}

	/**
	 * No longer used; retained for backward compatibility only.
	 *
	 * @return the stored value (has no effect on behavior)
	 * @deprecated This method has no effect
	 */
	@Deprecated
	public boolean returnData() {
		return returnData;
	}

	/**
	 * Returns the task ID for this statement, or a newly generated one if none was set.
	 *
	 * @return the user-set task ID if non-zero, otherwise a new unique task ID
	 */
	public long prepareTaskId() {
		return (taskId != 0)? taskId : RandomShift.instance().nextLong();
	}

	/**
	 * Indicates whether this statement represents a full namespace/set scan (no index filter).
	 *
	 * @return {@code true} if no filter is set (full scan), {@code false} if an index filter is set
	 * @see #getFilter()
	 */
	public boolean isScan() {
		return filter == null;
	}
}
