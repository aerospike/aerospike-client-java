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
import com.aerospike.client.Record;
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
	ReduceSpec<?, ?>[] reduceSpecs;
	private ReduceSpec<?, ?> resolvedReduce;
	private boolean reduceResolved;
	private String orderByBin;
	private BinDataType orderByType;
	private Order orderByOrder;
	private OrderByFlags orderByFlags;
	private boolean orderBySet;
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
	 * Set query bin names for bin projection in queries.
	 * Mutually exclusive with {@link #setOperations(Operation[])}.
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
	 * Set operations to be performed on query/execute.
	 * <p>
	 * For foreground queries ({@link com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement)}),
	 * only read operations are allowed. Read operations act as bin projections, limiting which bins are returned.
	 * <p>
	 * Basic read operations ({@link Operation#get(String)}, {@link Operation#get()},
	 * {@link Operation#getHeader()}) are supported on server versions prior to 8.1.2.
	 * Extended read operations (e.g., {@link com.aerospike.client.exp.ExpOperation#read(String, com.aerospike.client.exp.Expression, com.aerospike.client.exp.ExpReadFlags)},
	 * CDT read operations, bit read operations, HLL read operations) require server version 8.1.2+.
	 * <p>
	 * For background execute ({@link com.aerospike.client.AerospikeClient#execute(com.aerospike.client.policy.WritePolicy, Statement, Operation...)}),
	 * only write operations are allowed (e.g., {@link com.aerospike.client.exp.ExpOperation#write}).
	 * <p>
	 * Operations and {@link #setBinNames(String...)} are mutually exclusive. If both are set,
	 * operations take precedence and a warning is logged.
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
	 * Set reduce spec(s) for this query, used for client-side global reduce (e.g. Top-K,
	 * SUM, COUNT, MIN, MAX). Accepts:
	 * <ul>
	 *   <li>zero args — clear any reduce (stream all matching records; default behavior)</li>
	 *   <li>one scalar or Top-K spec — e.g. {@code setReduce(Reduce.sum("amt"))} or
	 *       {@code setReduce(Reduce.topK("d", BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, 10))}</li>
	 *   <li>exactly one {@link Reduce#orderBy} + one {@link Reduce#limit} on the same bin —
	 *       split Top-K, equivalent to {@link Reduce#topK}</li>
	 * </ul>
	 * Mutually exclusive with {@link #setAggregateFunction}. Replaces any previously set reduce
	 * (this setter does not accumulate across calls, consistent with {@link #setOperations(Operation[])}
	 * and {@link #setBinNames(String...)}).
	 */
	public void setReduce(ReduceSpec<?, ?>... reduceSpecs) {
		this.reduceSpecs = reduceSpecs;
		this.resolvedReduce = null;
		this.reduceResolved = false;
	}

	/**
	 * Return reduce spec(s) set by {@link #setReduce(ReduceSpec...)}.
	 */
	public ReduceSpec<?, ?>[] getReduce() {
		return reduceSpecs;
	}

	/**
	 * Sort order building block for {@link #setTopK(int)}. Equivalent to
	 * {@code setOrderBy(binName, type, order, OrderByFlags.NONE)}.
	 * <p>
	 * Sugar: remembers the sort order for a subsequent {@link #setTopK(int)} call, which
	 * together resolve internally to {@code setReduce(Reduce.topK(binName, type, order, flags, k))}.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 */
	public void setOrderBy(String binName, BinDataType type, Order order) {
		setOrderBy(binName, type, order, OrderByFlags.NONE);
	}

	/**
	 * Sort order building block for {@link #setTopK(int)}.
	 * <p>
	 * Sugar: remembers the sort order for a subsequent {@link #setTopK(int)} call, which
	 * together resolve internally to {@code setReduce(Reduce.topK(binName, type, order, flags, k))}.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 * @param flags		comparison options ({@link OrderByFlags#CASE_INSENSITIVE} for
	 *                  {@link BinDataType#STRING} only)
	 */
	public void setOrderBy(String binName, BinDataType type, Order order, OrderByFlags flags) {
		this.orderByBin = binName;
		this.orderByType = type;
		this.orderByOrder = order;
		this.orderByFlags = flags;
		this.orderBySet = true;
	}

	/**
	 * Ordered LIMIT k reduce; must be preceded by a {@link #setOrderBy} call on this statement.
	 * <p>
	 * Sugar for {@code setReduce(Reduce.topK(binName, type, order, flags, k))} using the bin,
	 * type, order, and flags from the preceding {@link #setOrderBy} call. Like
	 * {@link #setReduce(ReduceSpec...)}, replaces any previously set reduce.
	 *
	 * @param k	maximum number of records to return, in {@code [1, 1000]}
	 * @throws IllegalStateException if {@link #setOrderBy} was not called first
	 */
	public void setTopK(int k) {
		if (! orderBySet) {
			throw new IllegalStateException("setTopK() requires setOrderBy() to be called first");
		}
		setReduce(Reduce.topK(orderByBin, orderByType, orderByOrder, orderByFlags, k));
	}

	/**
	 * Resolve the reduce spec(s) set by {@link #setReduce(ReduceSpec...)} into a single combiner
	 * usable by a query executor. Returns {@code null} if no reduce was set. Composes a split
	 * {@link Reduce#orderBy} + {@link Reduce#limit} pair into a single Top-K combiner.
	 *
	 * @throws IllegalArgumentException if the reduce specs are not a single reducer or a valid
	 *                                   orderBy/limit pair on the same bin
	 */
	@SuppressWarnings("unchecked")
	public <I, O> ReduceSpec<I, O> resolveReduce() {
		if (! reduceResolved) {
			resolvedReduce = computeResolveReduce();
			reduceResolved = true;
		}
		return (ReduceSpec<I, O>)resolvedReduce;
	}

	@SuppressWarnings("unchecked")
	private ReduceSpec<?, ?> computeResolveReduce() {
		if (reduceSpecs == null || reduceSpecs.length == 0) {
			return null;
		}

		boolean isSplit = reduceSpecs[0] instanceof OrderByReduceSpec || reduceSpecs[0] instanceof LimitReduceSpec;

		if (reduceSpecs.length == 1 && !isSplit) {
			return reduceSpecs[0];
		}

		ReduceSpec<Record, Record> orderBy = null;
		ReduceSpec<Record, Record> limit = null;

		for (ReduceSpec<?, ?> spec : reduceSpecs) {
			if (spec instanceof OrderByReduceSpec) {
				orderBy = (ReduceSpec<Record, Record>)spec;
			}
			else if (spec instanceof LimitReduceSpec) {
				limit = (ReduceSpec<Record, Record>)spec;
			}
			else {
				throw new IllegalArgumentException("Cannot mix topK parts (orderBy/limit) with other reducers");
			}
		}

		if (orderBy == null || limit == null) {
			throw new IllegalArgumentException("topK requires both an orderBy spec and a limit spec");
		}
		return TopKReduceSpec.compose(orderBy, limit);
	}

	/**
	 * For internal use by query executors. Validate that this statement's reduce (if any) is
	 * compatible with record-streaming queries (e.g.
	 * {@link com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement)}),
	 * which return full records via a {@link RecordSet} and therefore only support a Top-K (or no)
	 * reduce.
	 *
	 * @throws IllegalArgumentException if a scalar reduce (sum/count/min/max) is set
	 */
	public void validateRecordQuery() {
		ReduceSpec<?, ?> reduce = resolveReduce();

		if (reduce != null && !(reduce instanceof TopKReduceSpec)) {
			throw new IllegalArgumentException(
				"Statement has a scalar reduce (sum/count/min/max) set. Use queryReduce() instead of query().");
		}
	}

	/**
	 * For internal use by query executors. Validate that this statement's reduce is compatible
	 * with scalar reduce queries (e.g.
	 * {@link com.aerospike.client.AerospikeClient#queryReduce(com.aerospike.client.policy.QueryPolicy, Statement)}),
	 * which return a single scalar result and therefore require a scalar reduce spec
	 * (sum/count/min/max).
	 *
	 * @throws IllegalArgumentException if no reduce is set, or a Top-K reduce is set
	 */
	public void validateReduceQuery() {
		ReduceSpec<?, ?> reduce = resolveReduce();

		if (reduce == null) {
			throw new IllegalArgumentException(
				"Statement has no reduce set. Call setReduce() with a scalar reducer " +
				"(Reduce.sum/count/min/max) before calling queryReduce().");
		}

		if (reduce instanceof TopKReduceSpec) {
			throw new IllegalArgumentException(
				"Statement has a Top-K reduce set. Use query() instead of queryReduce() " +
				"for Top-K / orderBy+limit reduces.");
		}
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
