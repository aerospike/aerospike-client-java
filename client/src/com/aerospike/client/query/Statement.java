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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.util.RandomShift;

/**
 * Query statement parameters.
 */
public final class Statement {
	private static final String NO_ORDER_KEY = "Top-K requires an order key; call setOrderBy()";
	private static final int MAX_ORDER_KEYS = 2;

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
	private final List<OrderBy> orderBy = new ArrayList<>(MAX_ORDER_KEYS);
	private int topK;
	private boolean topKSet;
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
	 * Set reduce spec(s) for this query. Accepts:
	 * <ul>
	 *   <li>zero args — clear any reduce (stream all matching records; default behavior)</li>
	 *   <li>one Top-K spec — e.g.
	 *       {@code setReduce(Reduce.topK("d", BinDataType.DOUBLE, Order.ASC, OrderByFlags.NONE, 10))}</li>
	 *   <li>exactly one {@link Reduce#orderBy} + one {@link Reduce#limit} on the same bin —
	 *       split Top-K, equivalent to {@link Reduce#topK}</li>
	 * </ul>
	 * Mutually exclusive with {@link #setAggregateFunction}. Replaces any previously set reduce
	 * (this setter does not accumulate across calls, consistent with {@link #setOperations(Operation[])}
	 * and {@link #setBinNames(String...)}).
	 * <p>
	 * Not public for now — {@link #setOrderBy} + {@link #setTopK} are the only supported public
	 * entry points into the reduce framework while the API is still settling.
	 */
	void setReduce(ReduceSpec<?, ?>... reduceSpecs) {
		this.reduceSpecs = reduceSpecs;
		this.orderBy.clear();
		this.topKSet = false;
	}

	/**
	 * Return reduce spec(s) set by {@code setReduce}.
	 */
	ReduceSpec<?, ?>[] getReduce() {
		return reduceSpecs;
	}

	/**
	 * Sort order for a Top-K query; see {@link #setTopK(int)}. Equivalent to
	 * {@code setOrderBy(binName, type, order, OrderByFlags.NONE)}.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 */
	public void setOrderBy(String binName, BinDataType type, Order order) {
		setOrderBy(binName, type, order, OrderByFlags.NONE);
	}

	/**
	 * Sort order for a Top-K query; see {@link #setTopK(int)}. May be called before or after
	 * {@code setTopK}. Calling it again replaces every order key, including one added by
	 * {@link #thenOrderBy}, and keeps k.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 * @param flags		comparison options ({@link OrderByFlags#CASE_INSENSITIVE} for
	 *                  {@link BinDataType#STRING} only)
	 */
	public void setOrderBy(String binName, BinDataType type, Order order, OrderByFlags flags) {
		orderBy.clear();
		orderBy.add(new OrderBy(binName, type, order, flags));
	}

	/**
	 * Add an order key for a Top-K query after the ones already set. Equivalent to
	 * {@code thenOrderBy(binName, type, order, OrderByFlags.NONE)}.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 */
	public void thenOrderBy(String binName, BinDataType type, Order order) {
		thenOrderBy(binName, type, order, OrderByFlags.NONE);
	}

	/**
	 * Add an order key for a Top-K query after the ones already set. A later key decides only
	 * where the keys before it tie; called before {@link #setOrderBy}, it adds the first key.
	 * A Top-K query takes at most two order keys, on different bins.
	 *
	 * @param binName	bin name to order by
	 * @param type		scalar type of {@code binName}
	 * @param order		sort direction
	 * @param flags		comparison options ({@link OrderByFlags#CASE_INSENSITIVE} for
	 *                  {@link BinDataType#STRING} only)
	 */
	public void thenOrderBy(String binName, BinDataType type, Order order, OrderByFlags flags) {
		orderBy.add(new OrderBy(binName, type, order, flags));
	}

	/**
	 * Ordered LIMIT k query, ranked by the order keys from {@link #setOrderBy} and
	 * {@link #thenOrderBy}. The calls may come in any order; calling {@code setTopK} again
	 * replaces k. Like {@code setReduce}, replaces any previously set reduce.
	 * <p>
	 * The Top-K specification is validated when the query runs; an invalid one, or a cluster where
	 * any node doesn't support Top-K, fails with {@link AerospikeException.InvalidArgument}
	 * ({@link ResultCode#PARAMETER_ERROR}) before anything is sent.
	 *
	 * @param k	maximum number of records to return, in {@code [1, 1000]}
	 */
	public void setTopK(int k) {
		this.reduceSpecs = null;
		this.topK = k;
		this.topKSet = true;
	}

	/**
	 * Return whether this statement has a Top-K specification.
	 */
	public boolean hasTopK() {
		return topKSet;
	}

	/**
	 * Return whether this statement has an order key, with or without k.
	 */
	public boolean hasOrderBy() {
		return ! orderBy.isEmpty();
	}

	/**
	 * Return the order keys, most significant first, or {@code null} when none is set. An order
	 * key without k is still sent, and the server rejects the query.
	 */
	public OrderBy[] getOrderBy() {
		return orderBy.isEmpty() ? null : orderBy.toArray(new OrderBy[0]);
	}

	/**
	 * Return the Top-K limit, or zero when Top-K is not set.
	 */
	public int getTopKLimit() {
		return topKSet ? topK : 0;
	}

	/**
	 * Validate the Top-K specification. Does nothing unless k is set.
	 *
	 * @throws AerospikeException.InvalidArgument	if Top-K has no order key or more than two, k
	 *								is outside {@code [1, 1000]}, maxRecords is set, an aggregate
	 *								UDF is set, an order key's bin name is not 1-15 UTF-8 bytes
	 *								without NUL, it has no type or direction, it has flags on a
	 *								non-STRING type, its bin is missing from the bin or operations
	 *								projection or produced more than once by the operations
	 *								projection, or both order keys use the same bin
	 */
	public void validateTopK() {
		if (! topKSet) {
			return;
		}

		if (orderBy.isEmpty()) {
			throw new AerospikeException.InvalidArgument(NO_ORDER_KEY);
		}

		if (orderBy.size() > MAX_ORDER_KEYS) {
			throw new AerospikeException.InvalidArgument("orderBy supports at most two keys");
		}

		if (topK < 1 || topK > 1000) {
			throw new AerospikeException.InvalidArgument("topK must be in [1, 1000]; got " + topK);
		}

		if (maxRecords != 0) {
			throw new AerospikeException.InvalidArgument("Top-K is incompatible with maxRecords");
		}

		if (functionName != null) {
			throw new AerospikeException.InvalidArgument(
				"orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage");
		}

		for (OrderBy key : orderBy) {
			validateOrderBy(key);
		}

		if (orderBy.size() > 1 && orderBy.get(0).binName.equals(orderBy.get(1).binName)) {
			throw new AerospikeException.InvalidArgument("orderBy keys must use different bins");
		}
	}

	private void validateOrderBy(OrderBy key) {
		int length = (key.binName == null) ? 0 : Buffer.estimateSizeUtf8(key.binName);

		if (length == 0 || length > 15 || key.binName.indexOf('\0') >= 0) {
			throw new AerospikeException.InvalidArgument(
				"Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL");
		}

		if (key.type == null) {
			throw new AerospikeException.InvalidArgument(
				"orderBy requires a type declaration (INTEGER or DOUBLE); Aerospike has no schema");
		}

		if (key.order == null) {
			throw new AerospikeException.InvalidArgument("orderBy direction must be ASC or DESC");
		}

		if (key.flags == null) {
			throw new AerospikeException.InvalidArgument(
				"orderBy flags must not be null; use OrderByFlags.NONE");
		}

		if (key.flags != OrderByFlags.NONE && key.type != BinDataType.STRING) {
			throw new AerospikeException.InvalidArgument("orderBy flags are only valid with type STRING");
		}

		if (operations != null) {
			int count = 0;

			for (Operation operation : operations) {
				if (key.binName.equals(operation.binName)) {
					count++;
				}
			}

			if (count == 0) {
				throw notInProjection(key.binName);
			}

			if (count > 1) {
				throw new AerospikeException.InvalidArgument(
					"orderBy bin '" + key.binName + "' is produced by more than one operation");
			}
			return;
		}

		if (binNames != null) {
			for (String binName : binNames) {
				if (key.binName.equals(binName)) {
					return;
				}
			}
			throw notInProjection(key.binName);
		}
	}

	private static AerospikeException notInProjection(String binName) {
		return new AerospikeException.InvalidArgument("orderBy bin '" + binName +
			"' is not in projection; add it to the projection or remove the projection");
	}

	/**
	 * Resolve the Top-K specification, or the reduce spec(s) set by {@code setReduce}, into a
	 * single combiner usable by a query executor. Returns {@code null} if neither was set.
	 * Composes a split {@link Reduce#orderBy} + {@link Reduce#limit} pair into a single Top-K
	 * combiner. A Top-K combiner is returned empty on every call, so a statement that runs again
	 * does not rank the records of an earlier run.
	 *
	 * @throws AerospikeException.InvalidArgument	if Top-K has no order key, or the reduce specs
	 *								are not a single reducer or a valid orderBy/limit pair on the
	 *								same bin
	 */
	@SuppressWarnings("unchecked")
	<I, O> ReduceSpec<I, O> resolveReduce() {
		if (topKSet) {
			if (orderBy.isEmpty()) {
				throw new AerospikeException.InvalidArgument(NO_ORDER_KEY);
			}
			return (ReduceSpec<I, O>)new TopKReduceSpec(orderBy.toArray(new OrderBy[0]), topK);
		}

		if (reduceSpecs == null || reduceSpecs.length == 0) {
			return null;
		}

		boolean isSplit = reduceSpecs[0] instanceof OrderByReduceSpec || reduceSpecs[0] instanceof LimitReduceSpec;

		if (reduceSpecs.length == 1 && !isSplit) {
			ReduceSpec<?, ?> spec = reduceSpecs[0];

			if (spec instanceof TopKReduceSpec) {
				return (ReduceSpec<I, O>)((TopKReduceSpec)spec).emptyCopy();
			}
			return (ReduceSpec<I, O>)spec;
		}

		ReduceSpec<Record, Record> orderBySpec = null;
		ReduceSpec<Record, Record> limit = null;

		for (ReduceSpec<?, ?> spec : reduceSpecs) {
			if (spec instanceof OrderByReduceSpec) {
				if (orderBySpec != null) {
					throw new AerospikeException.InvalidArgument("topK requires exactly one orderBy spec");
				}
				orderBySpec = (ReduceSpec<Record, Record>)spec;
			}
			else if (spec instanceof LimitReduceSpec) {
				if (limit != null) {
					throw new AerospikeException.InvalidArgument("topK requires exactly one limit spec");
				}
				limit = (ReduceSpec<Record, Record>)spec;
			}
			else {
				throw new AerospikeException.InvalidArgument(
					"Cannot mix topK parts (orderBy/limit) with other reducers");
			}
		}

		if (orderBySpec == null || limit == null) {
			throw new AerospikeException.InvalidArgument(
				"topK requires both an orderBy spec and a limit spec");
		}
		return (ReduceSpec<I, O>)TopKReduceSpec.compose(orderBySpec, limit);
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
