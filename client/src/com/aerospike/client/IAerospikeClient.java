/*
 * Copyright 2012-2015 Aerospike, Inc.
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
package com.aerospike.client;

import java.util.List;

import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

/**
 * This interface's sole purpose is to allow mock frameworks to operate on
 * AerospikeClient without being constrained by final methods.
 */
public interface IAerospikeClient {
	//-------------------------------------------------------
	// Cluster Connection Management
	//-------------------------------------------------------
		
	/**
	 * Close all client connections to database server nodes.
	 */
	public void close();

	/**
	 * Determine if we are ready to talk to the database server cluster.
	 * 
	 * @return	<code>true</code> if cluster is ready,
	 * 			<code>false</code> if cluster is not ready
	 */
	public boolean isConnected();

	/**
	 * Return array of active server nodes in the cluster.
	 * 
	 * @return	array of active nodes
	 */
	public Node[] getNodes();

	/**
	 * Return list of active server node names in the cluster.
	 * 
	 * @return	list of active node names
	 */
	public List<String> getNodeNames();
	
	/**
	 * Return node given its name.
	 * 
	 * @throws AerospikeException.InvalidNode	if node does not exist.
	 */
	public Node getNode(String nodeName) throws AerospikeException.InvalidNode;

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------
	
	/**
	 * Write record bin(s).
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if write fails
	 */
	public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------
	
	/**
	 * Append bin string values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if append fails
	 */
	public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;
	
	/**
	 * Prepend bin string values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call works only for string values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if prepend fails
	 */
	public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------
	
	/**
	 * Add integer bin values to existing record bin values.
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if add fails
	 */
	public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------

	/**
	 * Delete record for specified key.
	 * The policy specifies the transaction timeout.
	 * 
	 * @param policy				delete configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						whether record existed on server before deletion 
	 * @throws AerospikeException	if delete fails
	 */
	public boolean delete(WritePolicy policy, Key key) throws AerospikeException;

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	/**
	 * Reset record's time to expiration using the policy's expiration.
	 * Fail if the record does not exist.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if touch fails
	 */
	public void touch(WritePolicy policy, Key key) throws AerospikeException;

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------

	/**
	 * Determine if a record key exists.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						whether record exists or not 
	 * @throws AerospikeException	if command fails
	 */
	public boolean exists(Policy policy, Key key) throws AerospikeException;

	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 * The policy can be used to specify timeouts.
	 *  
	 * @deprecated Use {@link #exists(BatchPolicy, Key[])} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array key/existence status pairs
	 * @throws AerospikeException	if command fails
	 */
	@Deprecated
	public boolean[] exists(Policy policy, Key[] keys) throws AerospikeException;

	/**
	 * Check if multiple record keys exist in one batch call.
	 * The returned boolean array is in positional order with the original key array order.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 *  
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array key/existence status pairs
	 * @throws AerospikeException	if command fails
	 */
	public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException;

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------

	/**
	 * Read entire record for specified key.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public Record get(Policy policy, Key key) throws AerospikeException;

	/**
	 * Read record header and bins for specified key.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public Record get(Policy policy, Key key, String... binNames) throws AerospikeException;

	/**
	 * Read record generation and expiration only for specified key.  Bins are not read.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @return						if found, return record instance.  If not found, return null.
	 * @throws AerospikeException	if read fails
	 */
	public Record getHeader(Policy policy, Key key) throws AerospikeException;

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Read multiple records for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @deprecated Use {@link #get(BatchPolicy, Key[])} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	@Deprecated
	public Record[] get(Policy policy, Key[] keys) throws AerospikeException;

	/**
	 * Read multiple records for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Read multiple record headers and bins for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @deprecated Use {@link #get(BatchPolicy, Key[], String...)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	@Deprecated
	public Record[] get(Policy policy, Key[] keys, String... binNames) 
		throws AerospikeException;

	/**
	 * Read multiple record headers and bins for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException;

	/**
	 * Read multiple record header data for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @deprecated Use {@link #getHeader(BatchPolicy, Key[])} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	@Deprecated
	public Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException;
	
	/**
	 * Read multiple record header data for specified keys in one batch call.
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @return						array of records
	 * @throws AerospikeException	if read fails
	 */
	public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException;

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------

	/**
	 * Perform multiple read/write operations on a single key in one batch call.
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * Write operations are always performed first, regardless of operation order 
	 * relative to read operations.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @return						record if there is a read in the operations list
	 * @throws AerospikeException	if command fails
	 */
	public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException;

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------
	
	/**
	 * Read all records in specified namespace and set.  If the policy's 
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if scan fails
	 */
	public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
		throws AerospikeException;

	/**
	 * Read all records in specified namespace and set for one node only.
	 * The node is specified by name.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param nodeName				server node name
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.  
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if scan fails
	 */
	public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) 
		throws AerospikeException;

	/**
	 * Read all records in specified namespace and set for one node only.
	 * <p>
	 * This call will block until the scan is complete - callbacks are made
	 * within the scope of this call.
	 * 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param node					server node
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param callback				read callback method - called with record data
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.  
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if transaction fails
	 */
	public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) 
		throws AerospikeException;

	//-------------------------------------------------------------------
	// Large collection functions (Supported by Aerospike 3 servers only)
	//-------------------------------------------------------------------

	/**
	 * Initialize large list operator.  This operator can be used to create and manage a list 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #getLargeList(WritePolicy policy, Key key, String binName)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			no longer used, pass in null.
	 */
	public LargeList getLargeList(Policy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large list operator.  This operator can be used to create and manage a list 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #getLargeList(WritePolicy policy, Key key, String binName)} instead. 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			no longer used, pass in null.
	 */
	public LargeList getLargeList(WritePolicy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large list operator.  This operator can be used to create and manage a list 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 */
	public LargeList getLargeList(WritePolicy policy, Key key, String binName);

	/**
	 * Initialize large map operator.  This operator can be used to create and manage a map 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #getLargeMap(WritePolicy policy, Key key, String binName, String userModule)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeMap getLargeMap(Policy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large map operator.  This operator can be used to create and manage a map 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeMap getLargeMap(WritePolicy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large set operator.  This operator can be used to create and manage a set 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #getLargeSet(WritePolicy policy, Key key, String binName, String userModule)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeSet getLargeSet(Policy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large set operator.  This operator can be used to create and manage a set 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeSet getLargeSet(WritePolicy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large stack operator.  This operator can be used to create and manage a stack 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #getLargeStack(WritePolicy policy, Key key, String binName, String userModule)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeStack getLargeStack(Policy policy, Key key, String binName, String userModule);

	/**
	 * Initialize large stack operator.  This operator can be used to create and manage a stack 
	 * within a single bin.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
	 */
	public LargeStack getLargeStack(WritePolicy policy, Key key, String binName, String userModule);

	//---------------------------------------------------------------
	// User defined functions (Supported by Aerospike 3 servers only)
	//---------------------------------------------------------------
	
	/**
	 * Register package located in a file containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param clientPath			path of client file containing user defined functions, relative to current directory
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) 
		throws AerospikeException;
	
	/**
	 * Register package located in a resource containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param resourceLoader		class loader where resource is located.  Example: MyClass.class.getClassLoader() or Thread.currentThread().getContextClassLoader() for webapps
	 * @param resourcePath          class path where Lua resource is located
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) 
		throws AerospikeException;

	/**
	 * Execute user defined function on server and return results.
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)} instead. 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param args					arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public Object execute(Policy policy, Key key, String packageName, String functionName, Value... args) 
		throws AerospikeException;
	
	/**
	 * Execute user defined function on server and return results.
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param args					arguments passed in to user defined function
	 * @return						return value of user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) 
		throws AerospikeException;

	//----------------------------------------------------------
	// Query/Execute UDF (Supported by Aerospike 3 servers only)
	//----------------------------------------------------------

	/**
	 * Apply user defined function on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @deprecated Use {@link #execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)} instead. 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param statement				record filter
	 * @param packageName			server package where user defined function resides
	 * @param functionName			function name
	 * @param functionArgs			to pass to function name, if any
	 * @throws AerospikeException	if command fails
	 */
	public ExecuteTask execute(
		Policy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException;

	/**
	 * Apply user defined function on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param statement				record filter
	 * @param packageName			server package where user defined function resides
	 * @param functionName			function name
	 * @param functionArgs			to pass to function name, if any
	 * @throws AerospikeException	if command fails
	 */
	public ExecuteTask execute(
		WritePolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException;

	//--------------------------------------------------------
	// Query functions (Supported by Aerospike 3 servers only)
	//--------------------------------------------------------

	/**
	 * Execute query on all server nodes and return record iterator.  The query executor puts 
	 * records on a queue in separate threads.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException;
	
	/**
	 * Execute query on a single server node and return record iterator.  The query executor puts
	 * records on a queue in a separate thread.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException;

	/**
	 * Execute query, apply statement's aggregation function, and return result iterator. The query 
	 * executor puts results on a queue in separate threads.  The calling thread concurrently pops 
	 * results off the queue through the result iterator.
	 * <p>
	 * The aggregation function is called on both server and client (final reduce).  Therefore,
	 * the Lua script files must also reside on both server and client.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <udf dir>/<package name>.lua
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param packageName			server package where user defined function resides
	 * @param functionName			aggregation function name
	 * @param functionArgs			arguments to pass to function name, if any
	 * @return						result iterator
	 * @throws AerospikeException	if query fails
	 */
	public ResultSet queryAggregate(
		QueryPolicy policy,
		Statement statement,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException;

	/**
	 * Execute query, apply statement's aggregation function, and return result iterator.
	 * The aggregation function should be initialized via the statement's setAggregateFunction()
	 * and should be located in a resource or a filesystem file.
	 * <p>
	 * The query executor puts results on a queue in separate threads.  The calling thread
	 * concurrently pops results off the queue through the ResultSet iterator.
	 * The aggregation function is called on both server and client (final reduce).
	 * Therefore, the Lua script file must also reside on both server and client.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @throws AerospikeException	if query fails
	 */
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException;

	/**
	 * Create scalar secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	public IndexTask createIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName, 
		String binName,
		IndexType indexType
	) throws AerospikeException;

	/**
	 * Create complex secondary index to be used on bins containing collections.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * <p>
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @throws AerospikeException	if index create fails
	 */
	public IndexTask createIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName, 
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType
	) throws AerospikeException;

	/**
	 * Delete secondary index.
	 * This method is only supported by Aerospike 3 servers.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	public void dropIndex(
		Policy policy, 
		String namespace, 
		String setName, 
		String indexName
	) throws AerospikeException;
	
	//-------------------------------------------------------
	// User administration
	//-------------------------------------------------------

	/**
	 * Create user with password and roles.  Clear-text password will be hashed using bcrypt
	 * before sending to server.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				user password in clear-text format
	 * @param roles					variable arguments array of role names.  Valid roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException;
	
	/**
	 * Remove user from cluster.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @throws AerospikeException	if command fails
	 */
	public void dropUser(AdminPolicy policy, String user) throws AerospikeException;

	/**
	 * Change user's password.  Clear-text password will be hashed using bcrypt before sending to server.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param password				user password in clear-text format
	 * @throws AerospikeException	if command fails
	 */
	public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException;

	/**
	 * Add roles to user's list of roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names.  Valid roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException;

	/**
	 * Remove roles from user's list of roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name
	 * @param roles					role names.  Valid roles are listed in Role.cs
	 * @throws AerospikeException	if command fails
	 */
	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException;

	/**
	 * Create user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException;

	/**
	 * Drop user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @throws AerospikeException	if command fails
	 */
	public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException;

	/**
	 * Grant privileges to an user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException;

	/**
	 * Revoke privileges from an user defined role.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name
	 * @param privileges			privileges assigned to the role.
	 * @throws AerospikeException	if command fails
	 */
	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException;

	/**
	 * Retrieve roles for a given user.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param user					user name filter
	 * @throws AerospikeException	if command fails
	 */
	public User queryUser(AdminPolicy policy, String user) throws AerospikeException;

	/**
	 * Retrieve all users and their roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public List<User> queryUsers(AdminPolicy policy) throws AerospikeException;

	/**
	 * Retrieve role definition.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @param roleName				role name filter
	 * @throws AerospikeException	if command fails
	 */
	public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException;

	/**
	 * Retrieve all roles.
	 * 
	 * @param policy				admin configuration parameters, pass in null for defaults
	 * @throws AerospikeException	if command fails
	 */
	public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException;
}
