/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.async;

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Statement;

/**
 * THIS CLASS IS OBSOLETE.
 * <p>
 * The new efficient asynchronous API is located directly in IAerospikeClient.
 * This class exists solely to provide compatibility with legacy applications.
 * <p>
 * This interface's sole purpose is to allow mock frameworks to operate on
 * AsyncClient without being constrained by final methods.
 */
public interface IAsyncClient extends IAerospikeClient {	
	//-------------------------------------------------------
	// Default Policies
	//-------------------------------------------------------

	public Policy getAsyncReadPolicyDefault();

	public WritePolicy getAsyncWritePolicyDefault();

	public ScanPolicy getAsyncScanPolicyDefault();

	public QueryPolicy getAsyncQueryPolicyDefault();

	public BatchPolicy getAsyncBatchPolicyDefault();

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously write record bin(s). 
	 * This method schedules the put command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if queue is full
	 */
	public void put(WritePolicy policy, WriteListener listener, Key key, Bin... bins)
		throws AerospikeException;

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------
		
	/**
	 * Asynchronously append bin string values to existing record bin values.
	 * This method schedules the append command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if queue is full
	 */
	public void append(WritePolicy policy, WriteListener listener, Key key, Bin... bins)
		throws AerospikeException;
	
	/**
	 * Asynchronously prepend bin string values to existing record bin values.
	 * This method schedules the prepend command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call works only for string values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if queue is full
	 */
	public void prepend(WritePolicy policy, WriteListener listener, Key key, Bin... bins)
		throws AerospikeException;

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously add integer bin values to existing record bin values.
	 * This method schedules the add command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values. 
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if queue is full
	 */
	public void add(WritePolicy policy, WriteListener listener, Key key, Bin... bins)
		throws AerospikeException;

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously delete record for specified key.
	 * This method schedules the delete command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout.
	 * 
	 * @param policy				delete configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @throws AerospikeException	if queue is full
	 */
	public void delete(WritePolicy policy, DeleteListener listener, Key key)
		throws AerospikeException;

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	/**
	 * Asynchronously create record if it does not already exist.  If the record exists, the record's 
	 * time to expiration will be reset to the policy's expiration.
	 * <p>
	 * This method schedules the touch command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @throws AerospikeException	if queue is full
	 */
	public void touch(WritePolicy policy, WriteListener listener, Key key)
		throws AerospikeException;

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously determine if a record key exists.
	 * This method schedules the exists command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param key					unique record identifier
	 * @throws AerospikeException	if queue is full
	 */
	public void exists(Policy policy, ExistsListener listener, Key key)
		throws AerospikeException;

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method schedules the exists command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * <p>
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 *  
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void exists(BatchPolicy policy, ExistsArrayListener listener, Key[] keys)
		throws AerospikeException;

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method schedules the exists command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in multiple unordered calls.
	 * <p>
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 *  
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void exists(BatchPolicy policy, ExistsSequenceListener listener, Key[] keys)
		throws AerospikeException;
	
	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously read entire record for specified key.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param key					unique record identifier
	 * @throws AerospikeException	if queue is full
	 */	
	public void get(Policy policy, RecordListener listener, Key key)
		throws AerospikeException;
	
	/**
	 * Asynchronously read record header and bins for specified key.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if queue is full
	 */
	public void get(Policy policy, RecordListener listener, Key key, String... binNames)
		throws AerospikeException;

	/**
	 * Asynchronously read record generation and expiration only for specified key.  Bins are not read.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param key					unique record identifier
	 * @throws AerospikeException	if queue is full
	 */
	public void getHeader(Policy policy, RecordListener listener, Key key)
		throws AerospikeException;

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * <p>
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * This method requires Aerospike Server version >= 3.6.0.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 * @throws AerospikeException	if read fails
	 */
	public void get(BatchPolicy policy, BatchListListener listener, List<BatchRead> records)
		throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * <p>
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * This method requires Aerospike Server version >= 3.6.0.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 * @throws AerospikeException	if read fails
	 */
	public void get(BatchPolicy policy, BatchSequenceListener listener, List<BatchRead> records)
		throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void get(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in multiple unordered calls.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		throws AerospikeException;

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if queue is full
	 */
	public void get(BatchPolicy policy, RecordArrayListener listener, Key[] keys, String... binNames) 
		throws AerospikeException;

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in multiple unordered calls.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if queue is full
	 */
	public void get(BatchPolicy policy, RecordSequenceListener listener, Key[] keys, String... binNames) 
		throws AerospikeException;

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in a single call.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void getHeader(BatchPolicy policy, RecordArrayListener listener, Key[] keys)
		throws AerospikeException;

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method schedules the get command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener in multiple unordered calls.
	 * <p>
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts and maximum parallel commands.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if queue is full
	 */
	public void getHeader(BatchPolicy policy, RecordSequenceListener listener, Key[] keys)
		throws AerospikeException;

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------
	
	/**
	 * Asynchronously perform multiple read/write operations on a single key in one batch call.
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * This method schedules the operate command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if queue is full
	 */
	public void operate(WritePolicy policy, RecordListener listener, Key key, Operation... operations)
		throws AerospikeException;

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------

	/**
	 * Asynchronously read all records in specified namespace and set.  If the policy's 
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method schedules the scan command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * 
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if queue is full
	 */
	public void scanAll(ScanPolicy policy, RecordSequenceListener listener, String namespace, String setName, String... binNames)
		throws AerospikeException;

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------

	/**
	 * Asynchronously execute user defined function on server and return results.
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location on the server:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 * <p>
	 * This method schedules the execute command with a channel selector and returns.
	 * Another thread will process the command and send the results to the listener.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public void execute(
		WritePolicy policy,
		ExecuteListener listener,
		Key key,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException;

	//-------------------------------------------------------
	// Query Operations
	//-------------------------------------------------------

	/**
	 * Asynchronously execute query on all server nodes.  The query policy's 
	 * <code>maxConcurrentNodes</code> dictate how many nodes can be queried in parallel.
	 * The default is to query all nodes in parallel.
	 * <p>
	 * This method schedules the node's query commands with channel selectors and returns.
	 * Selector threads will process the commands and send the results to the listener.
	 * 
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param listener				where to send results
	 * @param statement				database query command parameters
	 * @throws AerospikeException	if query fails
	 */
	public void query(QueryPolicy policy, RecordSequenceListener listener, Statement statement) 
		throws AerospikeException;
}
