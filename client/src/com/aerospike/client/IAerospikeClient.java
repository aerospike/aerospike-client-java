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
package com.aerospike.client;

import java.io.Closeable;
import java.util.Calendar;
import java.util.List;

import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
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
public interface IAerospikeClient extends Closeable {
	//-------------------------------------------------------
	// Default Policies
	//-------------------------------------------------------
	
	public Policy getReadPolicyDefault();

	public WritePolicy getWritePolicyDefault();

	public ScanPolicy getScanPolicyDefault();

	public QueryPolicy getQueryPolicyDefault();

	public BatchPolicy getBatchPolicyDefault();

	public InfoPolicy getInfoPolicyDefault();
	
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

	/**
	 * Asynchronously write record bin(s). 
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException;
	
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
	 * Asynchronously append bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values. 
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

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

	/**
	 * Asynchronously prepend bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values. 
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

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

	/**
	 * Asynchronously add integer bin values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values. 
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

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

	/**
	 * Asynchronously delete record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) throws AerospikeException;

	/**
	 * Remove records in specified namespace/set efficiently.  This method is many orders of magnitude 
	 * faster than deleting records one at a time.
	 * <p>
	 * See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
	 * <p>
	 * This asynchronous server call may return before the truncation is complete.  The user can still
	 * write new records after the server call returns because new records will have last update times
	 * greater than the truncate cutoff (set at the time of truncate call).
	 * 
	 * @param policy				info command configuration parameters, pass in null for defaults
	 * @param ns					required namespace
	 * @param set					optional set name.  Pass in null to delete all sets in namespace.
	 * @param beforeLastUpdate		optional delete records before record last update time.
	 * 								If specified, value must be before the current time.
	 * 								Pass in null to delete all records in namespace/set.
	 * @throws AerospikeException	if truncate fails
	 */
	public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException;

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

	/**
	 * Asynchronously reset record's time to expiration using the policy's expiration.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Fail if the record does not exist.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException;

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
	 * Asynchronously determine if a record key exists.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException;

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

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Asynchronously check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each key's result is returned in separate onExists() calls.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

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
	 * Asynchronously read entire record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException;

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
	 * Asynchronously read record header and bins for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) throws AerospikeException;

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

	/**
	 * Asynchronously read record generation and expiration only for specified key.  Bins are not read.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException;

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	/**
	 * Read multiple records for specified batch keys in one batch call.
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts and maximum concurrent threads.
	 * This method requires Aerospike Server version >= 3.6.0.
	 * 
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if read fails
	 */
	public void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException;

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
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

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
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException;

	/**
	 * Asynchronously read multiple record headers and bins for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @param binNames				array of bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException;

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

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Asynchronously read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException;

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

	/**
	 * Asynchronously perform multiple read/write operations on a single key in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * An example would be to add an integer value to an existing record and then
	 * read the result, all in one database call.
	 * <p>
	 * Write operations are always performed first, regardless of operation order 
	 * relative to read operations.
	 * <p>
	 * Both scalar bin operations (Operation) and list bin operations (ListOperation)
	 * can be performed in same call.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) throws AerospikeException;

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
	 * Asynchronously read all records in specified namespace and set.  If the policy's
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException;

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

	//---------------------------------------------------------------
	// User defined functions
	//---------------------------------------------------------------
	
	/**
	 * Register package located in a file containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
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
	 * Register UDF functions located in a code string with server.  Example:
	 * <pre>
	 * {@code
	 * String code = 
	 *   "local function reducer(val1,val2)\n" + 
	 *   "  return val1 + val2\n" + 
	 *   "end\n" +
	 *   "\n" +
	 *   "function sum_single_bin(stream,name)\n" +
	 *   "  local function mapper(rec)\n" +
	 *   "    return rec[name]\n" +
	 *   "  end\n" +
	 *   "  return stream : map(mapper) : reduce(reducer)\n" +
	 *   "end\n";
	 *   
	 * client.registerUdfString(null, code, "mysum.lua", Language.LUA);
	 * }
	 * </pre>
	 * <p>
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param code					code string containing user defined functions.
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @param language				language of user defined functions
	 * @throws AerospikeException	if register fails
	 */
	public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) 
		throws AerospikeException;

	/**
	 * Remove user defined function from server nodes.
	 * 
	 * @param policy				info configuration parameters, pass in null for defaults
	 * @param serverPath			location of UDF on server nodes.  Example: mylua.lua
	 * @throws AerospikeException	if remove fails
	 */
	public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException;
	
	/**
	 * Execute user defined function on server and return results.
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
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

	/**
	 * Asynchronously execute user defined function on server.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void execute(
		EventLoop eventLoop,
		ExecuteListener listener,
		WritePolicy policy,
		Key key,
		String packageName,
		String functionName,
		Value... functionArgs
	) throws AerospikeException;

	//----------------------------------------------------------
	// Query/Execute UDF
	//----------------------------------------------------------

	/**
	 * Apply user defined function on records that match the statement filter.
	 * Records are not returned to the client.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * ExecuteTask instance.
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
	// Query functions
	//--------------------------------------------------------

	/**
	 * Execute query on all server nodes and return record iterator.  The query executor puts 
	 * records on a queue in separate threads.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @return						record iterator
	 * @throws AerospikeException	if query fails
	 */
	public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException;
	
	/**
	 * Asynchronously execute query on all server nodes.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * 
	 * @param eventLoop				event loop that will process the command
	 * @param listener				where to send results
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @throws AerospikeException	if event loop registration fails
	 */
	public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) throws AerospikeException;

	/**
	 * Execute query on a single server node and return record iterator.  The query executor puts
	 * records on a queue in a separate thread.  The calling thread concurrently pops records off 
	 * the queue through the record iterator.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param node					server node to execute query
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
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @throws AerospikeException	if query fails
	 */
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException;

	/**
	 * Execute query on a single server node, apply statement's aggregation function, and return 
	 * result iterator.
	 * The aggregation function should be initialized via the statement's setAggregateFunction()
	 * and should be located in a resource or a filesystem file.
	 * <p>
	 * The query executor puts results on a queue in separate threads.  The calling thread
	 * concurrently pops results off the queue through the ResultSet iterator.
	 * The aggregation function is called on both server and client (final reduce).
	 * Therefore, the Lua script file must also reside on both server and client.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @param node					server node to execute query
	 * @throws AerospikeException	if query fails
	 */
	public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException;

	/**
	 * Create scalar secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
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
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * 
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	public IndexTask dropIndex(
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
	 * Change user's password.
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
