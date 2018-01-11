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

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
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
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Util;

/**
 * THIS CLASS IS OBSOLETE.
 * <p>
 * The new efficient asynchronous API is located directly in AerospikeClient.
 * This class is just a thin wrapper over the new AerospikeClient asynchronous API.
 * This class exists solely to provide compatibility with legacy applications.
 * <p>
 * Asynchronous Aerospike client.
 * <p>
 * Your application uses this class to perform asynchronous database operations 
 * such as writing and reading records, and selecting sets of records. Write 
 * operations include specialized functionality such as append/prepend and arithmetic
 * addition.
 * <p>
 * This client is thread-safe. One client instance should be used per cluster.
 * Multiple threads should share this cluster instance.
 * <p>
 * Each record may have multiple bins, unless the Aerospike server nodes are
 * configured as "single-bin". In "multi-bin" mode, partial records may be
 * written or read by specifying the relevant subset of bins.
 */
public class AsyncClient extends AerospikeClient implements IAsyncClient, Closeable {	
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------
	
	/**
	 * Default read policy that is used when asynchronous read command policy is null.
	 */
	public final Policy asyncReadPolicyDefault;
	
	/**
	 * Default write policy that is used when asynchronous write command policy is null.
	 */
	public final WritePolicy asyncWritePolicyDefault;
	
	/**
	 * Default scan policy that is used when asynchronous scan command policy is null.
	 */
	public final ScanPolicy asyncScanPolicyDefault;

	/**
	 * Default query policy that is used when asynchronous query command policy is null.
	 */
	public final QueryPolicy asyncQueryPolicyDefault;
	
	/**
	 * Default batch policy that is used when asynchronous batch command policy is null.
	 */
	public final BatchPolicy asyncBatchPolicyDefault;

	private final NioEventLoops eventLoops;
	private final ExecutorService taskThreadPool;	
	private final Throttle throttle;
	private final int maxCommandsPerEventLoop;
	private final boolean useListener;

	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	/**
	 * Initialize asynchronous client.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public AsyncClient(String hostname, int port) throws AerospikeException {
		this(new AsyncClientPolicy(), new Host(hostname, port));
	}

	/**
	 * Initialize asynchronous client.
	 * The client policy is used to set defaults and size internal data structures.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails and the policy's failIfNotConnected is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public AsyncClient(AsyncClientPolicy policy, String hostname, int port) throws AerospikeException {
		this(policy, new Host(hostname, port));	
	}

	/**
	 * Initialize asynchronous client with suitable hosts to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * For each host connection that succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * In most cases, only one host is necessary to seed the cluster. The remaining hosts 
	 * are added as future seeds in case of a complete network failure.
	 * <p>
	 * If one connection succeeds, the client is ready to process database requests.
	 * If all connections fail and the policy's failIfNotConnected is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public AsyncClient(AsyncClientPolicy policy, Host... hosts) throws AerospikeException {
		super(policy);
		
		if (policy == null) {
			policy = new AsyncClientPolicy();
		}

		this.asyncReadPolicyDefault = policy.asyncReadPolicyDefault;
		this.asyncWritePolicyDefault = policy.asyncWritePolicyDefault;
		this.asyncScanPolicyDefault = policy.asyncScanPolicyDefault;
		this.asyncQueryPolicyDefault = policy.asyncQueryPolicyDefault;
		this.asyncBatchPolicyDefault = policy.asyncBatchPolicyDefault;

		EventPolicy eventPolicy = new EventPolicy();
		
		if (policy.asyncSelectorTimeout > 0) {
			eventPolicy.minTimeout = policy.asyncSelectorTimeout;			
		}

		eventLoops = new NioEventLoops(eventPolicy, policy.asyncSelectorThreads);
		policy.eventLoops = eventLoops;

		try {
			// BLOCK mode or a task thread pool requires extra listener code that sits on top
			// of the new efficient async API.  Otherwise, the new efficient async API is used
			// directly.  All modes are still limited by ClientPolicy.maxConnsPerNode.
			useListener = (policy.asyncMaxCommandAction == MaxCommandAction.BLOCK || policy.asyncTaskThreadPool != null);
			taskThreadPool = policy.asyncTaskThreadPool;					
			
			if (policy.asyncMaxCommandAction == MaxCommandAction.BLOCK) {
				throttle = new Throttle(policy.asyncMaxCommands);
				maxCommandsPerEventLoop = policy.asyncMaxCommands / eventLoops.getSize();				
			}
			else {
				throttle = null;
				maxCommandsPerEventLoop = 0;
			}

			super.cluster = new Cluster(policy, hosts);
		}
		catch (Exception e) {
			eventLoops.close();
			throw e;
		}
	}

	//-------------------------------------------------------
	// Destructor
	//-------------------------------------------------------

	/**
	 * Close all client connections to database server nodes.
	 */
	@Override
	public final void close() {
		super.close();
		eventLoops.close();
	}

	//-------------------------------------------------------
	// Default Policies
	//-------------------------------------------------------

	public final Policy getAsyncReadPolicyDefault() {
		return asyncReadPolicyDefault;
	}

	public final WritePolicy getAsyncWritePolicyDefault() {
		return asyncWritePolicyDefault;
	}

	public final ScanPolicy getAsyncScanPolicyDefault() {
		return asyncScanPolicyDefault;
	}

	public final QueryPolicy getAsyncQueryPolicyDefault() {
		return asyncQueryPolicyDefault;
	}

	public final BatchPolicy getAsyncBatchPolicyDefault() {
		return asyncBatchPolicyDefault;
	}

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
	public final void put(final WritePolicy policy, final WriteListener listener, final Key key, final Bin... bins) throws AerospikeException {	
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					put(findEventLoop(), new AWriteListener(listener), wp, key, bins);
				}
			});
		}
		else {
			put(findEventLoop(), listener, wp, key, bins);
		}
	}

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
	public final void append(final WritePolicy policy, final WriteListener listener, final Key key, final Bin... bins) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					append(findEventLoop(), new AWriteListener(listener), wp, key, bins);
				}
			});
		}
		else {
			append(findEventLoop(), listener, wp, key, bins);
		}		
	}
	
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
	public final void prepend(final WritePolicy policy, final WriteListener listener, final Key key, final Bin... bins) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					prepend(findEventLoop(), new AWriteListener(listener), wp, key, bins);
				}
			});
		}
		else {
			prepend(findEventLoop(), listener, wp, key, bins);
		}		
	}

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
	public final void add(final WritePolicy policy, final WriteListener listener, final Key key, final Bin... bins) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					add(findEventLoop(), new AWriteListener(listener), wp, key, bins);
				}
			});
		}
		else {
			add(findEventLoop(), listener, wp, key, bins);
		}		
	}

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
	public final void delete(final WritePolicy policy, final DeleteListener listener, final Key key) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					delete(findEventLoop(), new ADeleteListener(listener), wp, key);
				}
			});
		}
		else {
			delete(findEventLoop(), listener, wp, key);
		}		
	}

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
	public final void touch(final WritePolicy policy, final WriteListener listener, final Key key) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					touch(findEventLoop(), new AWriteListener(listener), wp, key);
				}
			});
		}
		else {
			touch(findEventLoop(), listener, wp, key);
		}
	}

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
	public final void exists(final Policy policy, final ExistsListener listener, final Key key) throws AerospikeException {
		final Policy p = (policy != null)? policy : asyncReadPolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					exists(findEventLoop(), new AExistsListener(listener), p, key);
				}
			});
		}
		else {
			exists(findEventLoop(), listener, p, key);
		}
	}

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
	public final void exists(final BatchPolicy policy, final ExistsArrayListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					exists(findEventLoop(), new AExistsArrayListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			exists(findEventLoop(), listener, bp, keys);
		}
	}

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
	public final void exists(final BatchPolicy policy, final ExistsSequenceListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					exists(findEventLoop(), new AExistsSequenceListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			exists(findEventLoop(), listener, bp, keys);
		}
	}

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
	public final void get(final Policy policy, final RecordListener listener, final Key key) throws AerospikeException {
		final Policy p = (policy != null)? policy : asyncReadPolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordListener(listener), p, key);
				}
			});
		}
		else {
			get(findEventLoop(), listener, p, key);
		}
	}
	
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
	public final void get(final Policy policy, final RecordListener listener, final Key key, final String... binNames) throws AerospikeException {
		final Policy p = (policy != null)? policy : asyncReadPolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordListener(listener), p, key, binNames);
				}
			});
		}
		else {
			get(findEventLoop(), listener, p, key, binNames);
		}
	}

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
	public final void getHeader(final Policy policy, final RecordListener listener, final Key key) throws AerospikeException {
		final Policy p = (policy != null)? policy : asyncReadPolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					getHeader(findEventLoop(), new ARecordListener(listener), p, key);
				}
			});
		}
		else {
			getHeader(findEventLoop(), listener, p, key);
		}
	}

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
	public final void get(final BatchPolicy policy, final BatchListListener listener, final List<BatchRead> records) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ABatchListListener(listener, commands), bp, records);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, records);
		}
	}

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
	public final void get(final BatchPolicy policy, final BatchSequenceListener listener, final List<BatchRead> records) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ABatchSequenceListener(listener, commands), bp, records);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, records);
		}
	}

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
	public final void get(final BatchPolicy policy, final RecordArrayListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordArrayListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, keys);
		}
	}
	
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
	public final void get(final BatchPolicy policy, final RecordSequenceListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordSequenceListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, keys);
		}
	}

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
	public final void get(final BatchPolicy policy, final RecordArrayListener listener, final Key[] keys, final String... binNames) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordArrayListener(listener, commands), bp, keys, binNames);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, keys, binNames);
		}
	}
	
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
	public final void get(final BatchPolicy policy, final RecordSequenceListener listener, final Key[] keys, final String... binNames) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					get(findEventLoop(), new ARecordSequenceListener(listener, commands), bp, keys, binNames);
				}
			});
		}
		else {
			get(findEventLoop(), listener, bp, keys, binNames);
		}
	}

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
	public final void getHeader(final BatchPolicy policy, final RecordArrayListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					getHeader(findEventLoop(), new ARecordArrayListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			getHeader(findEventLoop(), listener, bp, keys);
		}
	}

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
	public final void getHeader(final BatchPolicy policy, final RecordSequenceListener listener, final Key[] keys) throws AerospikeException {
		final BatchPolicy bp = (policy != null)? policy : asyncBatchPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					getHeader(findEventLoop(), new ARecordSequenceListener(listener, commands), bp, keys);
				}
			});
		}
		else {
			getHeader(findEventLoop(), listener, bp, keys);
		}
	}

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
	 * <p>
	 * Both scalar bin operations (Operation) and list bin operations (ListOperation)
	 * can be performed in same call.
	 * 
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if queue is full
	 */
	public final void operate(final WritePolicy policy, final RecordListener listener, final Key key, final Operation... operations) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					operate(findEventLoop(), new ARecordListener(listener), wp, key, operations);
				}
			});
		}
		else {
			operate(findEventLoop(), listener, wp, key, operations);
		}		
	}

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
	 * @param listener				where to send results
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if queue is full
	 */
	public final void scanAll(final ScanPolicy policy, final RecordSequenceListener listener, final String namespace, final String setName, final String... binNames) throws AerospikeException {
		final ScanPolicy sp = (policy != null)? policy : asyncScanPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					scanAll(findEventLoop(), new ARecordSequenceListener(listener, commands), sp, namespace, setName, binNames);
				}
			});
		}
		else {
			scanAll(findEventLoop(), listener, sp, namespace, setName, binNames);
		}
	}
	
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
	 * @param listener				where to send results, pass in null for fire and forget
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if transaction fails
	 */
	public final void execute(
		final WritePolicy policy,
		final ExecuteListener listener,
		final Key key,
		final String packageName,
		final String functionName,
		final Value... functionArgs
	) throws AerospikeException {
		final WritePolicy wp = (policy != null)? policy : asyncWritePolicyDefault;

		if (useListener) {
			commandBegin(1, new Runnable() {
				public void run() {
					execute(findEventLoop(), new AExecuteListener(listener), wp, key, packageName, functionName, functionArgs);
				}
			});
		}
		else {
			execute(findEventLoop(), listener, wp, key, packageName, functionName, functionArgs);
		}		
	}

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
	public final void query(final QueryPolicy policy, final RecordSequenceListener listener, final Statement statement) throws AerospikeException {
		final QueryPolicy qp = (policy != null)? policy : asyncQueryPolicyDefault;

		if (useListener) {
			// Reserve one command for each node.
			final int commands = cluster.getNodes().length;

			commandBegin(commands, new Runnable() {
				public void run() {
					query(findEventLoop(), new ARecordSequenceListener(listener, commands), qp, statement);
				}
			});
		}
		else {
			query(findEventLoop(), listener, qp, statement);
		}
	}
	
	//-------------------------------------------------------
	// Private
	//-------------------------------------------------------

	private final class AWriteListener implements WriteListener {	
		private final WriteListener listener;
		
		private AWriteListener(WriteListener listener) {
			this.listener = listener;
		}
		
		@Override
		public void onSuccess(final Key key) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onSuccess(key);
					}
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onFailure(ae);
					}
				}
			});
		}	
	}

	private final class ADeleteListener implements DeleteListener {	
		private final DeleteListener listener;
		
		private ADeleteListener(DeleteListener listener) {
			this.listener = listener;
		}
		
		@Override
		public void onSuccess(final Key key, final boolean existed) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onSuccess(key, existed);
					}
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onFailure(ae);
					}
				}
			});
		}	
	}

	private final class AExistsListener implements ExistsListener {	
		private final ExistsListener listener;
		
		private AExistsListener(ExistsListener listener) {
			this.listener = listener;
		}
		
		@Override
		public void onSuccess(final Key key, final boolean existed) {
			commandComplete(1, new Runnable() {				
				public void run() {
					listener.onSuccess(key, existed);
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(1, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}	
	}
	
	private final class AExistsArrayListener implements ExistsArrayListener {	
		private final ExistsArrayListener listener;
		private final int commands;
		
		private AExistsArrayListener(ExistsArrayListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}
		
		@Override
		public void onSuccess(final Key[] keys, final boolean[] exists) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess(keys, exists);
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}
	
	private final class AExistsSequenceListener implements ExistsSequenceListener {	
		private final ExistsSequenceListener listener;
		private final int commands;
		
		private AExistsSequenceListener(ExistsSequenceListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}
		
		@Override
		public void onExists(final Key key, final boolean exists) {
			callListener(new Runnable() {				
				public void run() {
					listener.onExists(key, exists);			
				}
			});
		}

		@Override
		public void onSuccess() {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess();
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}

	private final class ARecordListener implements RecordListener {	
		private final RecordListener listener;
		
		private ARecordListener(RecordListener listener) {
			this.listener = listener;
		}
		
		@Override
		public void onSuccess(final Key key, final Record record) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onSuccess(key, record);
					}
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onFailure(ae);
					}
				}
			});
		}	
	}
		
	private final class ABatchListListener implements BatchListListener {	
		private final BatchListListener listener;
		private final int commands;
		
		private ABatchListListener(BatchListListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}

		@Override
		public void onSuccess(final List<BatchRead> records) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess(records);
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}
	
	private final class ABatchSequenceListener implements BatchSequenceListener {	
		private final BatchSequenceListener listener;
		private final int commands;
		
		private ABatchSequenceListener(BatchSequenceListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}

		@Override
		public void onRecord(final BatchRead record) {
			callListener(new Runnable() {				
				public void run() {
					listener.onRecord(record);
				}
			});
		}

		@Override
		public void onSuccess() {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess();
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}

	private final class ARecordArrayListener implements RecordArrayListener {	
		private final RecordArrayListener listener;
		private final int commands;
		
		private ARecordArrayListener(RecordArrayListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}

		@Override
		public void onSuccess(final Key[] keys, final Record[] records) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess(keys, records);
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}
	
	private final class ARecordSequenceListener implements RecordSequenceListener {	
		private final RecordSequenceListener listener;
		private final int commands;
		
		private ARecordSequenceListener(RecordSequenceListener listener, int commands) {
			this.listener = listener;
			this.commands = commands;
		}

		@Override
		public void onRecord(final Key key, final Record record) {
			callListener(new Runnable() {				
				public void run() {
					listener.onRecord(key, record);
				}
			});
		}

		@Override
		public void onSuccess() {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onSuccess();
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(commands, new Runnable() {				
				public void run() {
					listener.onFailure(ae);
				}
			});
		}
	}

	private final class AExecuteListener implements ExecuteListener {	
		private final ExecuteListener listener;
		
		private AExecuteListener(ExecuteListener listener) {
			this.listener = listener;
		}
		
		@Override
		public void onSuccess(final Key key, final Object obj) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onSuccess(key, obj);
					}
				}
			});
		}
		
		@Override
		public void onFailure(final AerospikeException ae) {
			commandComplete(1, new Runnable() {				
				public void run() {
					if (listener != null) {
						listener.onFailure(ae);
					}
				}
			});
		}
	}
	
	private final EventLoop findEventLoop() {
		// Find event loop that is not running at capacity.
		// maxCommandsPerEventLoop is a soft limit.
		// That hard limit is the blocking throttle.
		// Try robin-robin first.
		NioEventLoop eventLoop = eventLoops.next();

		// asyncPending is not atomic, so it's value is only approximate.
		if (cluster.eventState[eventLoop.index].pending < maxCommandsPerEventLoop) {
			return eventLoop;
		}

		// Try backwards.
		for (int i = eventLoop.index - 1; i >= 0; i--) {
			if (cluster.eventState[i].pending < maxCommandsPerEventLoop) {
				return eventLoops.eventLoops[i];
			}
		}

		// Try forwards.
		for (int i = eventLoop.index + 1; i < eventLoops.eventLoops.length; i++) {
			if (cluster.eventState[i].pending < maxCommandsPerEventLoop) {
				return eventLoops.eventLoops[i];
			}
		}

		// Nothing worked. Use original event loop.
		return eventLoop;
	}

	private final void commandBegin(int commands, final Runnable runnable) {
		if (throttle != null) {	
			if (throttle.waitForSlot(commands)) {
				try {
					runnable.run();
				}
				catch (Exception e) {
					throttle.addSlot(commands);
					throw e;
				}
			}
		}
		else {
			runnable.run();
		}
	}
	
	private final void commandComplete(int commands, final Runnable runnable) {
		if (throttle != null) {
			throttle.addSlot(commands);
		}
		callListener(runnable);
	}
	
	private final void callListener(final Runnable runnable) {
		if (taskThreadPool != null) {
			taskThreadPool.submit(new Runnable() {
				public void run() {
					try {
						runnable.run();
					}
					catch (Exception e) {
						Log.error("Listener error: " + Util.getErrorMessage(e));
					}
				}
	     	});
		}
		else {
			runnable.run();
		}
	}
}
