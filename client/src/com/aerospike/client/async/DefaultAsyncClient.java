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
package com.aerospike.client.async;

import java.util.Arrays;
import java.util.HashSet;

import com.aerospike.client.*;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
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
public class DefaultAsyncClient extends DefaultAerospikeClient implements AsyncClient {
	//-------------------------------------------------------
	// Member variables.
	//-------------------------------------------------------
	
	private final AsyncCluster cluster;
	
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
	public DefaultAsyncClient(String hostname, int port) throws AerospikeException {
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
	 * If the connection fails and the policy's failOnInvalidHosts is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public DefaultAsyncClient(AsyncClientPolicy policy, String hostname, int port) throws AerospikeException {
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
	public DefaultAsyncClient(AsyncClientPolicy policy, Host... hosts) throws AerospikeException {
		if (policy == null) {
			policy = new AsyncClientPolicy();
		}
		this.cluster = new AsyncCluster(policy, hosts);
		super.cluster = this.cluster;
		
		if (policy.failIfNotConnected && ! this.cluster.isConnected()) {
			throw new AerospikeException.Connection("Failed to connect to host(s): " + Arrays.toString(hosts));
		}
	}

	//-------------------------------------------------------
	// Write Record Operations
	//-------------------------------------------------------
	
	@Override
    public final void put(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException {
		AsyncWrite command = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.WRITE);
		command.execute();
	}

	//-------------------------------------------------------
	// String Operations
	//-------------------------------------------------------
		
	@Override
    public final void append(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException {
		AsyncWrite command = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.APPEND);
		command.execute();
	}
	
	@Override
    public final void prepend(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException {
		AsyncWrite command = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.PREPEND);
		command.execute();
	}

	//-------------------------------------------------------
	// Arithmetic Operations
	//-------------------------------------------------------
	
	@Override
    public final void add(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException {
		AsyncWrite command = new AsyncWrite(cluster, policy, listener, key, bins, Operation.Type.ADD);
		command.execute();
	}

	//-------------------------------------------------------
	// Delete Operations
	//-------------------------------------------------------
	
	@Override
    public final void delete(WritePolicy policy, DeleteListener listener, Key key) throws AerospikeException {
		AsyncDelete command = new AsyncDelete(cluster, policy, listener, key);
		command.execute();
	}

	//-------------------------------------------------------
	// Touch Operations
	//-------------------------------------------------------

	@Override
    public final void touch(WritePolicy policy, WriteListener listener, Key key) throws AerospikeException {
		AsyncTouch command = new AsyncTouch(cluster, policy, listener, key);
		command.execute();
	}

	//-------------------------------------------------------
	// Existence-Check Operations
	//-------------------------------------------------------
	
	@Override
    public final void exists(Policy policy, ExistsListener listener, Key key) throws AerospikeException {
		AsyncExists command = new AsyncExists(cluster, policy, listener, key);
		command.execute();
	}

	@Override
    public final void exists(Policy policy, ExistsArrayListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchExistsArrayExecutor(cluster, policy, keys, listener);		
	}

	@Override
    public final void exists(Policy policy, ExistsSequenceListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchExistsSequenceExecutor(cluster, policy, keys, listener);		
	}

	//-------------------------------------------------------
	// Read Record Operations
	//-------------------------------------------------------
	
	@Override
    public final void get(Policy policy, RecordListener listener, Key key) throws AerospikeException {
		AsyncRead command = new AsyncRead(cluster, policy, listener, key, null);
		command.execute();
	}
	
	@Override
    public final void get(Policy policy, RecordListener listener, Key key, String... binNames) throws AerospikeException {
		AsyncRead command = new AsyncRead(cluster, policy, listener, key, binNames);
		command.execute();
	}

	@Override
    public final void getHeader(Policy policy, RecordListener listener, Key key) throws AerospikeException {
		AsyncReadHeader command = new AsyncReadHeader(cluster, policy, listener, key);
		command.execute();
	}

	//-------------------------------------------------------
	// Batch Read Operations
	//-------------------------------------------------------

	@Override
    public final void get(Policy policy, RecordArrayListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);
	}

	@Override
    public final void get(Policy policy, RecordSequenceListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_GET_ALL);		
	}
	
	@Override
    public final void get(Policy policy, RecordArrayListener listener, Key[] keys, String... binNames)
		throws AerospikeException {
		HashSet<String> names = binNamesToHashSet(binNames);
		new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, names, Command.INFO1_READ);
	}

	@Override
    public final void get(Policy policy, RecordSequenceListener listener, Key[] keys, String... binNames)
		throws AerospikeException {
		HashSet<String> names = binNamesToHashSet(binNames);
		new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, names, Command.INFO1_READ);
	}
	
	@Override
    public final void getHeader(Policy policy, RecordArrayListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchGetArrayExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
	}

	@Override
    public final void getHeader(Policy policy, RecordSequenceListener listener, Key[] keys) throws AerospikeException {
		new AsyncBatchGetSequenceExecutor(cluster, policy, listener, keys, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
	}

	//-------------------------------------------------------
	// Generic Database Operations
	//-------------------------------------------------------
	
	@Override
    public final void operate(WritePolicy policy, RecordListener listener, Key key, Operation... operations)
		throws AerospikeException {		
		AsyncOperate command = new AsyncOperate(cluster, policy, listener, key, operations);
		command.execute();
	}

	//-------------------------------------------------------
	// Scan Operations
	//-------------------------------------------------------

	@Override
    public final void scanAll(ScanPolicy policy, RecordSequenceListener listener, String namespace, String setName, String... binNames)
		throws AerospikeException {
		if (policy == null) {
			policy = new ScanPolicy();
		}
		
		// Retry policy must be one-shot for scans.
		policy.maxRetries = 0;
		new AsyncScanExecutor(cluster, policy, listener, namespace, setName, binNames);
	}
}
