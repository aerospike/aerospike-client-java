package com.aerospike.client;

import com.aerospike.client.cluster.Node;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

import java.util.List;

public interface AerospikeClient {
    /**
     * Close all client connections to database server nodes.
     */
    void close();

    /**
     * Determine if we are ready to talk to the database server cluster.
     *
     * @return	<code>true</code> if cluster is ready,
     * 			<code>false</code> if cluster is not ready
     */
    boolean isConnected();

    /**
     * Return array of active server nodes in the cluster.
     *
     * @return	array of active nodes
     */
    Node[] getNodes();

    /**
     * Return list of active server node names in the cluster.
     *
     * @return	list of active node names
     */
    List<String> getNodeNames();

    /**
     * Write record bin(s).
     * The policy specifies the transaction timeout, record expiration and how the transaction is
     * handled when the record already exists.
     *
     * @param policy				write configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param bins					array of bin name/value pairs
     * @throws com.aerospike.client.AerospikeException	if write fails
     */
    void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

    /**
     * Append bin string values to existing record bin values.
     * The policy specifies the transaction timeout, record expiration and how the transaction is
     * handled when the record already exists.
     * This call only works for string values.
     *
     * @param policy				write configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param bins					array of bin name/value pairs
     * @throws com.aerospike.client.AerospikeException	if append fails
     */
    void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

    /**
     * Prepend bin string values to existing record bin values.
     * The policy specifies the transaction timeout, record expiration and how the transaction is
     * handled when the record already exists.
     * This call works only for string values.
     *
     * @param policy				write configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param bins					array of bin name/value pairs
     * @throws com.aerospike.client.AerospikeException	if prepend fails
     */
    void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

    /**
     * Add integer bin values to existing record bin values.
     * The policy specifies the transaction timeout, record expiration and how the transaction is
     * handled when the record already exists.
     * This call only works for integer values.
     *
     * @param policy				write configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param bins					array of bin name/value pairs
     * @throws com.aerospike.client.AerospikeException	if add fails
     */
    void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

    /**
     * Delete record for specified key.
     * The policy specifies the transaction timeout.
     *
     * @param policy				delete configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @return						whether record existed on server before deletion
     * @throws com.aerospike.client.AerospikeException	if delete fails
     */
    boolean delete(WritePolicy policy, Key key) throws AerospikeException;

    /**
     * Create record if it does not already exist.  If the record exists, the record's
     * time to expiration will be reset to the policy's expiration.
     *
     * @param policy				write configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @throws com.aerospike.client.AerospikeException	if touch fails
     */
    void touch(WritePolicy policy, Key key) throws AerospikeException;

    /**
     * Determine if a record key exists.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @return						whether record exists or not
     * @throws com.aerospike.client.AerospikeException	if command fails
     */
    boolean exists(Policy policy, Key key) throws AerospikeException;

    /**
     * Check if multiple record keys exist in one batch call.
     * The returned boolean array is in positional order with the original key array order.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param keys					array of unique record identifiers
     * @return						array key/existence status pairs
     * @throws com.aerospike.client.AerospikeException	if command fails
     */
    boolean[] exists(Policy policy, Key[] keys) throws AerospikeException;

    /**
     * Read entire record for specified key.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @return						if found, return record instance.  If not found, return null.
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record get(Policy policy, Key key) throws AerospikeException;

    /**
     * Read record header and bins for specified key.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param binNames				bins to retrieve
     * @return						if found, return record instance.  If not found, return null.
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record get(Policy policy, Key key, String... binNames) throws AerospikeException;

    /**
     * Read record generation and expiration only for specified key.  Bins are not read.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @return						if found, return record instance.  If not found, return null.
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record getHeader(Policy policy, Key key) throws AerospikeException;

    /**
     * Read multiple records for specified keys in one batch call.
     * The returned records are in positional order with the original key array order.
     * If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param keys					array of unique record identifiers
     * @return						array of records
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record[] get(Policy policy, Key[] keys) throws AerospikeException;

    /**
     * Read multiple record headers and bins for specified keys in one batch call.
     * The returned records are in positional order with the original key array order.
     * If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param keys					array of unique record identifiers
     * @param binNames				array of bins to retrieve
     * @return						array of records
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record[] get(Policy policy, Key[] keys, String... binNames)
        throws AerospikeException;

    /**
     * Read multiple record header data for specified keys in one batch call.
     * The returned records are in positional order with the original key array order.
     * If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param keys					array of unique record identifiers
     * @return						array of records
     * @throws com.aerospike.client.AerospikeException	if read fails
     */
    Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if command fails
     */
    Record operate(WritePolicy policy, Key key, Operation... operations)
        throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if scan fails
     */
    void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
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
     * @throws com.aerospike.client.AerospikeException	if scan fails
     */
    void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames)
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
     * @throws com.aerospike.client.AerospikeException	if transaction fails
     */
    void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
        throws AerospikeException;

    /**
     * Initialize large list operator.  This operator can be used to create and manage a list
     * within a single bin.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param binName				bin name
     * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
     */
    LargeList getLargeList(Policy policy, Key key, String binName, String userModule);

    /**
     * Initialize large map operator.  This operator can be used to create and manage a map
     * within a single bin.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param binName				bin name
     * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
     */
    LargeMap getLargeMap(Policy policy, Key key, String binName, String userModule);

    /**
     * Initialize large set operator.  This operator can be used to create and manage a set
     * within a single bin.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param binName				bin name
     * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
     */
    LargeSet getLargeSet(Policy policy, Key key, String binName, String userModule);

    /**
     * Initialize large stack operator.  This operator can be used to create and manage a stack
     * within a single bin.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param binName				bin name
     * @param userModule			Lua function name that initializes list configuration parameters, pass null for default
     */
    LargeStack getLargeStack(Policy policy, Key key, String binName, String userModule);

    /**
     * Register package containing user defined functions with server.
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
     * @throws com.aerospike.client.AerospikeException	if register fails
     */
    RegisterTask register(Policy policy, String clientPath, String serverPath, Language language)
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
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param key					unique record identifier
     * @param packageName			server package name where user defined function resides
     * @param functionName			user defined function
     * @param args					arguments passed in to user defined function
     * @return						return value of user defined function
     * @throws com.aerospike.client.AerospikeException	if transaction fails
     */
    Object execute(Policy policy, Key key, String packageName, String functionName, Value... args)
        throws AerospikeException;

    /**
     * Apply user defined function on records that match the statement filter.
     * Records are not returned to the client.
     * This asynchronous server call will return before command is complete.
     * The user can optionally wait for command completion by using the returned
     * ExecuteTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				scan configuration parameters, pass in null for defaults
     * @param statement				record filter
     * @param packageName			server package where user defined function resides
     * @param functionName			function name
     * @param functionArgs			to pass to function name, if any
     * @throws com.aerospike.client.AerospikeException	if command fails
     */
    ExecuteTask execute(
            Policy policy,
            Statement statement,
            String packageName,
            String functionName,
            Value... functionArgs
    ) throws AerospikeException;

    /**
     * Execute query and return record iterator.  The query executor puts records on a queue in
     * separate threads.  The calling thread concurrently pops records off the queue through the
     * record iterator.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param statement				database query command
     * @return						record iterator
     * @throws com.aerospike.client.AerospikeException	if query fails
     */
    RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if query fails
     */
    ResultSet queryAggregate(
            QueryPolicy policy,
            Statement statement,
            String packageName,
            String functionName,
            Value... functionArgs
    ) throws AerospikeException;

    /**
     * Create secondary index.
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
     * @param indexType				type of secondary index
     * @throws com.aerospike.client.AerospikeException	if index create fails
     */
    IndexTask createIndex(
            Policy policy,
            String namespace,
            String setName,
            String indexName,
            String binName,
            IndexType indexType
    ) throws AerospikeException;

    /**
     * Delete secondary index.
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param namespace				namespace - equivalent to database name
     * @param setName				optional set name - equivalent to database table
     * @param indexName				name of secondary index
     * @throws com.aerospike.client.AerospikeException	if index create fails
     */
    void dropIndex(
            Policy policy,
            String namespace,
            String setName,
            String indexName
    ) throws AerospikeException;
}
