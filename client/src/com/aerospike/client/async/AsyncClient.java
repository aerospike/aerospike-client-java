package com.aerospike.client.async;

import com.aerospike.client.*;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

public interface AsyncClient extends AerospikeClient {
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
     * @throws com.aerospike.client.AerospikeException    if queue is full
     */
    void put(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void append(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void prepend(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void add(WritePolicy policy, WriteListener listener, Key key, Bin... bins) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void delete(WritePolicy policy, DeleteListener listener, Key key) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void touch(WritePolicy policy, WriteListener listener, Key key) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void exists(Policy policy, ExistsListener listener, Key key) throws AerospikeException;

    /**
     * Asynchronously check if multiple record keys exist in one batch call.
     * This method schedules the exists command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in a single call.
     * <p>
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void exists(Policy policy, ExistsArrayListener listener, Key[] keys) throws AerospikeException;

    /**
     * Asynchronously check if multiple record keys exist in one batch call.
     * This method schedules the exists command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in multiple unordered calls.
     * <p>
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void exists(Policy policy, ExistsSequenceListener listener, Key[] keys) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordListener listener, Key key) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordListener listener, Key key, String... binNames) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void getHeader(Policy policy, RecordListener listener, Key key) throws AerospikeException;

    /**
     * Asynchronously read multiple records for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in a single call.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordArrayListener listener, Key[] keys) throws AerospikeException;

    /**
     * Asynchronously read multiple records for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in multiple unordered calls.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordSequenceListener listener, Key[] keys) throws AerospikeException;

    /**
     * Asynchronously read multiple record headers and bins for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in a single call.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @param binNames				array of bins to retrieve
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordArrayListener listener, Key[] keys, String... binNames)
        throws AerospikeException;

    /**
     * Asynchronously read multiple record headers and bins for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in multiple unordered calls.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @param binNames				array of bins to retrieve
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void get(Policy policy, RecordSequenceListener listener, Key[] keys, String... binNames)
        throws AerospikeException;

    /**
     * Asynchronously read multiple record header data for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in a single call.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void getHeader(Policy policy, RecordArrayListener listener, Key[] keys) throws AerospikeException;

    /**
     * Asynchronously read multiple record header data for specified keys in one batch call.
     * This method schedules the get command with a channel selector and returns.
     * Another thread will process the command and send the results to the listener in multiple unordered calls.
     * <p>
     * If a key is not found, the record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy				generic configuration parameters, pass in null for defaults
     * @param listener				where to send results
     * @param keys					array of unique record identifiers
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void getHeader(Policy policy, RecordSequenceListener listener, Key[] keys) throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void operate(WritePolicy policy, RecordListener listener, Key key, Operation... operations)
        throws AerospikeException;

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
     * @throws com.aerospike.client.AerospikeException	if queue is full
     */
    void scanAll(ScanPolicy policy, RecordSequenceListener listener, String namespace, String setName, String... binNames)
        throws AerospikeException;
}
