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
package com.aerospike.client.reactor;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.dto.KeyExists;
import com.aerospike.client.reactor.dto.KeyObject;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.List;
import java.util.Map;


public interface IAerospikeReactorClient extends Closeable{

	Policy getReadPolicyDefault();

	WritePolicy getWritePolicyDefault();

	ScanPolicy getScanPolicyDefault();

	QueryPolicy getQueryPolicyDefault();

	BatchPolicy getBatchPolicyDefault();

	InfoPolicy getInfoPolicyDefault();

	/**
	 * Reactively read entire record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> get(Key key) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysRecords> get(Key[] keys) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysRecords> get(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Reactively read entire record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> get(Policy policy, Key key) throws AerospikeException;

	/**
	 * Reactively read record header and bins for specified key.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binNames				bins to retrieve
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> get(Policy policy, Key key, String[] binNames) throws AerospikeException;

	/**
	 * Asynchronously read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 *
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<List<BatchRead>> get(List<BatchRead> records) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * The returned records are located in the same list.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<List<BatchRead>> get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 *
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<BatchRead> getFlux(List<BatchRead> records) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified batch keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * This method allows different namespaces/bins to be requested for each key in the batch.
	 * Each record result is returned in separate onRecord() calls.
	 * If the BatchRead key field is not found, the corresponding record field will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param records				list of unique record identifiers and the bins to retrieve.
	 *                              The returned records are located in the same list.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<BatchRead> getFlux(BatchPolicy policy, List<BatchRead> records) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> getFlux(Key[] keys) throws AerospikeException;

	/**
	 * Reactively read multiple records for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each record result is returned in separate onRecord() calls.
	 * If a key is not found, the record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> getFlux(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Reactively read record generation and expiration only for specified key.  Bins are not read.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 *
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> getHeader(Key key) throws AerospikeException;

	/**
	 * Reactively read record generation and expiration only for specified key.  Bins are not read.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> getHeader(Policy policy, Key key) throws AerospikeException;

	/**
	 * Reactively read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 *
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysRecords> getHeaders(Key[] keys) throws AerospikeException;

	/**
	 * Reactively read multiple record header data for specified keys in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned records are in positional order with the original key array order.
	 * If a key is not found, the positional record will be null.
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					array of unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysRecords> getHeaders(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Reactively reset record's time to expiration using the policy's expiration.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Fail if the record does not exist.
	 *
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> touch(Key key) throws AerospikeException;


	/**
	 * Reactively reset record's time to expiration using the policy's expiration.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Fail if the record does not exist.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> touch(WritePolicy policy, Key key) throws AerospikeException;

	/**
	 * Reactively determine if a record key exists.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> exists(Key key) throws AerospikeException;

	/**
	 * Reactively determine if a record key exists.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy can be used to specify timeouts.
	 *
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> exists(Policy policy, Key key) throws AerospikeException;

	/**
	 * Reactively check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysExists> exists(Key[] keys) throws AerospikeException;

	/**
	 * Reactively check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The returned boolean array is in positional order with the original key array order.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeysExists> exists(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Reactively check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each key's result is returned in separate onExists() calls.
	 *
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyExists> existsFlux(Key[] keys) throws AerospikeException;

	/**
	 * Reactively check if multiple record keys exist in one batch call.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * Each key's result is returned in separate onExists() calls.
	 *
	 * @param policy				batch configuration parameters, pass in null for defaults
	 * @param keys					unique record identifiers
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyExists> existsFlux(BatchPolicy policy, Key[] keys) throws AerospikeException;

	/**
	 * Reactively write record bin(s).
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and publish result.
	 *
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException    if event loop registration fails
	 */
	Mono<Key> put(Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively write record bin(s).
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and publish result.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException    if event loop registration fails
	 */
	Mono<Key> put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively append bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> append(Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively append bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively prepend bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 *
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> prepend(Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively prepend bin string values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for string values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively add integer bin values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> add(Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively add integer bin values to existing record bin values.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout, record expiration and how the transaction is
	 * handled when the record already exists.
	 * This call only works for integer values.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param bins					array of bin name/value pairs
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException;

	/**
	 * Reactively delete record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> delete(Key key) throws AerospikeException;
	/**
	 * Reactively delete record for specified key.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The policy specifies the transaction timeout.
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<Key> delete(WritePolicy policy, Key key) throws AerospikeException;

	/**
	 * Reactively perform multiple read/write operations on a single key in one batch call.
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
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */

	Mono<KeyRecord> operate(Key key, Operation... operations) throws AerospikeException;

	/**
	 * Reactively perform multiple read/write operations on a single key in one batch call.
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
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param operations			database operations to perform
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyRecord> operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException;

	/**
	 * Reactively execute query on all server nodes.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the flux.
	 *
	 * @param statement				database query command
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> query(Statement statement) throws AerospikeException;

	/**
	 * Reactively execute query on all server nodes.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the flux.
	 *
	 * @param policy				query configuration parameters, pass in null for defaults
	 * @param statement				database query command
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> query(QueryPolicy policy, Statement statement) throws AerospikeException;

	/**
	 * Reactively read all records in specified namespace and set.  If the policy's
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> scanAll(String namespace, String setName, String... binNames) throws AerospikeException;

	/**
	 * Reactively read all records in specified namespace and set.  If the policy's
	 * <code>concurrentNodes</code> is specified, each server node will be read in
	 * parallel.  Otherwise, server nodes are read in series.
	 * <p>
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 *
	 * @param policy				scan configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param binNames				optional bin to retrieve. All bins will be returned if not specified.
	 * 								Aerospike 2 servers ignore this parameter.
	 * @throws AerospikeException	if event loop registration fails
	 */
	Flux<KeyRecord> scanAll(ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException;

	/**
	 * Reactively execute user defined function on server.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 *
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyObject> execute(Key key,
							String packageName, String functionName, Value... functionArgs) throws AerospikeException;

	/**
	 * Reactively execute user defined function on server.
	 * This method registers the command with an event loop and returns.
	 * The event loop thread will process the command and send the results to the listener.
	 * <p>
	 * The function operates on a single record.
	 * The package name is used to locate the udf file location:
	 * <p>
	 * udf file = <server udf dir>/<package name>.lua
	 *
	 * @param policy				write configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param packageName			server package name where user defined function resides
	 * @param functionName			user defined function
	 * @param functionArgs			arguments passed in to user defined function
	 * @throws AerospikeException	if event loop registration fails
	 */
	Mono<KeyObject> execute(WritePolicy policy, Key key,
								   String packageName, String functionName, Value... functionArgs) throws AerospikeException;

	/**
	 * Reactively access server's info monitoring protocol.
	 * <p>
	 * The info protocol is a name/value pair based system, where an individual
	 * database server node is queried to determine its configuration and status.
	 * The list of supported names can be found at:
	 * <p>
	 * <a href="https://www.aerospike.com/docs/reference/info/index.html">https://www.aerospike.com/docs/reference/info/index.html</a>
	 * <p>
	 *
	 */
	Mono<String> info(InfoPolicy infoPolicy, Node node, String command);

	/**
	 * Reactively access server's info monitoring protocol.
	 * <p>
	 * The info protocol is a name/value pair based system, where an individual
	 * database server node is queried to determine its configuration and status.
	 * The list of supported names can be found at:
	 * <p>
	 * <a href="https://www.aerospike.com/docs/reference/info/index.html">https://www.aerospike.com/docs/reference/info/index.html</a>
	 * <p>
	 *
	 */
	Mono<Map<String,String>> info(InfoPolicy infoPolicy, Node node, List<String> commands);

	/**
	 * Reactively create complex secondary index to be used on bins containing collections.
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
	Mono<Void> createIndex(Policy policy,
						   String namespace, String setName, String indexName, String binName,
			               IndexType indexType, IndexCollectionType indexCollectionType);

	/**
	 * Reactively delete secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index create fails
	 */
	Mono<Void> dropIndex(
			Policy policy,
			String namespace,
			String setName,
			String indexName
	);

}
