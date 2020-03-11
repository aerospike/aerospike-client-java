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
package com.aerospike.client.reactor.retry;

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
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.client.reactor.dto.KeyExists;
import com.aerospike.client.reactor.dto.KeyObject;
import com.aerospike.client.reactor.dto.KeysExists;
import com.aerospike.client.reactor.dto.KeysRecords;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Allows to setup retry policy for all operations
 * @author Sergii Karpenko
 */
public class AerospikeReactorRetryClient implements IAerospikeReactorClient {

	private final IAerospikeReactorClient client;
	private final Function<Flux<Throwable>, ? extends Publisher<?>> whenFactory;

	public AerospikeReactorRetryClient(IAerospikeReactorClient client,
									   Function<Flux<Throwable>, ? extends Publisher<?>> whenFactory) {
		this.client = client;
		this.whenFactory = whenFactory;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	@Override
	public final Mono<KeyRecord> get(Key key) throws AerospikeException {
		return get(null, key);
	}

	@Override
	public final Mono<KeyRecord> get(Policy policy, Key key) throws AerospikeException {
		return client.get(policy, key).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeyRecord> get(Policy policy, Key key, String[] binNames) throws AerospikeException {
		return client.get(policy, key, binNames).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeysRecords> get(Key[] keys) throws AerospikeException {
		return get(null, keys);
	}

	@Override
	public final Mono<KeysRecords> get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return client.get(policy, keys).retryWhen(whenFactory);
	}

	@Override
	public final Mono<List<BatchRead>> get(List<BatchRead> records) throws AerospikeException {
		return get(null, records);
	}

	@Override
	public final Mono<List<BatchRead>> get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		return client.get(policy, records).retryWhen(whenFactory);
	}

	@Override
	public final Flux<BatchRead> getFlux(List<BatchRead> records) throws AerospikeException {
		return getFlux(null, records);
	}

	@Override
	public final Flux<BatchRead> getFlux(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		return client.getFlux(policy, records).retryWhen(whenFactory);
	}

	@Override
	public final Flux<KeyRecord> getFlux(Key[] keys) throws AerospikeException {
		return getFlux(null, keys);
	}

	@Override
	public final Flux<KeyRecord> getFlux(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return client.getFlux(policy, keys).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeyRecord> getHeader(Key key) throws AerospikeException {
		return getHeader(null, key);
	}

	@Override
	public final Mono<KeyRecord> getHeader(Policy policy, Key key) throws AerospikeException {
		return client.getHeader(policy, key).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeysRecords> getHeaders(Key[] keys) throws AerospikeException {
		return getHeaders(null, keys);
	}

	@Override
	public final Mono<KeysRecords> getHeaders(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return client.getHeaders(policy, keys).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> touch(Key key) throws AerospikeException {
		return touch(null, key);
	}

	@Override
	public final Mono<Key> touch(WritePolicy policy, Key key) throws AerospikeException {
		return client.touch(policy, key).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> exists(Key key) throws AerospikeException {
		return exists(null, key);
	}

	@Override
	public final Mono<Key> exists(Policy policy, Key key) throws AerospikeException {
		return client.exists(policy, key).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeysExists> exists(Key[] keys) throws AerospikeException {
		return exists(null, keys);
	}

	@Override
	public final Mono<KeysExists> exists(BatchPolicy policy, Key[] keys) throws AerospikeException{
		return client.exists(policy, keys).retryWhen(whenFactory);
	}

	@Override
	public final Flux<KeyExists> existsFlux(Key[] keys) throws AerospikeException {
		return existsFlux(null, keys);
	}

	@Override
	public final Flux<KeyExists> existsFlux(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return client.existsFlux(policy, keys).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> put(Key key, Bin... bins) throws AerospikeException {
		return put(null, key, bins);
	}

	@Override
	public final Mono<Key> put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return client.put(policy, key, bins).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> append(Key key, Bin... bins) throws AerospikeException {
		return append(null, key, bins);
	}

	@Override
	public final Mono<Key> append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return client.append(policy, key, bins).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> prepend(Key key, Bin... bins) throws AerospikeException {
		return prepend(null, key, bins);
	}

	@Override
	public final Mono<Key> prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return client.prepend(policy, key, bins).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> add(Key key, Bin... bins) throws AerospikeException {
		return add(null, key, bins);
	}

	@Override
	public final Mono<Key> add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		return client.add(policy, key, bins).retryWhen(whenFactory);
	}

	@Override
	public final Mono<Key> delete(Key key) throws AerospikeException {
		return delete(null, key);
	}

	@Override
	public final Mono<Key> delete(WritePolicy policy, Key key) throws AerospikeException {
		return client.delete(policy, key).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeyRecord> operate(Key key, Operation... operations) throws AerospikeException {
		return operate(null, key, operations);
	}

	@Override
	public final Mono<KeyRecord> operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		return client.operate(policy, key, operations).retryWhen(whenFactory);
	}

	@Override
	public final Flux<KeyRecord> query(Statement statement) throws AerospikeException {
		return query(null, statement);
	}

	@Override
	public final Flux<KeyRecord> query(QueryPolicy policy, Statement statement) throws AerospikeException {
		return client.query(policy, statement).retryWhen(whenFactory);
	}

	@Override
	public final Flux<KeyRecord> scanAll(String namespace, String setName, String... binNames) throws AerospikeException {
		return scanAll(null, namespace, setName, binNames);
	}

	@Override
	public final Flux<KeyRecord> scanAll(ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException {
		return client.scanAll(policy, namespace, setName, binNames).retryWhen(whenFactory);
	}

	@Override
	public final Mono<KeyObject> execute(Key key, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return execute(null, key, packageName, functionName, functionArgs);
	}

	@Override
	public final Mono<KeyObject> execute(WritePolicy policy, Key key,
								   String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return client.execute(policy, key, packageName, functionName, functionArgs).retryWhen(whenFactory);
	}

	@Override
	public Mono<String> info(InfoPolicy infoPolicy, Node node, String command){
		return client.info(infoPolicy, node, command).retryWhen(whenFactory);
	}

	@Override
	public Mono<Map<String,String>> info(InfoPolicy infoPolicy, Node node, List<String> commands){
		return client.info(infoPolicy, node, commands).retryWhen(whenFactory);
    }

	@Override
	public Mono<Void> createIndex(Policy policy,
								  String namespace, String setName, String indexName, String binName,
								  IndexType indexType, IndexCollectionType indexCollectionType){
		return client.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType).retryWhen(whenFactory);
	}

	@Override
	public Mono<Void> dropIndex(Policy policy, String namespace, String setName, String indexName){
		return client.dropIndex(policy, namespace, setName, indexName).retryWhen(whenFactory);
	}

	@Override
	public Policy getReadPolicyDefault() {
		return client.getReadPolicyDefault();
	}

	@Override
	public WritePolicy getWritePolicyDefault() {
		return client.getWritePolicyDefault();
	}

	@Override
	public ScanPolicy getScanPolicyDefault() {
		return client.getScanPolicyDefault();
	}

	@Override
	public QueryPolicy getQueryPolicyDefault() {
		return client.getQueryPolicyDefault();
	}

	@Override
	public BatchPolicy getBatchPolicyDefault() {
		return client.getBatchPolicyDefault();
	}

	@Override
	public InfoPolicy getInfoPolicyDefault() {
		return client.getInfoPolicyDefault();
	}

}
