/*
 * Copyright 2012-2019 Aerospike, Inc.
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
package com.aerospike.helper.query;

import com.aerospike.client.Key;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.helper.query.cache.IndexInfoParser;
import com.aerospike.helper.query.cache.InternalIndexOperations;
import com.aerospike.helper.query.cache.ReactorIndexCache;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;

import static com.aerospike.helper.query.QueryEngine.updateStatement;

/**
 * This class provides a multi-filter reactive query engine that
 * augments the query capability in Aerospike.
 *
 */
public class ReactorQueryEngine implements Closeable {

	private final IAerospikeReactorClient client;

	private final QueryPolicy queryPolicy;

	private final ReactorIndexCache indexCache;

	/**
	 * The Query engine is constructed by passing in an existing
	 * AerospikeClient instance
	 *
	 * @param client An instance of Aerospike reactor client
	 */
	public ReactorQueryEngine(IAerospikeReactorClient client) {
		this(client, client.getQueryPolicyDefault(),
				new ReactorIndexCache(client, client.getInfoPolicyDefault(), new InternalIndexOperations(new IndexInfoParser())));
	}

	public ReactorQueryEngine(IAerospikeReactorClient client,
							  QueryPolicy queryPolicy,
							  ReactorIndexCache indexCache) {

		this.client = client;
		this.queryPolicy = queryPolicy;
		this.indexCache = indexCache;
	}

	/*
	 * *****************************************************
	 *
	 * Select
	 *
	 * *****************************************************
	 */


	/**
	 * Select records filtered by a Filter and Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set		Set storing the data
	 * @param filter	 Aerospike Filter to be used
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public Flux<KeyRecord> select(String namespace, String set, Filter filter, Qualifier... qualifiers) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilter(filter);
		return select(stmt, qualifiers);
	}

	/**
	 * Select records filtered by Qualifiers
	 *
	 * @param stmt	   A Statement object containing Namespace, Set and the Bins to be returned.
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public Flux<KeyRecord> select(Statement stmt, Qualifier... qualifiers) {
		return select(stmt, false, qualifiers);
	}

	/**
	 * Select records filtered by Qualifiers
	 *
	 * @param stmt       A Statement object containing Namespace, Set and the Bins to be returned.
	 * @param metaOnly   Set to true to return only the record meta data
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public Flux<KeyRecord> select(Statement stmt, boolean metaOnly, Qualifier... qualifiers) {

		/*
		 * no filters
		 */
		if (qualifiers == null || qualifiers.length == 0) {
			return this.client.query(queryPolicy, stmt);

		}
		/*
		 * singleton using primary key
		 */
		if (qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier kq = (KeyQualifier) qualifiers[0];
			Key key = kq.makeKey(stmt.getNamespace(), stmt.getSetName());
			if (metaOnly)
				return Flux.from(this.client.getHeader(null, key));
			else
				return Flux.from(this.client.get(null, key, stmt.getBinNames()));
		}
		/*
		 *  query with filters
		 */
		updateStatement(stmt, qualifiers, indexCache::hasIndexFor);

		return client.query(queryPolicy, stmt);
	}

	/**
	 * refreshes the Index cache from the Cluster
	 */
	public Mono<Void> refreshIndexes() {
		return indexCache.refreshIndexes();
	}

	/**
	 * closes the QueryEngine, clearing the cached information are closing the AerospikeClient.
	 * Once the QueryEngine is closed, it cannot be used, nor can the AerospikeClient.
	 */
	@Override
	public void close() throws IOException {
		if (this.client != null)
			this.client.close();
		indexCache.close();
	}

}
