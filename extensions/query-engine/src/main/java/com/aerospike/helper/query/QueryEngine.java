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
package com.aerospike.helper.query;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.model.Module;
import com.aerospike.helper.model.Namespace;

/**
 * This class provides a multi-filter query engine that
 * augments the query capability in Aerospike.
 * To achieve this the class uses a UserDefined Function written in Lua to
 * provide the additional filtering. This UDF module packaged in the JAR and is automatically registered
 * with the cluster.
 *
 * @author peter
 */
public class QueryEngine implements Closeable {

	protected static final String QUERY_MODULE = "as_utility"; //DO NOT use decimal places in the module name
	protected static final String AS_UTILITY_PATH = QUERY_MODULE + ".lua";
	protected static Logger log = LoggerFactory.getLogger(QueryEngine.class);
	protected AerospikeClient client;
	protected Map<String, Index> indexCache;
	protected Map<String, Module> moduleCache;
	protected TreeMap<String, Namespace> namespaceCache;

	public WritePolicy updatePolicy;
	public WritePolicy insertPolicy;
	public InfoPolicy infoPolicy;
	public QueryPolicy queryPolicy;

	public enum Meta {
		KEY,
		TTL,
		EXPIRATION,
		GENERATION;

		@Override
		public String toString() {
			switch (this) {
				case KEY:
					return "__key";
				case EXPIRATION:
					return "__Expiration";
				case GENERATION:
					return "__generation";
				default:
					throw new IllegalArgumentException();
			}
		}
	}

	/**
	 * The Query engine is constructed by passing in an existing
	 * AerospikeClient instance
	 *
	 * @param client An instance of Aerospike client
	 */
	public QueryEngine(AerospikeClient client) {
		this();
		setClient(client);
	}

	public QueryEngine() {
		super();
	}

	/**
	 * Sets the AerospikeClient
	 *
	 * @param client An instance of AerospikeClient
	 */
	public void setClient(AerospikeClient client) {
		this.client = client;
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		this.insertPolicy = new WritePolicy(this.client.writePolicyDefault);
		this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.queryPolicy = client.queryPolicyDefault;
		refreshCluster();
		registerUDF();
	}

	/*
	 * *****************************************************
	 *
	 * Select
	 *
	 * *****************************************************
	 */


	/**
	 * Select records filtered by Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set		Set storing the data
	 * @param filter	 Aerospike Filter to be used
	 * @param sortMap	<STRONG>NOT IMPLEMENTED</STRONG>
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public KeyRecordIterator select(String namespace, String set, Filter filter, Map<String, String> sortMap, Qualifier... qualifiers) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilter(filter);
		return select(stmt, sortMap, qualifiers);
	}

	/**
	 * Select records filtered by Qualifiers
	 *
	 * @param stmt	   A Statement object containing Namespace, Set and the Bins to be returned.
	 * @param sortMap	<STRONG>NOT IMPLEMENTED</STRONG>
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public KeyRecordIterator select(Statement stmt, Map<String, String> sortMap, Qualifier... qualifiers) {
		KeyRecordIterator results = null;

		if (qualifiers != null && qualifiers.length > 0) {
			Map<String, Object> originArgs = new HashMap<String, Object>();
			originArgs.put("includeAllFields", 1);
			String filterFuncStr = buildFilterFunction(qualifiers);
			originArgs.put("filterFuncStr", filterFuncStr);
			String sortFuncStr = buildSortFunction(sortMap);
			originArgs.put("sortFuncStr", sortFuncStr);
			stmt.setAggregateFunction(this.getClass().getClassLoader(), AS_UTILITY_PATH, QUERY_MODULE, "select_records", Value.get(originArgs));
			ResultSet resultSet = this.client.queryAggregate(queryPolicy, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), resultSet);
		} else {
			RecordSet recordSet = this.client.query(queryPolicy, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), recordSet);
		}
		return results;
	}

	/**
	 * Select records filtered by a Filter and Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set		Set storing the data
	 * @param filter	 Aerospike Filter to be used
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public KeyRecordIterator select(String namespace, String set, Filter filter, Qualifier... qualifiers) {
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
	public KeyRecordIterator select(Statement stmt, Qualifier... qualifiers) {
		return select(stmt, false, null, qualifiers);
	}

	/**
	 * Select records filtered by Qualifiers
	 *
	 * @param stmt       A Statement object containing Namespace, Set and the Bins to be returned.
	 * @param metaOnly   Set to true to return only the record meta data
	 * @param node       Node to query.  Use null to query all nodes.
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public KeyRecordIterator select(Statement stmt, boolean metaOnly, Node node, Qualifier... qualifiers) {

		/*
		 * no filters
		 */
		if (qualifiers == null || qualifiers.length == 0) {
			RecordSet recordSet = null;
			if (node != null)
				recordSet = this.client.queryNode(queryPolicy, stmt, node);
			else
				recordSet = this.client.query(queryPolicy, stmt);
			return new KeyRecordIterator(stmt.getNamespace(), recordSet);
		}
		/*
		 * singleton using primary key
		 */
		if (qualifiers != null && qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier kq = (KeyQualifier) qualifiers[0];
			Key key = kq.makeKey(stmt.getNamespace(), stmt.getSetName());
			Record record = null;
			if (metaOnly)
				record = this.client.getHeader(null, key);
			else
				record = this.client.get(null, key, stmt.getBinNames());
			if (record == null) {
				return new KeyRecordIterator(stmt.getNamespace());
			} else {
				KeyRecord keyRecord = new KeyRecord(key, record);
				return new KeyRecordIterator(stmt.getNamespace(), keyRecord);
			}
		}
		/*
		 *  query with filters
		 */
		for (int i = 0; i < qualifiers.length; i++) {
			Qualifier qualifier = qualifiers[i];

			if(qualifier == null) continue;
			if(qualifier.getOperation()==Qualifier.FilterOperation.AND){
				for(Qualifier q: qualifier.getQualifiers()){
					Filter filter = q == null ? null : q.asFilter();
					if (filter != null) {
						stmt.setFilter(filter);
						q.asFilter(true);;
						break;
					}
				}
			}else if (isIndexedBin(stmt, qualifier)) {
				Filter filter = qualifier.asFilter();
				if (filter != null) {
					stmt.setFilter(filter);
					qualifiers[i] = null;
					break;
				}
			}
		}

		try {
			PredExp[] predexps;
			predexps = buildPredExp(qualifiers).toArray(new PredExp[0]);
			if(predexps.length > 0){
				stmt.setPredExp(predexps);
				RecordSet rs;
				if(null == node){
					rs = client.query(queryPolicy, stmt);
				}else{
					rs = client.queryNode(queryPolicy, stmt, node);
				}
				return new KeyRecordIterator(stmt.getNamespace(), rs);
			}else{
				return queryByLua(stmt, metaOnly, node, qualifiers);
			}
		} catch (PredExpException e) {
			return queryByLua(stmt, metaOnly, node, qualifiers);
		}
	}

	private KeyRecordIterator queryByLua(Statement stmt, Boolean metaOnly, Node node, Qualifier[] qualifiers){
		Map<String, Object> originArgs = new HashMap<String, Object>();
		originArgs.put("includeAllFields", 1);
		ResultSet resultSet = null;

		String filterFuncStr = buildFilterFunction(qualifiers);
		originArgs.put("filterFuncStr", filterFuncStr);

		if (metaOnly)
			stmt.setAggregateFunction(this.getClass().getClassLoader(), AS_UTILITY_PATH, QUERY_MODULE, "query_meta", Value.get(originArgs));
		else
			stmt.setAggregateFunction(this.getClass().getClassLoader(), AS_UTILITY_PATH, QUERY_MODULE, "select_records", Value.get(originArgs));
		if (node != null) {
			resultSet = this.client.queryAggregateNode(queryPolicy, stmt, node);
		} else {
			resultSet = this.client.queryAggregate(queryPolicy, stmt);
		}
		return new KeyRecordIterator(stmt.getNamespace(), resultSet);

	}

	protected boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
		if(null == qualifier.getField()) return false;
		Index index = this.indexCache.get(String.join(":", Arrays.asList(stmt.getNamespace(), stmt.getSetName(), qualifier.getField())));
		if (index == null)
			return false;

		switch (qualifier.getOperation()){
			case EQ: case BETWEEN: case GT: case GTEQ: case LT: case LTEQ:
				return true;
			default:
				return false;
		}
	}

	/*
	 * *****************************************************
	 *
	 * Insert
	 *
	 * *****************************************************
	 */

	/**
	 * inserts a record. If the record exists, and exception will be thrown.
	 *
	 * @param namespace Namespace to store the record
	 * @param set	   Set to store the record
	 * @param key	   Key of the record
	 * @param bins	  A list of Bins to insert
	 */
	public void insert(String namespace, String set, Key key, List<Bin> bins) {
		insert(namespace, set, key, bins, 0);
	}

	/**
	 * inserts a record with a time to live. If the record exists, and exception will be thrown.
	 *
	 * @param namespace Namespace to store the record
	 * @param set	   Set to store the record
	 * @param key	   Key of the record
	 * @param bins	  A list of Bins to insert
	 * @param ttl	   The record time to live in seconds
	 */
	public void insert(String namespace, String set, Key key, List<Bin> bins, int ttl) {
		this.client.put(this.insertPolicy, key, bins.toArray(new Bin[0]));
	}

	/**
	 * inserts a record using a Statement and KeyQualifier. If the record exists, and exception will be thrown.
	 *
	 * @param stmt		 A Statement object containing Namespace and Set
	 * @param keyQualifier KeyQualifier containin the primary key
	 * @param bins		 A list of Bins to insert
	 */
	public void insert(Statement stmt, KeyQualifier keyQualifier, List<Bin> bins) {
		insert(stmt, keyQualifier, bins, 0);
	}

	/**
	 * inserts a record, with a time to live, using a Statement and KeyQualifier. If the record exists, and exception will be thrown.
	 *
	 * @param stmt		 A Statement object containing Namespace and Set
	 * @param keyQualifier KeyQualifier containin the primary key
	 * @param bins		 A list of Bins to insert
	 * @param ttl		  The record time to live in seconds
	 */
	public void insert(Statement stmt, KeyQualifier keyQualifier, List<Bin> bins, int ttl) {
		Key key = keyQualifier.makeKey(stmt.getNamespace(), stmt.getSetName());
		//		Key key = new Key(stmt.getNamespace(), stmt.getSetName(), keyQualifier.getValue1());
		this.client.put(this.insertPolicy, key, bins.toArray(new Bin[0]));
	}


	/*
	 * *****************************************************
	 *
	 * Update
	 *
	 * *****************************************************
	 */

	/**
	 * The list of Bins will update each record that match the Qualifiers supplied.
	 *
	 * @param stmt	   A Statement object containing Namespace and Set
	 * @param bins	   A list of Bin objects with the values to updated
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return returns a Map containing a number of successful updates. The Map will contain 2 keys "read" and "write", the values will be the count of successful operations
	 */
	public Map<String, Long> update(Statement stmt, List<Bin> bins, Qualifier... qualifiers) {
		if (qualifiers != null && qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier keyQualifier = (KeyQualifier) qualifiers[0];
			Key key = keyQualifier.makeKey(stmt.getNamespace(), stmt.getSetName());
			this.client.put(this.updatePolicy, key, bins.toArray(new Bin[0]));
			Map<String, Long> result = new HashMap<String, Long>();
			result.put("read", 1L);
			result.put("write", 1L);
			return result;
		} else {
			KeyRecordIterator results = select(stmt, true, null, qualifiers);
			return update(results, bins);
		}
	}

	private Map<String, Long> update(KeyRecordIterator results, List<Bin> bins) {
		long readCount = 0;
		long updateCount = 0;
		while (results.hasNext()) {
			KeyRecord keyRecord = results.next();
			readCount++;
			WritePolicy up = new WritePolicy(updatePolicy);
			up.generation = keyRecord.record.generation;
			try {
				client.put(up, keyRecord.key, bins.toArray(new Bin[0]));
				updateCount++;
			} catch (AerospikeException e) {
				System.out.println(keyRecord.key);
			}
		}
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("read", readCount);
		map.put("write", updateCount);
		return map;
	}

	/*
	 * *****************************************************
	 *
	 * Delete
	 *
	 * *****************************************************
	 */

	/**
	 * Deletes the records specified by the Statement and Qualifiers
	 *
	 * @param stmt	   A Statement object containing Namespace and Set
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return returns a Map containing a number of successful updates. The Map will contain 2 keys "read" and "write", the values will be the count of successful operations
	 */
	public Map<String, Long> delete(Statement stmt, Qualifier... qualifiers) {
		if (qualifiers == null || qualifiers.length == 0) {
			/*
			 * There are no qualifiers, so delete every record in the set
			 * using Scan UDF delete
			 */
			ExecuteTask task = client.execute(null, stmt, QUERY_MODULE, "delete_record");
			task.waitTillComplete();
			return null;
		}

		if (qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier keyQualifier = (KeyQualifier) qualifiers[0];
			Key key = keyQualifier.makeKey(stmt.getNamespace(), stmt.getSetName());
			this.client.delete(null, key);
			Map<String, Long> map = new HashMap<String, Long>();
			map.put("read", 1L);
			map.put("write", 1L);
			return map;
		}
		KeyRecordIterator results = select(stmt, true, null, qualifiers);
		return delete(results);
	}

	private Map<String, Long> delete(KeyRecordIterator results) {
		long readCount = 0;
		long updateCount = 0;
		while (results.hasNext()) {
			KeyRecord keyRecord = results.next();
			readCount++;
			try {
				if (client.delete(null, keyRecord.key))
					updateCount++;
			} catch (AerospikeException e) {
				log.error("Unexpected exception deleting " + keyRecord.key, e);
			}
		}
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("read", readCount);
		map.put("write", updateCount);
		return map;
	}

	private String buildSortFunction(Map<String, String> sortMap) {
		// TODO Auto-generated method stub
		return null;
	}

	protected List<PredExp> buildPredExp(Qualifier[] qualifiers) throws PredExpException{
		List<PredExp> pes = new ArrayList<PredExp>();
		int qCount = 0;
		for(Qualifier q : qualifiers){
			if(null != q && !q.queryAsFilter()) {
				List<PredExp> tpes = q.toPredExp();
				if(tpes.size()>0){
					pes.addAll(tpes);
					qCount ++;
					q = null;
				}
			}
		}

		if(qCount>1) pes.add(PredExp.and(qCount));
		return pes;
	}

	protected String buildFilterFunction(Qualifier[] qualifiers) {
		int count = 0;
		StringBuilder sb = new StringBuilder("if ");
		for (int i = 0; i < qualifiers.length; i++) {
			if (qualifiers[i] == null) //Skip nulls
				continue;
			if (qualifiers[i] instanceof KeyQualifier) //Skip primary key -- should not happen
				continue;
			if (count > 0)
				sb.append(" and ");

			sb.append(qualifiers[i].luaFilterString());
			count++;
		}
		sb.append(" then selectedRec = true end");
		return sb.toString();
	}

	private void registerUDF() {
		if (!this.moduleCache.containsKey(QUERY_MODULE + ".lua")) { // register the as_utility udf module

			RegisterTask task = this.client.register(null, this.getClass().getClassLoader(),
					AS_UTILITY_PATH,
					QUERY_MODULE + ".lua", Language.LUA);
			task.isDone();
		}
	}

	/**
	 * Gets the current InfoPolicy
	 *
	 * @return the current InfoPolicy
	 */
	public InfoPolicy getInfoPolicy() {
		if (this.infoPolicy == null) {
			this.infoPolicy = new InfoPolicy();
		}
		return this.infoPolicy;
	}

	/**
	 * refreshes the cached Cluster information
	 */
	public void refreshCluster() {
		refreshNamespaces();
		refreshIndexes();
		refreshModules();
	}

	/**
	 * refreshes the cached Namespace information
	 */
	public synchronized void refreshNamespaces() {
		/*
		 * cache namespaces
		 */
		if (this.namespaceCache == null) {
			this.namespaceCache = new TreeMap<String, Namespace>();
			Node[] nodes = client.getNodes();
			for (Node node : nodes) {
				try {
					String namespaceString = Info.request(getInfoPolicy(), node, "namespaces");
					if (!namespaceString.isEmpty()) {
						String[] namespaceList = namespaceString.split(";");
						for (String namespace : namespaceList) {
							Namespace ns = this.namespaceCache.get(namespace);
							if (ns == null) {
								ns = new Namespace(namespace);
								this.namespaceCache.put(namespace, ns);
							}
							refreshNamespaceData(node, ns);
						}
					}
				} catch (AerospikeException e) {
					log.error("Error geting Namespaces ", e);
				}

			}
		}
	}

	public void refreshNamespaceData(Node node, Namespace namespace) {
		/*
		 * refresh namespace data
		 */
		try {
			String nameSpaceString = Info.request(infoPolicy, node, "namespace/" + namespace);
			namespace.mergeNamespaceInfo(nameSpaceString);
			String setsString = Info.request(infoPolicy, node, "sets/" + namespace);
			if (!setsString.isEmpty()) {
				String[] sets = setsString.split(";");
				for (String setData : sets) {
					namespace.mergeSet(setData);
				}
			}
		} catch (AerospikeException e) {
			log.error("Error geting Namespace details", e);
		}
	}

	/**
	 * Get as specific Namespace from the cache
	 *
	 * @param namespace Namespace name
	 * @return The Namespace model object
	 */
	public Namespace getNamespace(String namespace) {
		return namespaceCache.get(namespace);
	}

	/**
	 * Gets all the Namespaces from the cache
	 *
	 * @return A collection of Namespace model objects
	 */
	public Collection<Namespace> getNamespaces() {
		return namespaceCache.values();
	}

	/**
	 * refreshes the Index cache from the Cluster
	 */
	public synchronized void refreshIndexes() {
		/*
		 * cache index by Bin name
		 */
		if (this.indexCache == null)
			this.indexCache = new TreeMap<String, Index>();

		Node[] nodes = client.getNodes();
		for (Node node : nodes) {
			if (node.isActive()) {
				try {
					String indexString = Info.request(getInfoPolicy(), node, "sindex");
					if (!indexString.isEmpty()) {
						String[] indexList = indexString.split(";");
						for (String oneIndexString : indexList) {
							Index index = new Index(oneIndexString);
							this.indexCache.put(index.toKeyString(), index);
						}
					}
					break;
				} catch (AerospikeException e) {
					log.error("Error geting Index informaton", e);
				}
			}
		}
	}

	/**
	 * Gets a specific index from the index cache by Bin name
	 *
	 * @param key The key = namespace:set:bin built from the indexed Bin
	 * @return An Index model object
	 */
	public synchronized Index getIndex(String key) {
		return this.indexCache.get(key);
	}

	/**
	 * refreshes the Module cache from the cluster. The Module cache contains a list of register UDF modules.
	 */
	public synchronized void refreshModules() {
		if (this.moduleCache == null)
			this.moduleCache = new TreeMap<String, Module>();
		boolean loadedModules = false;
		Node[] nodes = client.getNodes();
		for (Node node : nodes) {
			try {

				String packagesString = Info.request(infoPolicy, node, "udf-list");
				if (!packagesString.isEmpty()) {
					String[] packagesList = packagesString.split(";");
					for (String pkgString : packagesList) {
						Module module = new Module(pkgString);
						String udfString = Info.request(infoPolicy, node, "udf-get:filename=" + module.getName());
						module.setDetailInfo(udfString);//gen=qgmyp0d8hQNvJdnR42X3BXgUGPE=;type=LUA;recordContent=bG9jYWwgZnVuY3Rpb24gcHV0QmluKHIsbmFtZSx2YWx1ZSkKICAgIGlmIG5vdCBhZXJvc3Bpa2U6ZXhpc3RzKHIpIHRoZW4gYWVyb3NwaWtlOmNyZWF0ZShyKSBlbmQKICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgYWVyb3NwaWtlOnVwZGF0ZShyKQplbmQKCi0tIFNldCBhIHBhcnRpY3VsYXIgYmluCmZ1bmN0aW9uIHdyaXRlQmluKHIsbmFtZSx2YWx1ZSkKICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCmVuZAoKLS0gR2V0IGEgcGFydGljdWxhciBiaW4KZnVuY3Rpb24gcmVhZEJpbihyLG5hbWUpCiAgICByZXR1cm4gcltuYW1lXQplbmQKCi0tIFJldHVybiBnZW5lcmF0aW9uIGNvdW50IG9mIHJlY29yZApmdW5jdGlvbiBnZXRHZW5lcmF0aW9uKHIpCiAgICByZXR1cm4gcmVjb3JkLmdlbihyKQplbmQKCi0tIFVwZGF0ZSByZWNvcmQgb25seSBpZiBnZW4gaGFzbid0IGNoYW5nZWQKZnVuY3Rpb24gd3JpdGVJZkdlbmVyYXRpb25Ob3RDaGFuZ2VkKHIsbmFtZSx2YWx1ZSxnZW4pCiAgICBpZiByZWNvcmQuZ2VuKHIpID09IGdlbiB0aGVuCiAgICAgICAgcltuYW1lXSA9IHZhbHVlCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgZW5kCmVuZAoKLS0gU2V0IGEgcGFydGljdWxhciBiaW4gb25seSBpZiByZWNvcmQgZG9lcyBub3QgYWxyZWFkeSBleGlzdC4KZnVuY3Rpb24gd3JpdGVVbmlxdWUocixuYW1lLHZhbHVlKQogICAgaWYgbm90IGFlcm9zcGlrZTpleGlzdHMocikgdGhlbiAKICAgICAgICBhZXJvc3Bpa2U6Y3JlYXRlKHIpIAogICAgICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFZhbGlkYXRlIHZhbHVlIGJlZm9yZSB3cml0aW5nLgpmdW5jdGlvbiB3cml0ZVdpdGhWYWxpZGF0aW9uKHIsbmFtZSx2YWx1ZSkKICAgIGlmICh2YWx1ZSA+PSAxIGFuZCB2YWx1ZSA8PSAxMCkgdGhlbgogICAgICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCiAgICBlbHNlCiAgICAgICAgZXJyb3IoIjEwMDA6SW52YWxpZCB2YWx1ZSIpIAogICAgZW5kCmVuZAoKLS0gUmVjb3JkIGNvbnRhaW5zIHR3byBpbnRlZ2VyIGJpbnMsIG5hbWUxIGFuZCBuYW1lMi4KLS0gRm9yIG5hbWUxIGV2ZW4gaW50ZWdlcnMsIGFkZCB2YWx1ZSB0byBleGlzdGluZyBuYW1lMSBiaW4uCi0tIEZvciBuYW1lMSBpbnRlZ2VycyB3aXRoIGEgbXVsdGlwbGUgb2YgNSwgZGVsZXRlIG5hbWUyIGJpbi4KLS0gRm9yIG5hbWUxIGludGVnZXJzIHdpdGggYSBtdWx0aXBsZSBvZiA5LCBkZWxldGUgcmVjb3JkLiAKZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyLG5hbWUxLG5hbWUyLGFkZFZhbHVlKQogICAgbG9jYWwgdiA9IHJbbmFtZTFdCgogICAgaWYgKHYgJSA5ID09IDApIHRoZW4KICAgICAgICBhZXJvc3Bpa2U6cmVtb3ZlKHIpCiAgICAgICAgcmV0dXJuCiAgICBlbmQKCiAgICBpZiAodiAlIDUgPT0gMCkgdGhlbgogICAgICAgIHJbbmFtZTJdID0gbmlsCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgICAgIHJldHVybgogICAgZW5kCgogICAgaWYgKHYgJSAyID09IDApIHRoZW4KICAgICAgICByW25hbWUxXSA9IHYgKyBhZGRWYWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFNldCBleHBpcmF0aW9uIG9mIHJlY29yZAotLSBmdW5jdGlvbiBleHBpcmUocix0dGwpCi0tICAgIGlmIHJlY29yZC50dGwocikgPT0gZ2VuIHRoZW4KLS0gICAgICAgIHJbbmFtZV0gPSB2YWx1ZQotLSAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQotLSAgICBlbmQKLS0gZW5kCg==;
						this.moduleCache.put(module.getName(), module);
					}
				}
				loadedModules = true;
				break;
			} catch (AerospikeException e) {

			}
		}
		if (!loadedModules) {
			throw new ClusterRefreshError("Cannot find UDF modules");
		}
	}

	/**
	 * Gets a specific Module from the cache by name
	 *
	 * @param moduleName The name of the module
	 * @return A Module model object
	 */
	public synchronized Module getModule(String moduleName) {
		return this.moduleCache.get(moduleName);
	}

	/**
	 * closes the QueryEngine, clearing the cached information are closing the AerospikeClient.
	 * Once the QueryEngine is closed, it cannot be used, nor can the AerospikeClient.
	 */
	@Override
	public void close() throws IOException {
		if (this.client != null)
			this.client.close();
		indexCache.clear();
		indexCache = null;
		updatePolicy = null;
		insertPolicy = null;
		infoPolicy = null;
		queryPolicy = null;
		moduleCache.clear();
		moduleCache = null;
	}

}
