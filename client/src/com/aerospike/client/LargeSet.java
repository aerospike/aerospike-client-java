/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client;

import java.util.List;
import java.util.Map;

import com.aerospike.client.policy.Policy;

/**
 * Create and manage a list within a single bin.
 */
public final class LargeSet {
	private static final String Filename = "LSET";
	
	private final AerospikeClient client;
	private final Policy policy;
	private final Key key;
	private final Value binName;
	
	/**
	 * Initialize large set operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 */
	public LargeSet(AerospikeClient client, Policy policy, Key key, String binName) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
	}
	
	/**
	 * Create a list in a single record bin.
	 * 
	 * @param configName		Lua function name that initializes list configuration parameters, pass null for default list 
	 */
	public final void create(String configName) throws AerospikeException {
		Value configValue = LargeStack.getConfigValue(configName);
		client.execute(policy, key, Filename, "lset_create", binName, configValue);
	}

	/**
	 * Insert value into list.  If the list does not exist, create it using specified configuration.
	 * 
	 * @param configName		Lua function name that initializes list configuration parameters, pass null for default list 
	 * @param value				value to insert
	 */
	public final void insert(String configName, Value value) throws AerospikeException {
		Value configValue = LargeStack.getConfigValue(configName);
		client.execute(policy, key, Filename, "lset_create_and_insert", binName, value, configValue);
	}

	/**
	 * Insert value into list.  If the list does not exist, throw an exception.
	 * 
	 * @param value				value to insert
	 */
	public final void insert(Value value) throws AerospikeException {
		client.execute(policy, key, Filename, "lset_insert", binName, value);
	}
	
	/**
	 * Delete value from list.
	 * 
	 * @param value				value to delete
	 */
	public final void delete(Value value) throws AerospikeException {
		client.execute(policy, key, Filename, "lset_delete", binName, value);
	}

	/**
	 * Select value from list.
	 * 
	 * @param value				value to select
	 * @return					list of entries selected
	 */
	public final List<?> search(Value value) throws AerospikeException {
		return (List<?>) client.execute(policy, key, Filename, "lset_search", binName, value, Value.get(0));
	}

	/**
	 * Select value from list and apply specified Lua filter.
	 * 
	 * @param value				value to select
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public final List<?> search(Value value, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>) client.execute(policy, key, Filename, "lset_search_then_filter", binName, value, Value.get(0), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Return size of list.
	 */
	public final int size() throws AerospikeException {
		return (Integer)client.execute(policy, key, Filename, "lset_size", binName);
	}

	/**
	 * Return map of list configuration parameters.
	 */
	public final Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, Filename, "lset_config", binName);
	}
}
