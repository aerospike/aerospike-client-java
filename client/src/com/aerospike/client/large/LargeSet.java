/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.client.large;

import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

/**
 * Create and manage a Large Set within a single bin.
 * <p>
 * Deprecated: LDT functionality has been deprecated.
 */
public class LargeSet {
	private static final String PackageName = "lset";
	
	private final AerospikeClient client;
	private final WritePolicy policy;
	private final Key key;
	private final Value binName;
	private final Value createModule;
	
	/**
	 * Initialize large set operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param createModule			Lua function name that initializes list configuration parameters, pass null for default set
	 */
	public LargeSet(AerospikeClient client, WritePolicy policy, Key key, String binName, String createModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.createModule = Value.get(createModule);
	}
	
	/**
	 * Add a value to the set.  If the set does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to add
	 */
	public void add(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "add", binName, value, createModule);
	}

	/**
	 * Add values to the set.  If the set does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public void add(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values), createModule);
	}
	
	/**
	 * Add values to the set.  If the set does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public void add(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values), createModule);
	}

	/**
	 * Delete value from set.
	 * 
	 * @param value				value to delete
	 */
	public void remove(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove", binName, value);
	}

	/**
	 * Select value from set.
	 * 
	 * @param value				value to select
	 * @return					found value
	 */
	public Object get(Value value) throws AerospikeException {
		return client.execute(policy, key, PackageName, "get", binName, value);
	}

	/**
	 * Check existence of value in the set.
	 * 
	 * @param value				value to check
	 * @return					true if found, otherwise false
	 */
	public boolean exists(Value value) throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "exists", binName, value);
		return Util.toBoolean(result);
	}

	/**
	 * Return list of all objects in the set.
	 */
	public List<?> scan() throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select values from set and apply specified Lua filter.
	 * 
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public List<?> filter(String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Delete bin containing the set.
	 */
	public void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of set.
	 */
	public int size() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "size", binName);
		return Util.toInt(result);
	}

	/**
	 * Return map of set configuration parameters.
	 */
	public Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "get_config", binName);
	}
	
	/**
	 * Set maximum number of entries in the set.
	 *  
	 * @param capacity			max entries in set
	 */
	public void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries in the set.
	 */
	public int getCapacity() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "get_capacity", binName);
		return Util.toInt(result);
	}
}
