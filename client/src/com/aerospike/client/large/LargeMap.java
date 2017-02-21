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

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

/**
 * Create and manage a map within a single bin.
 * <p>
 * Deprecated: LDT functionality has been deprecated.
 */
public class LargeMap {
	private static final String PackageName = "lmap";
	
	private final AerospikeClient client;
	private final WritePolicy policy;
	private final Key key;
	private final Value binName;
	private final Value createModule;
	
	/**
	 * Initialize large map operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param createModule			Lua function name that initializes list configuration parameters, pass null for default set
	 */
	public LargeMap(AerospikeClient client, WritePolicy policy, Key key, String binName, String createModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.createModule = Value.get(createModule);
	}
	
	/**
	 * Add entry to map.  If the map does not exist, create it using specified userModule configuration.
	 * 
	 * @param name				entry key
	 * @param value				entry value
	 */
	public void put(Value name, Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "put", binName, name, value, createModule);
	}

	/**
	 * Add map values to map.  If the map does not exist, create it using specified userModule configuration.
	 * 
	 * @param map				map values to push
	 */
	public void put(Map<?,?> map) throws AerospikeException {
		client.execute(policy, key, PackageName, "put_all", binName, Value.get(map), createModule);
	}
	
	/**
	 * Get value from map given name key.
	 * 
	 * @param name				key.
	 * @return					map of items selected
	 */
	public Map<?,?> get(Value name) throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "get", binName, name);
	}

	/**
	 * Check existence of key in the map.
	 * 
	 * @param keyValue			key to check
	 * @return					true if found, otherwise false
	 */
	public boolean exists(Value keyValue) throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "exists", binName, keyValue);
		return Util.toBoolean(result);
	}

	/**
	 * Return all objects in the map.
	 */
	public Map<?,?> scan() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select items from map.
	 * 
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of items selected
	 */
	public Map<?,?> filter(String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "filter", binName, Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}
	
	/**
	 * Remove entry from map.  
	 * 
	 * @param name				entry key
	 */
	public void remove(Value name) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove", binName, name, createModule);
	}


	/**
	 * Delete bin containing the map.
	 */
	public void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of map.
	 */
	public int size() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "size", binName);
		return Util.toInt(result);
	}

	/**
	 * Return map configuration parameters.
	 */
	public Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "config", binName);
	}
	
	/**
	 * Set maximum number of entries for the map.
	 *  
	 * @param capacity			max entries in set
	 */
	public void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries for the map.
	 */
	public int getCapacity() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "get_capacity", binName);
		return Util.toInt(result);
	}
}
