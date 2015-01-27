/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
 * Create and manage a list within a single bin.
 */
public class LargeList {
	private static final String PackageName = "llist";
	
	private final AerospikeClient client;
	private final WritePolicy policy;
	private final Key key;
	private final Value binName;
	private final Value createModule;
	
	/**
	 * Initialize large list operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param createModule			Lua function name that initializes list configuration parameters, pass null for default list
	 */
	public LargeList(AerospikeClient client, WritePolicy policy, Key key, String binName, String createModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.createModule = Value.get(createModule);
	}
	
	/**
	 * Add value to list. Fail if value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to add
	 */
	public void add(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "add", binName, value, createModule);
	}

	/**
	 * Add values to list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public void add(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values), createModule);
	}
	
	/**
	 * Add values to the list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public void add(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.getAsList(values), createModule);
	}

	/**
	 * Update value in list if key exists.  Add value to list if key does not exist.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to update
	 */
	public void update(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "update", binName, value, createModule);
	}

	/**
	 * Update/Add each value in array depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to update
	 */
	public void update(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "update_all", binName, Value.get(values), createModule);
	}
	
	/**
	 * Update/Add each value in values list depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to update
	 */
	public void update(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "update_all", binName, Value.getAsList(values), createModule);
	}

	/**
	 * Delete value from list.
	 * 
	 * @param value				value to delete
	 */
	public void remove(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove", binName, value);
	}

	/**
	 * Delete values from list.
	 * 
	 * @param values			values to delete
	 */
	public void remove(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove_all", binName, Value.getAsList(values));
	}

	/**
	 * Delete values from list between range.
	 * 
	 * @param begin				low value of the range (inclusive)
	 * @param end				high value of the range (inclusive)
	 * @return					count of entries removed
	 */
	public int remove(Value begin, Value end) throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "remove_range", binName, begin, end);
		return Util.toInt(result);
	}

	/**
	 * Select values from list.
	 * 
	 * @param value				value to select
	 * @return					list of entries selected
	 */
	public List<?> find(Value value) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find", binName, value);
	}

	/**
	 * Select values from list and apply specified Lua filter.
	 * 
	 * @param value				value to select
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public List<?> findThenFilter(Value value, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_then_filter", binName, value, Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}
	

	/**
	 * Select a range of values from the large list.
	 * 
	 * @param begin				low value of the range (inclusive)
	 * @param end				high value of the range (inclusive)
	 * @return					list of entries selected
	 */
	public List<?> range(Value begin, Value end) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "range", binName, begin, end);
	}

	/**
	 * Select a range of values from the large list, then apply a Lua filter.
	 * 
	 * @param begin				low value of the range (inclusive)
	 * @param end				high value of the range (inclusive)
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public List<?> range(Value begin, Value end, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "range", binName, begin, end, Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Return all objects in the list.
	 */
	public List<?> scan() throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select values from list and apply specified Lua filter.
	 * 
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public List<?> filter(String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, Value.getAsNull(), Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Delete bin containing the list.
	 */
	public void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of list.
	 */
	public int size() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "size", binName);
		return Util.toInt(result);
	}

	/**
	 * Return map of list configuration parameters.
	 */
	public Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "config", binName);
	}
	
	/**
	 * Set maximum number of entries in the list.
	 *  
	 * @param capacity			max entries in list
	 */
	public void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries in the list.
	 */
	public int getCapacity() throws AerospikeException {
		Object result = client.execute(policy, key, PackageName, "get_capacity", binName);
		return Util.toInt(result);
	}
}
