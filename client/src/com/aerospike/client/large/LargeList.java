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
import com.aerospike.client.policy.Policy;

/**
 * Create and manage a list within a single bin.
 */
public final class LargeList {
	private static final String PackageName = "llist";
	
	private final AerospikeClient client;
	private final Policy policy;
	private final Key key;
	private final Value binName;
	private final Value userModule;
	
	/**
	 * Initialize large list operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default list
	 */
	public LargeList(AerospikeClient client, Policy policy, Key key, String binName, String userModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.userModule = Value.get(userModule);
	}
	
	/**
	 * Add a value to the list.  If the list does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to add
	 */
	public final void add(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "add", binName, value, userModule);
	}

	/**
	 * Add values to the list.  If the list does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public final void add(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values), userModule);
	}
	
	/**
	 * Delete value from list.
	 * 
	 * @param value				value to delete
	 */
	public final void remove(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove", binName, value);
	}

	/**
	 * Select values from list.
	 * 
	 * @param value				value to select
	 * @return					list of entries selected
	 */
	public final List<?> find(Value value) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find", binName, value);
	}

	/**
	 * Select values from list and apply specified Lua filter.
	 * 
	 * @param value				value to select
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public final List<?> findThenFilter(Value value, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_then_filter", binName, value, userModule, Value.get(filterName), Value.get(filterArgs));
	}
	

	/**
	 * Select a range of values from the large list.
	 * 
	 * @param minValue			low value of the range
	 * @param maxValue			high value of the range
	 * @return					list of entries selected
	 */
	public final List<?> range(Value minValue, Value maxValue) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "range", binName, minValue, maxValue);
	}

	/**
	 * Select a range of values from the large list, then apply a Lua filter.
	 * 
	 * @param minValue			low value of the range
	 * @param maxValue			high value of the range
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public final List<?> range(Value minValue, Value maxValue, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "range", binName, minValue, maxValue, userModule, Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Return all objects in the list.
	 */
	public final List<?> scan() throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select values from list and apply specified Lua filter.
	 * 
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public final List<?> filter(String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, userModule, Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Delete bin containing the list.
	 */
	public final void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of list.
	 */
	public final int size() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "size", binName);
	}

	/**
	 * Return map of list configuration parameters.
	 */
	public final Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "config", binName);
	}
	
	/**
	 * Set maximum number of entries in the list.
	 *  
	 * @param capacity			max entries in list
	 */
	public final void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries in the list.
	 */
	public final int getCapacity() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "get_capacity", binName);
	}
}
