/*
 * Copyright 2012-2015 Aerospike, Inc.
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
 * Create and manage a list within a single bin.
 */
public class LargeList {
	private static final String PackageName = "llist";
	
	private final AerospikeClient client;
	private final WritePolicy policy;
	private final Key key;
	private final Value binName;
	
	/**
	 * Initialize large list operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 */
	public LargeList(AerospikeClient client, WritePolicy policy, Key key, String binName) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
	}

	/**
	 * Add value to list. Fail if value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param value				value to add
	 */
	public void add(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "add", binName, value);
	}

	/**
	 * Add values to list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param values			values to add
	 */
	public void add(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values));
	}
	
	/**
	 * Add values to the list.  Fail if a value's key exists and list is configured for unique keys.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param values			values to add
	 */
	public void add(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values));
	}

	/**
	 * Update value in list if key exists.  Add value to list if key does not exist.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param value				value to update
	 */
	public void update(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "update", binName, value);
	}

	/**
	 * Update/Add each value in array depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param values			values to update
	 */
	public void update(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "update_all", binName, Value.get(values));
	}
	
	/**
	 * Update/Add each value in values list depending if key exists or not.
	 * If value is a map, the key is identified by "key" entry.  Otherwise, the value is the key.
	 * If large list does not exist, create it.
	 * 
	 * @param values			values to update
	 */
	public void update(List<?> values) throws AerospikeException {
		client.execute(policy, key, PackageName, "update_all", binName, Value.get(values));
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
		client.execute(policy, key, PackageName, "remove_all", binName, Value.get(values));
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
	 * Select values from the beginning of list up to a maximum count.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param count				maximum number of values to return
	 * @return					list of entries selected
	 */
	public List<?> findFirst(int count) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_first", binName, Value.get(count));
	}

	/**
	 * Select values from the beginning of list up to a maximum count after applying lua filter.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param count				maximum number of values to return after applying lua filter
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public List<?> findFirst(int count, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_first", binName, Value.get(count), Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Select values from the end of list up to a maximum count.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param count				maximum number of values to return
	 * @return					list of entries selected in reverse order
	 */
	public List<?> findLast(int count) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_last", binName, Value.get(count));
	}

	/**
	 * Select values from the end of list up to a maximum count after applying lua filter.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param count				maximum number of values to return after applying lua filter
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected in reverse order
	 */
	public List<?> findLast(int count, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_last", binName, Value.get(count), Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Select values from the begin key up to a maximum count.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param begin				start value (inclusive)
	 * @param count				maximum number of values to return
	 * @return					list of entries selected
	 */
	public List<?> findFrom(Value begin, int count) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_from", binName, begin, Value.get(count));
	}

	/**
	 * Select values from the begin key up to a maximum count after applying lua filter.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param begin				start value (inclusive)
	 * @param count				maximum number of values to return after applying lua filter
	 * @param filterModule		Lua module name which contains filter function
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected in reverse order
	 */
	public List<?> findFrom(Value begin, int count, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_from", binName, begin, Value.get(count), Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
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
	 * Select a range of values from the large list.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param begin				low value of the range (inclusive)
	 * @param end				high value of the range (inclusive)
	 * @param count				maximum number of values to return, pass in zero to obtain all values within range
	 * @return					list of entries selected
	 */
	public List<?> range(Value begin, Value end, int count) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_range", binName, begin, end, Value.get(count));
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
		return (List<?>)client.execute(policy, key, PackageName, "range", binName, begin, end, Value.get(filterModule), Value.get(filterModule), Value.get(filterArgs));
	}

	/**
	 * Select a range of values from the large list, then apply a lua filter.
	 * Supported by server versions >= 3.5.8.
	 * 
	 * @param begin				low value of the range (inclusive)
	 * @param end				high value of the range (inclusive)
	 * @param count				maximum number of values to return after applying lua filter. Pass in zero to obtain all values within range. 
	 * @param filterModule		lua module name which contains filter function
	 * @param filterName		lua function name which applies filter to returned list
	 * @param filterArgs		arguments to lua function name
	 * @return					list of entries selected
	 */
	public List<?> range(Value begin, Value end, int count, String filterModule, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "find_range", binName, begin, end, Value.get(count), Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
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
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, Value.get(filterModule), Value.get(filterName), Value.get(filterArgs));
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
	 * Set LDT page size. 
	 * Supported by server versions >= 3.5.8.
	 *  
	 * @param pageSize 			page size in bytes
	 */
	public void setPageSize(int pageSize) throws AerospikeException {
		client.execute(policy, key, PackageName, "setPageSize", binName, Value.get(pageSize));
	}
}
