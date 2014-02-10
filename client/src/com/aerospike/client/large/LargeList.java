/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
