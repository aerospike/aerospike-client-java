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
 * Create and manage a set within a single bin.
 */
public final class LargeSet {
	private static final String PackageName = "lset";
	
	private final AerospikeClient client;
	private final Policy policy;
	private final Key key;
	private final Value binName;
	private final Value userModule;
	
	/**
	 * Initialize large set operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default set
	 */
	public LargeSet(AerospikeClient client, Policy policy, Key key, String binName, String userModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.userModule = Value.get(userModule);
	}
	
	/**
	 * Add a value to the set.  If the set does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to add
	 */
	public final void add(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "add", binName, value, userModule);
	}

	/**
	 * Add values to the set.  If the set does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to add
	 */
	public final void add(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "add_all", binName, Value.get(values), userModule);
	}
	
	/**
	 * Delete value from set.
	 * 
	 * @param value				value to delete
	 */
	public final void remove(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "remove", binName, value);
	}

	/**
	 * Select value from set.
	 * 
	 * @param value				value to select
	 * @return					found value
	 */
	public final Object get(Value value) throws AerospikeException {
		return client.execute(policy, key, PackageName, "get", binName, value);
	}

	/**
	 * Check existence of value in the set.
	 * 
	 * @param value				value to check
	 * @return					true if found, otherwise false
	 */
	public final boolean exists(Value value) throws AerospikeException {
		int ret = (Integer)client.execute(policy, key, PackageName, "exists", binName, value);
		return ret == 1;
	}

	/**
	 * Return list of all objects in the set.
	 */
	public final List<?> scan() throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select values from set and apply specified Lua filter.
	 * 
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of entries selected
	 */
	public final List<?> filter(String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, userModule, Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Delete bin containing the set.
	 */
	public final void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of set.
	 */
	public final int size() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "size", binName);
	}

	/**
	 * Return map of set configuration parameters.
	 */
	public final Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "get_config", binName);
	}
	
	/**
	 * Set maximum number of entries in the set.
	 *  
	 * @param capacity			max entries in set
	 */
	public final void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries in the set.
	 */
	public final int getCapacity() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "get_capacity", binName);
	}
}
