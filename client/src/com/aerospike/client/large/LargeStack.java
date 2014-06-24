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
 * Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
 */
public final class LargeStack {
	private static final String PackageName = "lstack";
	
	private final AerospikeClient client;
	private final Policy policy;
	private final Key key;
	private final Value binName;
	private final Value userModule;
	
	/**
	 * Initialize large stack operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 * @param userModule			Lua function name that initializes list configuration parameters, pass null for default set
	 */
	public LargeStack(AerospikeClient client, Policy policy, Key key, String binName, String userModule) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.userModule = Value.get(userModule);
	}
	
	/**
	 * Push value onto stack.  If the stack does not exist, create it using specified userModule configuration.
	 * 
	 * @param value				value to push
	 */
	public final void push(Value value) throws AerospikeException {
		client.execute(policy, key, PackageName, "push", binName, value, userModule);
	}

	/**
	 * Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
	 * 
	 * @param values			values to push
	 */
	public final void push(Value... values) throws AerospikeException {
		client.execute(policy, key, PackageName, "push_all", binName, Value.get(values), userModule);
	}
	
	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @return					list of items selected
	 */
	public final List<?> peek(int peekCount) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "peek", binName, Value.get(peekCount));
	}

	/**
	 * Return list of all objects on the stack.
	 */
	public final List<?> scan() throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "scan", binName);
	}

	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of items selected
	 */
	public final List<?> filter(int peekCount, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, PackageName, "filter", binName, Value.get(peekCount), userModule, Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Delete bin containing the stack.
	 */
	public final void destroy() throws AerospikeException {
		client.execute(policy, key, PackageName, "destroy", binName);
	}

	/**
	 * Return size of stack.
	 */
	public final int size() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "size", binName);
	}

	/**
	 * Return map of stack configuration parameters.
	 */
	public final Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, PackageName, "get_config", binName);
	}
	
	/**
	 * Set maximum number of entries for the stack.
	 *  
	 * @param capacity			max entries in set
	 */
	public final void setCapacity(int capacity) throws AerospikeException {
		client.execute(policy, key, PackageName, "set_capacity", binName, Value.get(capacity));
	}

	/**
	 * Return maximum number of entries for the stack.
	 */
	public final int getCapacity() throws AerospikeException {
		return (Integer)client.execute(policy, key, PackageName, "get_capacity", binName);
	}
}
