package com.aerospike.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.policy.Policy;

/**
 * Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
 */
public final class LargeStack {
	private static final String Filename = "lstack";
	
	private final AerospikeClient client;
	private final Policy policy;
	private final Key key;
	private final Value binName;
	
	/**
	 * Initialize large stack operator.
	 * 
	 * @param client				client
	 * @param policy				generic configuration parameters, pass in null for defaults
	 * @param key					unique record identifier
	 * @param binName				bin name
	 */
	public LargeStack(AerospikeClient client, Policy policy, Key key, String binName) {
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
	}
	
	/**
	 * Create a stack in a single record bin.
	 * 
	 * @param configName		Lua function name that initializes stack configuration parameters, pass null for default stack 
	 */
	public final void create(String configName) throws AerospikeException {
		Value configValue = getConfigValue(configName);
		client.execute(policy, key, Filename, "lstack_create", binName, configValue);
	}

	/**
	 * Push value onto stack.  If the stack does not exist, create it using specified configuration.
	 * 
	 * @param configName		Lua function name that initializes stack configuration parameters, pass null for default stack 
	 * @param value				value to push
	 */
	public final void push(String configName, Value value) throws AerospikeException {
		Value configValue = getConfigValue(configName);
		client.execute(policy, key, Filename, "lstack_create_and_push", binName, value, configValue);
	}

	/**
	 * Push value onto stack.  If the stack does not exist, throw an exception.
	 * 
	 * @param value				value to insert
	 */
	public final void push(Value value) throws AerospikeException {
		client.execute(policy, key, Filename, "lstack_push", binName, value);
	}
	
	/**
	 * Delete items from the top of stack.
	 * 
	 * @param trimCount			number of items to pop off stack
	 */
	public final void trim(int trimCount) throws AerospikeException {
		client.execute(policy, key, Filename, "lstack_trim", binName, Value.get(trimCount));
	}

	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @return					list of items selected
	 */
	public final List<?> peek(int peekCount) throws AerospikeException {
		return (List<?>)client.execute(policy, key, Filename, "lstack_peek", binName, Value.get(peekCount));
	}

	/**
	 * Select items from top of stack.
	 * 
	 * @param peekCount			number of items to select.
	 * @param filterName		Lua function name which applies filter to returned list
	 * @param filterArgs		arguments to Lua function name
	 * @return					list of items selected
	 */
	public final List<?> peek(int peekCount, String filterName, Value... filterArgs) throws AerospikeException {
		return (List<?>)client.execute(policy, key, Filename, "lstack_peek_then_filter", binName, Value.get(peekCount), Value.get(filterName), Value.get(filterArgs));
	}

	/**
	 * Return size of stack.
	 */
	public final int size() throws AerospikeException {
		return (Integer)client.execute(policy, key, Filename, "lstack_size", binName);
	}

	/**
	 * Return map of stack configuration parameters.
	 */
	public final Map<?,?> getConfig() throws AerospikeException {
		return (Map<?,?>)client.execute(policy, key, Filename, "lstack_config", binName);
	}
	
	protected static final Value getConfigValue(String configName) {
		if (configName != null) {
			HashMap<String,String> map = new HashMap<String,String>();
			map.put("Package", configName);
			return Value.getAsMap(map);
		}
		else {
			return Value.getAsNull();
		}
	}
}
