/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.lua;

/**
 * Lua static configuration variables.  These variables apply to all AerospikeClient instances
 * in a single process.
 */
public final class LuaConfig {
	/**
	 * File location of user defined Lua source files.
	 */
	public static String SourceDirectory = "udf";
	
	/**
	 * Maximum number of Lua runtime instances to cache at any point in time.
	 * Each query with an aggregation function requires a Lua instance.
	 * If the number of concurrent queries exceeds the Lua pool size, a new Lua 
	 * instance will still be created, but it will not be returned to the pool. 
	 */
	public static int InstancePoolSize = 5;
}
