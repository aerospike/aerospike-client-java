/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client;

import java.util.List;
import java.util.Map;


/**
 * Column name/value pair. 
 */
public final class Bin {
	/**
	 * Bin name. Current limit is 14 characters.
	 */
	public final String name;

	/**
	 * Bin value.
	 */
	public final Value value;
	
	/**
	 * Constructor, specifying bin name and string value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, String value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and byte array value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, byte[] value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and integer value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, int value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and long value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, long value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Value value) {
		this.name = name;
		this.value = value;
	}
	
	/**
	 * Constructor, specifying bin name and object value.
	 * This is the slowest of the Bin constructors because the type
	 * must be determined using multiple "instanceof" checks.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 *
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Object value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Create bin with a list value.  The list value will be serialized as a 3.0 server list type.
	 * Supported by 3.0 servers only.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asList(String name, List<?> value) {
		return new Bin(name, Value.getAsList(value));
	}

	/**
	 * Create bin with a map value.  The map value will be serialized as a 3.0 server map type.
	 * Supported by 3.0 servers only.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asMap(String name, Map<?,?> value) {
		return new Bin(name, Value.getAsMap(value));
	}
	
	/**
	 * Create bin with a blob value.  The value will be java serialized.
	 * This method is faster than the bin Object constructor because the blob is converted 
	 * directly instead of using multiple "instanceof" type checks with a blob default.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asBlob(String name, Object value) {
		return new Bin(name, Value.getAsBlob(value));
	}

	/**
	 * Create bin with a null value. This is useful for bin deletions within a record.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 */
	public static Bin asNull(String name) {
		return new Bin(name, Value.getAsNull());
	}
}
