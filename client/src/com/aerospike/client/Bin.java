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

/**
 * Column name/value pair. 
 */
public final class Bin {
	/**
	 * Bin name. Current limit is 14 characters.
	 */
	public String name;

	/**
	 * Bin value.
	 */
	public Object value;

	/**
	 * Constructor, specifying bin name and value.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Object value) {
		this.name = name;
		this.value = value;
	}
	
	/**
	 * Constructor for single bin servers, specifying bin value only.
	 * 
	 * @param value		bin value
	 */
	public Bin(Object value) {
		this.name = "";
		this.value = value;
	}
	
	/**
	 * Construct empty bin.
	 */
	public Bin() {
	}
}
