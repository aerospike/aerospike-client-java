/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.sql;

/**
 * SQL statement creation routines.
 */
public class Statement {
	/**
	 * SQL string with argument place holders.
	 */
	public final String statement;
	
	/**
	 * Argument values.
	 */
	public final Object[] objects;
	
	private Statement(String statement) {
		this.statement = statement;
		objects = null;
	}

	private Statement(String statement, Object... args) {
		this.statement = statement;
		objects = args;
	}
	
	/**
	 * Create statement from string.
	 */
	public static Statement create(String statement) {
		return new Statement(statement);
	}	

	/**
	 * Create statement from format string and argument values.
	 */
	public static Statement create(String statement, Object... args) {
		return new Statement(statement, args);
	}	
}
