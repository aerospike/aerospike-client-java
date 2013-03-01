/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

/**
 * SQL statement creation routines.
 */
public class Statement {	
	String namespace;
	String indexName;
	String setName;
	String[] binNames;
	Filter[] filters;
	Order[] orders;
	int limit;
	int taskId;
	
	public Statement() {
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public void setSetName(String setName) {
		this.setName = setName;
	}

	public void setBinNames(String... binNames) {
		this.binNames = binNames;
	}
	
	public void setFilters(Filter... filters) {
		this.filters = filters;
	}
	
	/**
	 * Create statement from string.
	 */
	public static Statement create(String sql) {
		Statement statement = new Statement();
		// parse here.
		return statement;
	}	

	/**
	 * Create statement from format string and argument values.
	 */
	public static Statement create(String sql, Object... args) {
		Statement statement = new Statement();
		// parse here.
		return statement;
	}
}
