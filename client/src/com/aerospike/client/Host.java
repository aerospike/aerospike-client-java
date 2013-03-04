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
 * Host name/port of database server. 
 */
public final class Host {
	/**
	 * Host name or IP address of database server.
	 */
	public final String name;
	
	/**
	 * Port of database server.
	 */
	public final int port;
	
	/**
	 * Initialize host.
	 */
	public Host(String name, int port) {
		this.name = name;
		this.port = port;
	}

	@Override
	public String toString() {
		return name + ':' + port;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + name.hashCode();
		return prime * result + port;
	}

	@Override
	public boolean equals(Object obj) {
		Host other = (Host) obj;
		return this.name.equals(other.name) && this.port == other.port;
	}
}
