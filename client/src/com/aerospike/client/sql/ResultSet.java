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

import com.aerospike.client.Key;

/**
 * Return from SQL requests. Not implemented.
 */
public class ResultSet {	
	int offset;
	
	//-------------------------------------------------------
	// Record traversal methods
	//-------------------------------------------------------

	public boolean next() {
		return false;
	}
		
	public void close() {
	}
	
	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------
	
	public int getGeneration() {
		return 0;
	}
	
	public int getExpirationSeconds() {
		return 0;
	}
	
	public Key getKey() {
		return null;
	}

	//-------------------------------------------------------
	// Bin retrieval methods by bin name
	//-------------------------------------------------------
	
	public String getString(String name) {
		return null;
	}
	
	public double getDouble(String name) {
		return 0.0;
	}

	public long getLong(String name) {
		return 0;
	}
	
	public int getInt(String name) {
		return 0;
	}
	
	public byte[] getBytes(String name) {
		return null;
	}

	public Object getObject(String name) {
		return null;
	}

	//-------------------------------------------------------
	// Bin retrieval methods by bin index
	//-------------------------------------------------------
	
	public String getString(int index) {
		return null;
	}

	public double getDouble(int index) {
		return 0.0;
	}

	public long getLong(int index) {
		return 0;
	}

	public int getInt(int index) {
		return 0;
	}
	
	public byte[] getBytes(int index) {
		return null;
	}

	public Object getObject(int index) {
		return null;
	}

	//-------------------------------------------------------
	// Bin retrieval methods by positional offset
	//-------------------------------------------------------
	
	public String getString() {
		offset++;
		return null;
	}

	public double getDouble() {
		offset++;
		return 0.0;
	}

	public long getLong() {
		offset++;
		return 0;
	}

	public int getInt() {
		offset++;
		return 0;
	}
	
	public byte[] getBytes() {
		offset++;
		return null;
	}

	public Object getObject() {
		return null;
	}
}
