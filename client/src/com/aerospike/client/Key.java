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

import gnu.crypto.hash.RipeMD160;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.Value;

/**
 * Unique record identifier. Records can be identified using a specified namespace,
 * an optional set name, and a user defined key which must be unique within a set.
 * Records can also be identified by namespace/digest which is the combination used 
 * on the server.
 */
public final class Key {
	/**
	 * Namespace. Equivalent to database name.
	 */
	public final String namespace;
	
	/**
	 * Optional set name. Equivalent to database table.
	 */
	public final String setName;
	
	/**
	 * Unique server hash value generated from set name and user key.
	 */
	public final byte[] digest;
	
	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, Object key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		digest = computeDigest(setName, key);
	}
	
	/**
	 * Initialize key from namespace, optional set name and digest.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param digest				unique server hash value
	 */
	public Key(String namespace, String setName, byte[] digest) {
		this.namespace = namespace; 
		this.setName = setName;
		this.digest = digest;
	}
	
	/**
	 * Initialize key from namespace and digest.
	 * 
	 * @param namespace				namespace
	 * @param digest				unique server hash value
	 */
	public Key(String namespace, byte[] digest) {
		this.namespace = namespace; 
		this.setName = null;
		this.digest = digest;
	}

	/**
	 * Generate unique server hash value from set name and key type and user defined key.  
	 * The hash function is RIPEMD-160 (a 160 bit hash).
	 * 
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					record identifier, unique within set
	 * @return						unique server hash value
	 * @throws AerospikeException	if digest computation fails
	 */
	public static byte[] computeDigest(String setName, Object key) throws AerospikeException {
		// This method runs 14% faster using thread local byte array 
		// versus creating the buffer each time.
		byte[] buffer = Command.SendBufferThreadLocal.get();
		int setLength = Buffer.stringToUtf8(setName, buffer, 0);

		Value value = Value.getValue(key);
		buffer[setLength] = (byte)value.getType();
		
		int keyLength = value.write(buffer, setLength + 1);

		// Run additional 16% faster using direct class versus 
		// HashFactory.getInstance("RIPEMD-160").
		RipeMD160 hash = new RipeMD160();
		hash.update(buffer, 0, setLength);
		hash.update(buffer, setLength, keyLength + 1);
		return hash.digest();
	}
}
