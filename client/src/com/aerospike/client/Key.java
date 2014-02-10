/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client;

import gnu.crypto.hash.RipeMD160;

import java.util.Arrays;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.ThreadLocalData;

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
	 * Original user key. This key is immediately converted to a hash digest.
	 * This key is not used or returned by the server.  If the user key needs 
	 * to persist on the server, the key should be explicitly stored in a bin.
	 */
	public final Object userKey;
	
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
	public Key(String namespace, String setName, String key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, new Value.StringValue(key));
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * If the user key needs to be stored on the server, the key should be stored in a bin.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, byte[] key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, new Value.BytesValue(key));
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * If the user key needs to be stored on the server, the key should be stored in a bin.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @param offset				byte array segment offset
	 * @param length				byte array segment length
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, byte[] key, int offset, int length) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		Value value = new Value.ByteSegmentValue(key, offset, length);
		this.userKey = value;
		digest = computeDigest(setName, value);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * If the user key needs to be stored on the server, the key should be stored in a bin.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, int key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, new Value.LongValue(key));
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * If the user key needs to be stored on the server, the key should be stored in a bin.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, long key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, new Value.LongValue(key));
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The server handles record identifiers by digest only.
	 * If the user key needs to be stored on the server, the key should be stored in a bin.
	 * 
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, Value key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, key);
	}

	/*
	 * Removed Object constructor because the type must be determined using multiple "instanceof"
	 * checks.  If the type is not known, java serialization (slow) is used for byte conversion.
	 * These two performance penalties make this constructor unsuitable in all cases from
	 * a performance perspective.
	 * 
	 * The preferred method when using compound java key objects is to explicitly convert the 
	 * object to a byte[], String (or other known type) and call the associated Key constructor.
	 * 
	public Key(String namespace, String setName, Object key) throws AerospikeException {
		this.namespace = namespace; 
		this.setName = setName;
		this.userKey = key;
		digest = computeDigest(setName, Value.get(key));
	} */
	
	/**
	 * Initialize key from namespace, digest and optional set name.
	 * 
	 * @param namespace				namespace
	 * @param digest				unique server hash value
	 * @param setName				optional set name, enter null when set does not exist
	 */
	public Key(String namespace, byte[] digest, String setName) {
		this.namespace = namespace; 
		this.digest = digest;
		this.setName = setName;
		this.userKey = null;
	}
	
	/**
	 * Initialize key from namespace and digest.
	 * This constructor exists for the legacy CitrusleafClient compatibility layer only.
	 * Do not use.
	 * 
	 * @param namespace				namespace
	 * @param digest				unique server hash value
	 */
	public Key(String namespace, byte[] digest) {
		this.namespace = namespace; 
		this.digest = digest;
		this.setName = null;
		this.userKey = null;
	}

	/**
	 * Hash lookup uses namespace and digest.
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + Arrays.hashCode(digest);
		return prime * result + namespace.hashCode();
	}

	/**
	 * Equality uses namespace and digest.
	 */
	@Override
	public boolean equals(Object obj) {
		Key other = (Key) obj;
		
		if (! Arrays.equals(digest, other.digest))
			return false;
		
		return namespace.equals(other.namespace);
	}

	/**
	 * Generate unique server hash value from set name, key type and user defined key.  
	 * The hash function is RIPEMD-160 (a 160 bit hash).
	 * 
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					record identifier, unique within set
	 * @return						unique server hash value
	 * @throws AerospikeException	if digest computation fails
	 */
	public static byte[] computeDigest(String setName, Value key) throws AerospikeException {
		int keyType = key.getType();
		
		if (keyType == ParticleType.NULL) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid key: null");
		}
		
		// This method runs 14% faster using thread local byte array 
		// versus creating the buffer each time.
		byte[] buffer = ThreadLocalData.getBuffer();
		int setLength = Buffer.stringToUtf8(setName, buffer, 0);

		buffer[setLength] = (byte)keyType;		
		int keyLength = key.write(buffer, setLength + 1);

		// Run additional 16% faster using direct class versus 
		// HashFactory.getInstance("RIPEMD-160").
		RipeMD160 hash = new RipeMD160();
		hash.update(buffer, 0, setLength);
		hash.update(buffer, setLength, keyLength + 1);
		return hash.digest();
	}
}
