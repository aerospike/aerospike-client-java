/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client;

import java.util.Arrays;

import com.aerospike.client.command.Buffer;
import com.aerospike.client.util.Crypto;

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
	 * This key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 */
	public final Value userKey;

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 * <p>
	 * The key is converted to bytes to compute the digest.  The key's byte size is
	 * limited to the current thread's buffer size (min 8KB).  To store keys > 8KB, do one of the
	 * following:
	 * <ul>
	 * <li>Set once: <pre>{@code ThreadLocalData.DefaultBufferSize = maxKeySize + maxSetNameSize + 1;}</pre></li>
	 * <li>Or for every key:
	 * <pre>
	 * {@code int len = key.length() + setName.length() + 1;
	 * if (len > ThreadLocalData.getBuffer().length))
	 *     ThreadLocalData.resizeBuffer(len);}
	 * </pre>
	 * </li>
	 * </ul>
	 *
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, String key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.StringValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 * <p>
	 * The key's byte size is limited to the current thread's buffer size (min 8KB).  To store keys > 8KB, do one of the
	 * following:
	 * <ul>
	 * <li>Set once: <pre>{@code ThreadLocalData.DefaultBufferSize = maxKeySize + maxSetNameSize + 1;}</pre></li>
	 * <li>Or for every key:
	 * <pre>
	 * {@code int len = key.length + setName.length() + 1;
	 * if (len > ThreadLocalData.getBuffer().length))
	 *     ThreadLocalData.resizeBuffer(len);}
	 * </pre>
	 * </li>
	 * </ul>
	 *
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, byte[] key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.BytesValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 * <p>
	 * The key's byte size is limited to the current thread's buffer size (min 8KB).  To store keys > 8KB, do one of the
	 * following:
	 * <ul>
	 * <li>Set once: <pre>{@code ThreadLocalData.DefaultBufferSize = maxKeySize + maxSetNameSize + 1;}</pre></li>
	 * <li>Or for every key:
	 * <pre>
	 * {@code int len = length + setName.length() + 1;
	 * if (len > ThreadLocalData.getBuffer().length))
	 *     ThreadLocalData.resizeBuffer(len);}
	 * </pre>
	 * </li>
	 * </ul>
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
		this.userKey = new Value.ByteSegmentValue(key, offset, length);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 *
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, int key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.LongValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
	 *
	 * @param namespace				namespace
	 * @param setName				optional set name, enter null when set does not exist
	 * @param key					user defined unique identifier within set.
	 * @throws AerospikeException	if digest computation fails
	 */
	public Key(String namespace, String setName, long key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.LongValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initialize key from namespace, optional set name and user key.
	 * The set name and user defined key are converted to a digest before sending to the server.
	 * The user key is not used or returned by the server by default. If the user key needs
	 * to persist on the server, use one of the following methods:
	 * <ul>
	 * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	 * and retrieved on multi-record scans and queries.</li>
	 * <li>Explicitly store and retrieve the key in a bin.</li>
	 * </ul>
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

		// Some value types can't be used as keys (jblob, list, map, null).  Verify key type.
		key.validateKeyType();

		digest = Crypto.computeDigest(setName, key);
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
	 * Initialize key from namespace, digest, optional set name and optional userKey.
	 *
	 * @param namespace				namespace
	 * @param digest				unique server hash value
	 * @param setName				optional set name, enter null when set does not exist
	 * @param userKey				optional original user key (not hash digest).
	 */
	public Key(String namespace, byte[] digest, String setName, Value userKey) {
		this.namespace = namespace;
		this.digest = digest;
		this.setName = setName;
		// Do not try to validate userKey type because it is most likely null.
		this.userKey = userKey;
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
		return Crypto.computeDigest(setName, key);
	}

	@Override
	public String toString() {
		return this.namespace + ":" + this.setName + ":" + this.userKey + ":" + Buffer.bytesToHexString(this.digest);
	}
}
