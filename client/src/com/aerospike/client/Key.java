/*
 * Copyright 2012-2024 Aerospike, Inc.
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
 * Unique record identifier combining namespace, optional set name, and a user-defined key (or digest).
 *
 * <p>The server identifies records by namespace and a digest (hash of set name + user key). The user key
 * is not stored or returned by the server unless {@link com.aerospike.client.policy.WritePolicy#sendKey} is {@code true} or you store
 * it in a bin. Key size is limited by the thread's buffer (min 8KB); see constructors for large-key handling.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * Key key = new Key("test", "users", "user-123");
 * Record rec = client.get(policy, key);
 * }</pre>
 *
 * @see com.aerospike.client.policy.WritePolicy#sendKey
 * @see #computeDigest(String, Value)
 */
public final class Key {
	/**
	 * Namespace name (equivalent to database name); must not be {@code null}.
	 */
	public final String namespace;

	/**
	 * Optional set name (equivalent to database table); {@code null} when the set does not exist.
	 */
	public final String setName;

	/**
	 * Unique server hash (digest) computed from set name and user key; used by the server to identify the record.
	 */
	public final byte[] digest;

	/**
	 * Original user key; converted to {@link #digest} for the server.
	 *
 * <p>Not stored or returned by the server unless {@link com.aerospike.client.policy.WritePolicy#sendKey} is {@code true} or the key
 * is stored in a bin explicitly.
	 */
	public final Value userKey;

	/**
	 * Initializes a key from namespace, optional set name, and string user key.
	 *
	 * <p>The user key is hashed into {@link #digest}; it is not stored or returned by the server unless
	 * {@link com.aerospike.client.policy.WritePolicy#sendKey} is {@code true} or you store it in a bin. Key byte size is limited by the
	 * thread buffer (min 8KB). For keys &gt; 8KB, set {@code ThreadLocalData.DefaultBufferSize} or call
	 * {@code ThreadLocalData.resizeBuffer(len)} before creating the key.
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key user-defined unique identifier within the set; must not be {@code null}
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 */
	public Key(String namespace, String setName, String key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.StringValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initializes a key from namespace, optional set name, and byte-array user key.
	 *
	 * <p>The user key is hashed into {@link #digest}; it is not stored or returned by the server unless
	 * {@link com.aerospike.client.policy.WritePolicy#sendKey} is {@code true} or you store it in a bin. Key byte size is limited by the
	 * thread buffer (min 8KB). For larger keys, resize the buffer before creating the key.
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key user-defined unique identifier within the set; must not be {@code null}
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 */
	public Key(String namespace, String setName, byte[] key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.BytesValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initializes a key from namespace, optional set name, and a segment of a byte array as user key.
	 *
	 * <p>The user key segment is hashed into {@link #digest}. Key byte size is limited by the thread buffer (min 8KB).
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key byte array containing the user key; must not be {@code null}
	 * @param offset offset into {@code key} of the segment
	 * @param length length of the segment in bytes
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 */
	public Key(String namespace, String setName, byte[] key, int offset, int length) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.ByteSegmentValue(key, offset, length);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initializes a key from namespace, optional set name, and integer user key.
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key user-defined unique identifier within the set
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 */
	public Key(String namespace, String setName, int key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.LongValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initializes a key from namespace, optional set name, and long user key.
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key user-defined unique identifier within the set
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 */
	public Key(String namespace, String setName, long key) throws AerospikeException {
		this.namespace = namespace;
		this.setName = setName;
		this.userKey = new Value.LongValue(key);
		digest = Crypto.computeDigest(setName, this.userKey);
	}

	/**
	 * Initializes a key from namespace, optional set name, and {@link Value} user key.
	 *
	 * <p>Only key-capable value types are allowed (e.g. string, long, bytes); list, map, and null are not valid.
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key user-defined key as a Value; must not be {@code null} and must be a valid key type
	 * @throws AerospikeException	when digest computation fails or the key type is invalid (e.g. unsupported type such as list or map).
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
	 * Initializes a key from namespace, digest, optional set name, and optional user key (e.g. when reconstructing from scan/query).
	 *
	 * @param namespace namespace name; must not be {@code null}
	 * @param digest unique server hash (digest) for the record; must not be {@code null}
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param userKey original user key if known, or {@code null}
	 */
	public Key(String namespace, byte[] digest, String setName, Value userKey) {
		this.namespace = namespace;
		this.digest = digest;
		this.setName = setName;
		// Do not try to validate userKey type because it is most likely null.
		this.userKey = userKey;
	}

	/**
	 * Returns a hash code based on namespace and digest (for use in hash-based collections).
	 *
	 * @return the hash code
	 */
	@Override
	public int hashCode() {
		// The digest is already a hash, so pick 4 bytes from the 20 byte digest at a
		// random offset (in this case 8).
		final int result = Buffer.littleBytesToInt(digest, 8) + 31;
		return result * 31 + namespace.hashCode();
	}

	/**
	 * Compares this key to the specified object for equality (namespace and digest).
	 *
	 * @param obj the object to compare to
	 * @return {@code true} if equal, {@code false} otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		Key other = (Key) obj;

		if (! Arrays.equals(digest, other.digest)) {
			return false;
		}
		return namespace.equals(other.namespace);
	}

	/**
	 * Computes the unique server digest (RIPEMD-160 hash) from set name and user key.
	 *
	 * <p>Used when you need the digest without constructing a full {@link Key} (e.g. for custom partitioning).
	 *
	 * @param setName set name, or {@code null} when the set does not exist
	 * @param key record identifier as a Value; must not be {@code null} and must be a valid key type
	 * @return 20-byte digest used by the server to identify the record
	 * @throws AerospikeException	when digest computation fails (e.g. unsupported or invalid key type).
	 * @see Key
	 */
	public static byte[] computeDigest(String setName, Value key) throws AerospikeException {
		return Crypto.computeDigest(setName, key);
	}

	@Override
	public String toString() {
		return this.namespace + ":" + this.setName + ":" + this.userKey + ":" + Buffer.bytesToHexString(this.digest);
	}
}
