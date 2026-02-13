/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.aerospike.client.cdt.MapOrder;

/**
 * A named bin (column) and its value for use in put and operate calls.
 *
 * <p>Bin names are limited to {@link #MAX_BIN_NAME_LENGTH} characters. Use the constructors or
 * static helpers ({@link #asNull}, {@link #asGeoJSON}) to create bins; the server accepts string,
 * numeric, blob, list, map, and GeoJSON values.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 * client.put(writePolicy, key,
 *     new Bin("name", "Alice"),
 *     new Bin("count", 42),
 *     new Bin("tags", Arrays.asList("a", "b")));
 * }</pre>
 *
 * @see Value
 * @see com.aerospike.client.AerospikeClient#put(com.aerospike.client.policy.WritePolicy, Key, Bin...)
 */
public final class Bin {
	/** Maximum allowed length for a bin name (15 characters). */
	public static final int MAX_BIN_NAME_LENGTH = 15;

	/**
	 * Bin name; must not exceed {@link #MAX_BIN_NAME_LENGTH} characters.
	 */
	public final String name;

	/**
	 * Bin value (string, number, blob, list, map, GeoJSON, etc.).
	 */
	public final Value value;

	/**
	 * Constructor, specifying bin name and string value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, String value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and byte array value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, byte[] value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name, byte array value and particle type.
	 * For internal use only.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 * @param type		bin type, see {@link com.aerospike.client.command.ParticleType}
	 */
	public Bin(String name, byte[] value, int type) {
		this.name = name;
		this.value = Value.get(value, type);
	}

	/**
	 * Constructor, specifying bin name and byte array segment value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		byte array value
	 * @param offset	byte array segment offset
	 * @param length	byte array segment length
	 */
	public Bin(String name, byte[] value, int offset, int length) {
		this.name = name;
		this.value = Value.get(value, offset, length);
	}

	/**
	 * Constructor, specifying bin name and byte value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, byte value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and short value.
	 * The server will convert all shorts to longs.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, short value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and integer value.
	 * The server will convert all integers to longs.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, int value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and long value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, long value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and double value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, double value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and float value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, float value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and boolean value.
	 * Either a boolean or integer bin is sent to the server, depending
	 * on configuration {@link com.aerospike.client.Value#UseBoolBin}.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, boolean value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Create bin with a list value.  The list value will be serialized as a server list type.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, List<?> value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Create bin with a map value.  The map value will be serialized as a server map type.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, Map<?,?> value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Create bin with a sorted map value.  The map value will be serialized as a server ordered map type.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, SortedMap<?,?> value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * This constructor is deprecated.
	 * Use {@link Bin#Bin(String, Map)} if the map is unsorted (like HashMap).
	 * Use {@link Bin#Bin(String, SortedMap)} if the map is sorted (like TreeMap).
	 * <p>
	 * Create bin with a map value and order.  The map value will be serialized as a server map type.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value, pass in a {@link java.util.SortedMap} instance if map order is sorted.
	 * @param mapOrder	map sorted order.
	 */
	@Deprecated
	public Bin(String name, Map<?,?> value, MapOrder mapOrder) {
		this.name = name;
		this.value = Value.get(value, mapOrder);
	}

	/**
	 * Create a map bin from a list of key/value entries.  The value will be serialized as a
	 * server map type with specified mapOrder.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		list of key/value entries already in desired sorted order
	 * @param mapOrder	map sorted order
	 */
	public Bin(String name, List<? extends Entry<?,?>> value, MapOrder mapOrder) {
		this.name = name;
		this.value = Value.get(value, mapOrder);
	}

	/**
	 * Constructs a bin with the given name and Value.
	 *
	 * @param name bin name; must not exceed {@link #MAX_BIN_NAME_LENGTH} characters
	 * @param value the value; may be {@code null} (use {@link #asNull} for explicit null)
	 */
	public Bin(String name, Value value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Creates a bin with a null value, used for bin deletions within a record.
	 *
	 * @param name bin name; must not exceed {@link #MAX_BIN_NAME_LENGTH} characters
	 * @return a bin whose value is null
	 */
	public static Bin asNull(String name) {
		return new Bin(name, Value.getAsNull());
	}

	/**
	 * Creates a bin with a GeoJSON string value.
	 *
	 * @param name bin name; must not exceed {@link #MAX_BIN_NAME_LENGTH} characters
	 * @param value GeoJSON string (e.g. point, polygon)
	 * @return a bin with the given GeoJSON value
	 */
	public static Bin asGeoJSON(String name, String value) {
		return new Bin(name, Value.getAsGeoJSON(value));
	}

	/**
	 * Returns a string representation of this bin (name:value).
	 *
	 * @return string representation
	 */
	@Override
	public String toString() {
		return name + ':' + value;
	}

	/**
	 * Compares this bin to the specified object for equality (name and value).
	 *
	 * @param obj the object to compare to
	 * @return {@code true} if equal, {@code false} otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Bin other = (Bin) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	/**
	 * Returns a hash code for this bin (based on name and value).
	 *
	 * @return the hash code
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}
}
