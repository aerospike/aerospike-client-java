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
 * Column name/value pair.
 */
public final class Bin {
	/**
	 * Bin name. Current limit is 15 characters.
	 */
	public final String name;

	/**
	 * Bin value.
	 */
	public final Value value;

	/**
	 * Constructor, specifying bin name and string value.
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * <p>
	 * If connecting to Aerospike 2 servers, use the following instead:
	 * <pre>
	 * {@code
	 * Bin bin = new Bin(name, (Object)list);
	 * }
	 * </pre>
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * For servers configured as "single-bin", enter a null or empty name.
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
	 * server map type with specified mapOrder.  For servers configured as "single-bin", enter
	 * a null or empty name.
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
	 * Constructor, specifying bin name and value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public Bin(String name, Value value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Create bin with an object value. This is the slowest of the Bin constructors because the type
	 * must be determined using multiple "instanceof" checks. If the object type is unrecognized,
	 * the default java serializer is used.
	 * <p>
	 * To disable this constructor, set {@link com.aerospike.client.Value#DisableSerializer} to true.
	 *
	 * @param name		bin name, current limit is 15 characters.
	 * 					For servers configured as "single-bin", enter a null or empty name.
	 * @param value		bin value
	 */
	public Bin(String name, Object value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Create bin with a blob value.  The value will be java serialized.
	 * This method is faster than the bin object constructor because the blob is converted
	 * directly instead of using multiple "instanceof" type checks with a blob default.
	 * <p>
	 * To disable this method, set {@link com.aerospike.client.Value#DisableSerializer} to true.
	 *
	 * @param name		bin name, current limit is 15 characters.
	 * 					For servers configured as "single-bin", enter a null or empty name.
	 * @param value		bin value
	 */
	public static Bin asBlob(String name, Object value) {
		return new Bin(name, Value.getAsBlob(value));
	}

	/**
	 * Create bin with a null value. This is useful for bin deletions within a record.
	 * For servers configured as "single-bin", enter a null or empty name.
	 *
	 * @param name		bin name, current limit is 15 characters
	 */
	public static Bin asNull(String name) {
		return new Bin(name, Value.getAsNull());
	}

	/**
	 * Create bin with a GeoJSON value.
	 *
	 * @param name		bin name, current limit is 15 characters
	 * @param value		bin value
	 */
	public static Bin asGeoJSON(String name, String value) {
		return new Bin(name, Value.getAsGeoJSON(value));
	}

	/**
	 * Return string representation of bin.
	 */
	@Override
	public String toString() {
		return name + ':' + value;
	}

	/**
	 * Compare Bin for equality.
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
	 * Return hash code for Bin.
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
