/*
 * Copyright 2012-2018 Aerospike, Inc.
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


/**
 * Column name/value pair. 
 */
public final class Bin {
	/**
	 * Bin name. Current limit is 14 characters.
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
	 * @param name		bin name, current limit is 14 characters
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
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, byte[] value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and byte array segment value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
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
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, byte value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and integer value.
	 * The server will convert all integers to longs.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
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
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, long value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Constructor, specifying bin name and double value.
	 * Aerospike server versions >= 3.6.0 natively support floating point values.  If your cluster
	 * supports floating point values, then this is always the correct constructor for double.
	 * Remember to also set {@link com.aerospike.client.Value#UseDoubleType} to true;
	 * <p>
	 * If your cluster does not support floating point, the value is converted to long bits.
	 * On reads, it's important to call {@link com.aerospike.client.Record#getDouble(String name)}
	 * to indicate that the long returned by the server should be converted back to a double.
	 * If the same bin name holds different types for different records, then this constructor
	 * should not be used because there is no way to know when reading if the long should be
	 * converted to a double.  Instead, use {@link #Bin(String name, Object value)} which converts
	 * the double to a java serialized blob.
	 * <pre>
	 * double value = 22.7;
	 * Bin bin = new Bin("mybin", (Object) value);
	 * </pre>
	 * This is slower and not portable to other languages, but the double type is preserved, so a
	 * Double will be returned without knowing if a conversion should be made.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, double value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and float value.
	 * Aerospike server versions >= 3.6.0 natively support floating point values.  If your cluster
	 * supports floating point values, then this is always the correct constructor for float.
	 * Remember to also set {@link com.aerospike.client.Value#UseDoubleType} to true;
	 * <p>
	 * If your cluster does not support floating point, the value is converted to long bits.
	 * On reads, it's important to call {@link com.aerospike.client.Record#getFloat(String name)}
	 * to indicate that the long returned by the server should be converted back to a float.
	 * If the same bin name holds different types for different records, then this constructor
	 * should not be used because there is no way to know when reading if the long should be
	 * converted to a float.  Instead, use {@link #Bin(String name, Object value)} which converts
	 * the float to a java serialized blob.
	 * <pre>
	 * float value = 11.7;
	 * Bin bin = new Bin("mybin", (Object) value);
	 * </pre>
	 * This is slower and not portable to other languages, but the float type is preserved, so a
	 * Float will be returned without knowing if a conversion should be made.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, float value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and boolean value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
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
	 * @param name		bin name, current limit is 14 characters
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
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Map<?,?> value) {
		this.name = name;
		this.value = Value.get(value);
	}

	/**
	 * Constructor, specifying bin name and value.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Value value) {
		this.name = name;
		this.value = value;
	}
	
	/**
	 * Constructor, specifying bin name and object value.
	 * This is the slowest of the Bin constructors because the type
	 * must be determined using multiple "instanceof" checks.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 *
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public Bin(String name, Object value) {
		this.name = name;
		this.value = Value.get(value);
	}
	
	/**
	 * Create bin with a blob value.  The value will be java serialized.
	 * This method is faster than the bin Object constructor because the blob is converted 
	 * directly instead of using multiple "instanceof" type checks with a blob default.
	 * <p>
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asBlob(String name, Object value) {
		return new Bin(name, Value.getAsBlob(value));
	}

	/**
	 * Create bin with a null value. This is useful for bin deletions within a record.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 */
	public static Bin asNull(String name) {
		return new Bin(name, Value.getAsNull());
	}

	/**
	 * Create bin with a GeoJSON value.
	 * 
	 * @param name		bin name, current limit is 14 characters
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
