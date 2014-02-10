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
	 * Constructor, specifying bin name and integer value.
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
	 * Create bin with a list value.  The list value will be serialized as a Aerospike 3 server list type.
	 * Supported by Aerospike 3 servers only.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asList(String name, List<?> value) {
		return new Bin(name, Value.getAsList(value));
	}

	/**
	 * Create bin with a map value.  The map value will be serialized as a Aerospike 3 server map type.
	 * Supported by Aerospike 3 servers only.
	 * For servers configured as "single-bin", enter a null or empty name.
	 * 
	 * @param name		bin name, current limit is 14 characters
	 * @param value		bin value
	 */
	public static Bin asMap(String name, Map<?,?> value) {
		return new Bin(name, Value.getAsMap(value));
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
	 * Return string representation of bin.
	 */
	@Override
	public String toString() {
		return name + ':' + value;
	}
}
