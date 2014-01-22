/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;

/**
 * Query filter definition.
 */
public final class Filter {
	/**
	 * Create long equality filter for query.
	 * 
	 * @param name			bin name
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(String name, long value) {
		Value val = Value.get(value);
		return new Filter(name, val, val);
	}

	/**
	 * Create string equality filter for query.
	 * 
	 * @param name			bin name
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(String name, String value) {
		Value val = Value.get(value);
		return new Filter(name, val, val);
	}

	/**
	 * Create equality filter for query.
	 * This method exists for backward compatibility only.  Do not use.
	 * 
	 * @param name			bin name
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter equal(String name, Value value) {
		return new Filter(name, value, value);
	}
	
	/**
	 * Create range filter for query.  
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 * 
	 * @param name			bin name
	 * @param begin			filter begin value
	 * @param end			filter end value
	 * @return				filter instance
	 */
	public static Filter range(String name, long begin, long end) {
		return new Filter(name, Value.get(begin), Value.get(end));
	}

	/**
	 * Create range filter for query.  
	 * Range arguments must be longs or integers which can be cast to longs.
	 * String ranges are not supported.
	 * This method exists for backward compatibility only.  Do not use.
	 * 
	 * @param name			bin name
	 * @param begin			filter begin value
	 * @param end			filter end value
	 * @return				filter instance
	 */
	public static Filter range(String name, Value begin, Value end) {
		return new Filter(name, begin, end);
	}

	private final String name;
	private final Value begin;
	private final Value end;
		
	private Filter(String name, Value begin, Value end) {
		this.name = name;
		this.begin = begin;
		this.end = end;
	}

	protected int estimateSize() throws AerospikeException {
		// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
		return Buffer.estimateSizeUtf8(name) + begin.estimateSize() + end.estimateSize() + 10;
	}
	
	protected int write(byte[] buf, int offset) throws AerospikeException {
		// Write name.
		int len = Buffer.stringToUtf8(name, buf, offset + 1);
		buf[offset] = (byte)len;
		offset += len + 1;
		
		// Write particle type.
		buf[offset++] = (byte)begin.getType();
		
		// Write filter begin.
		len = begin.write(buf, offset + 4);
		Buffer.intToBytes(len, buf, offset);
		offset += len + 4;
		
		// Write filter end.
		len = end.write(buf, offset + 4);
		Buffer.intToBytes(len, buf, offset);
		offset += len + 4;
		
		return offset;
	}	
}
