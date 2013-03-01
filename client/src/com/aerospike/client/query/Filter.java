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

public final class Filter {
	/**
	 * Create single filter for query.
	 * 
	 * @param name			bin name
	 * @param operator		currently ignored
	 * @param value			filter value
	 * @return				filter instance
	 */
	public static Filter create(String name, Operator operator, Value value) {
		return new Filter(name, operator, value, null, Value.getAsNull());
	}
	
	/**
	 * Create range filter for query.
	 * 
	 * @param name			bin name
	 * @param beginOperator	currently ignored
	 * @param begin			filter begin value
	 * @param endOperator	currently ignored
	 * @param end			filter end value
	 * @return				filter instance
	 */
	public static Filter range(String name, Operator beginOperator, Value begin, Operator endOperator, Value end) {
		return new Filter(name, beginOperator, begin, endOperator, end);
	}

	private final String name;
	//private final Operator beginOperator;
	private final Value begin;
	//private final Operator endOperator;
	private final Value end;
		
	private Filter(String name, Operator beginOperator, Value begin, Operator endOperator, Value end) {
		this.name = name;
		//this.beginOperator = beginOperator;
		this.begin = begin;
		//this.endOperator = endOperator;
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
