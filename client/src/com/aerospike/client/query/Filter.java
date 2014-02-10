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
