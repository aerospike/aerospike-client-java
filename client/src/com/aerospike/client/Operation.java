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

/**
 * Database operation definition.  The class is used in client's operate() method. 
 */
public final class Operation {	
	/**
	 * Create read bin database operation.
	 */
	public static Operation get(String binName) {
		return new Operation(Type.READ, binName);
	}

	/**
	 * Create read all record bins database operation.
	 */
	public static Operation get() {
		return new Operation(Type.READ);
	}

	/**
	 * Create read record header database operation.
	 */
	public static Operation getHeader() {
		return new Operation(Type.READ_HEADER);
	}

	/**
	 * Create set database operation.
	 */
	public static Operation put(Bin bin) {
		return new Operation(Type.WRITE, bin);
	}

	/**
	 * Create string append database operation.
	 */
	public static Operation append(Bin bin) {
		return new Operation(Type.APPEND, bin);
	}
	
	/**
	 * Create string prepend database operation.
	 */
	public static Operation prepend(Bin bin) {
		return new Operation(Type.PREPEND, bin);
	}
	
	/**
	 * Create integer add database operation.
	 */
	public static Operation add(Bin bin) {
		return new Operation(Type.ADD, bin);
	}

	/**
	 * Create touch database operation.
	 */
	public static Operation touch() {
		return new Operation(Type.TOUCH);
	}

	public static enum Type {	
		READ(1),
		READ_HEADER(1),
		WRITE(2),
		ADD(5),
		APPEND(9),
		PREPEND(10),
		TOUCH(11);
		
		public final int protocolType;
		
		private Type(int protocolType) {
			this.protocolType = protocolType;
		}
	}

	/**
	 * Type of operation.
	 */
	public final Type type;
	
	/**
	 * Optional bin name used in operation.
	 */
	public final String binName;
	
	/**
	 * Optional bin value used in operation.
	 */
	public final Value binValue;
		 
	private Operation(Type type, Bin bin) {
		this.type = type;
		this.binName = bin.name;
		this.binValue = bin.value;
	}
	
	private Operation(Type type, String binName) {
		this.type = type;
		this.binName = binName;
		this.binValue = Value.getAsNull();
	}
	
	private Operation(Type type) {
		this.type = type;
		this.binName = null;
		this.binValue = Value.getAsNull();
	}
}
