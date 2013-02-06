/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
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
