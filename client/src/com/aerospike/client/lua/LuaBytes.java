/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.lua;

import org.luaj.vm2.LuaUserdata;

public class LuaBytes extends LuaUserdata {

	protected byte[] bytes;

	public LuaBytes(int size) {
		super(new byte[size]);
		bytes = (byte[])super.m_instance;
	}
	
	public LuaBytes(byte[] bytes) {
		super(bytes);
		this.bytes = bytes;
	}
	
	public void setLength(int length) {
		if (bytes.length == length) {
			return;
		}
		
		byte[] target = new byte[length];
		
		if (length > bytes.length) {
			length = bytes.length;
		}
		System.arraycopy(bytes, 0, target, 0, length);
		bytes = target;
	}
}
