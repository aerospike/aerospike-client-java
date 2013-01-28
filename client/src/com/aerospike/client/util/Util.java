/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.util;

import java.io.EOFException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import com.aerospike.client.AerospikeException;

public final class Util {
	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException ie) {
		}
	}
	
	public static String getErrorMessage(Exception e) {
		// Connection error messages don't need a stacktrace.
		if (e instanceof SocketException || e instanceof AerospikeException.Connection || 
			e.getCause() instanceof SocketTimeoutException) {			
			return e.getMessage();
		}
		
		if (e instanceof EOFException) {
			return EOFException.class.getName();
		}
		
		// Unexpected exceptions need a stacktrace.
		StringWriter sw = new StringWriter(1000);
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
}
