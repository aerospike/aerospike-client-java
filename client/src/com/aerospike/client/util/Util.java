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

import gnu.crypto.util.Base64;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
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
		Throwable cause = e.getCause();
		if (e instanceof SocketException || e instanceof AerospikeException.Connection || 
			cause instanceof SocketTimeoutException) {			
			return e.getMessage();
		}
		
		if (e instanceof EOFException || cause instanceof EOFException) {
			return EOFException.class.getName();
		}
		
		// Unexpected exceptions need a stacktrace.
		StringWriter sw = new StringWriter(1000);
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}

	public static String readFileEncodeBase64(String path) throws AerospikeException {
		try {
			File file = new File(path);
			byte[] bytes = new byte[(int)file.length()];
			FileInputStream in = new FileInputStream(file);
			
			try {
				int pos = 0;
				int len = 0;
				
				while (pos < bytes.length) {
					len = in.read(bytes, pos, bytes.length - pos);
					pos += len;
				}
			}
			finally {
				in.close();
			}
			return Base64.encode(bytes, 0, bytes.length, false);
		}
		catch (Exception e) {
			throw new AerospikeException("Failed to read " + path, e);
		}
	}
}
