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
