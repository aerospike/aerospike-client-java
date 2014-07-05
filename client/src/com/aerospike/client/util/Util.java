/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
	
	/**
	 * Convert a string to a time stamp using the same algorithm as the Aerospike loader.
	 */
	public static long toTimeStamp(String dateTime, SimpleDateFormat format, int timeZoneOffset) throws ParseException {
		Date formatDate = format.parse(dateTime);
		long miliSecondForDate = formatDate.getTime()
				- timeZoneOffset;
		return miliSecondForDate / 1000;		
	}
	
	/**
	 * Convert a string to a time stamp using a string pattern.
	 */
	public static long toTimeStamp(String dateTime, String pattern, int timeZoneOffset) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		return toTimeStamp(dateTime, format, timeZoneOffset);
	}
	
	/**
	 * Convert a time stamp (time in milliseconds) to a string.
	 */
	public static String fromTimeStamp(long timeStamp, SimpleDateFormat format){
		Date formatDate = new Date(timeStamp);
		return format.format(formatDate);
	}
	
	/**
	 * Convert a time stamp (time in milliseconds) to a string using a string pattern.
	 */
	public static String fromTimeStamp(long timeStamp, String pattern){
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		return fromTimeStamp(timeStamp, format);
	}
}
