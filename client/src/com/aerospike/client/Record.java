/*
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
package com.aerospike.client;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Container object for records.  Records are equivalent to rows.
 */
public final class Record {
	/**
	 * Map of requested name/value bins.
	 */
	public final Map<String,Object> bins;
	
	/**
	 * Record modification count.
	 */
	public final int generation;
	
	/**
	 * Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
	 */
	public final int expiration;

	/**
	 * Initialize record.
	 */
	public Record(
		Map<String,Object> bins,
		int generation,
		int expiration
	) {
		this.bins = bins;
		this.generation = generation;
		this.expiration = expiration;
	}
	
	/**
	 * Get bin value given bin name.
	 * Enter empty string ("") for servers configured as single-bin.
	 */
	public Object getValue(String name) {
		return (bins == null)? null : bins.get(name);
	}
	
	/**
	 * Get bin value as String.
	 */
	public String getString(String name) {
		return (String)getValue(name);
	}
	
	/**
	 * Get bin value as double.
	 */
	public double getDouble(String name) {
		// The server always returns numbers as longs, so get long and convert bits.
		return Double.longBitsToDouble(getLong(name));
	}

	/**
	 * Get bin value as float.
	 */
	public float getFloat(String name) {
		// The server always returns numbers as longs, so get long and convert bits.
		return (float)Double.longBitsToDouble(getLong(name));
	}

	/**
	 * Get bin value as long.
	 */
	public long getLong(String name) {
		// The server always returns numbers as longs if bin found.
		// If bin not found, the result will be null.  Convert null to zero.
		Object result = getValue(name);
		return (result != null)? (Long)result : 0;
	}

	/**
	 * Get bin value as int.
	 */
	public int getInt(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (int)getLong(name);
	}

	/**
	 * Get bin value as short.
	 */
	public short getShort(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (short)getLong(name);
	}

	/**
	 * Get bin value as byte.
	 */
	public byte getByte(String name) {
		// The server always returns numbers as longs, so get long and cast.
		return (byte)getLong(name);
	}

	/**
	 * Get bin value as boolean.
	 */
	public boolean getBoolean(String name) {
		// The server always returns booleans as longs, so get long and convert.
		return (getLong(name) != 0) ? true : false;
	}
	
	/**
	 * Convert record expiration (seconds from Jan 01 2010 00:00:00 GMT) to
	 * ttl (seconds from now).
	 */
	public int getTimeToLive() {
		// This is the server's flag indicating the record never expires.
		if (expiration == 0) {
			// Convert to client-side convention for "never expires".
			return -1;
		}
		
		// Subtract milliseconds (1970/1/1 GMT to 2010/1/1 GMT) from current time.
		// Handle server's unsigned int ttl with java's usage of long for time.
		int now = (int)((System.currentTimeMillis() - 1262304000000L) / 1000);
		
		// Record may not have expired on server, but delay or clock differences may
		// cause it to look expired on client. Floor at 1, not 0, to avoid old
		// "never expires" interpretation.
		return (expiration < 0 || expiration > now) ? expiration - now : 1;
	}

	/**
	 * Return String representation of record.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(500);
		sb.append("(gen:");
		sb.append(generation);
		sb.append("),(exp:");
		sb.append(expiration);
		sb.append("),(bins:");
		
		if (bins != null) {
			boolean sep = false;
			
			for (Entry<String,Object> entry : bins.entrySet()) {
				if (sep) {
					sb.append(',');
				}
				else {
					sep = true;
				}
				sb.append('(');
				sb.append(entry.getKey());
				sb.append(':');
				sb.append(entry.getValue());
				sb.append(')');
				
				if (sb.length() > 1000) {
					sb.append("...");
					break;
				}
			}
		}
		else {
			sb.append("null");
		}
		sb.append(')');
		return sb.toString();
	}
}
